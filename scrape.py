#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrapes a Looker Studio report for NPS comments and posts them to Google Chat.

2FA upgrades:
- Polls up to 120s for Google's "Match the number" screen.
- Extracts digits from visible buttons (primary) and body text (fallback).
- Sends immediate alerts with the detected code + all options.
- Saves screenshots/HTML snapshots when 2FA is detected and on timeout.
"""

import os
import sys
import csv
import time
import logging
import re
import requests
import schedule
import configparser
import unicodedata
from pathlib import Path
from typing import List, Optional, Tuple
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

############################################
# PATHS & CONSTANTS
############################################
BASE_DIR = Path(__file__).resolve().parent
AUTH_STATE_PATH = BASE_DIR / "auth_state.json"
COMMENTS_LOG_PATH = BASE_DIR / "comments_log.csv"
LOG_FILE_PATH = BASE_DIR / "scrape.log"
SCREENS_DIR = BASE_DIR / "screens"
LOCK_FILE = BASE_DIR / "scrape.lock"

# Use the embed (or normal) URL of your report
LOOKER_STUDIO_URL = "https://lookerstudio.google.com/embed/u/0/reporting/d93a03c7-25dc-439d-abaa-dd2f3780daa5/page/p_9x4lp9ksld"

# Webhook batching / rate limiting
BATCH_SIZE = 10
MAX_COMMENTS_PER_RUN = 30
BASE_BACKOFF = 2.0
MAX_BACKOFF = 30.0

############################################
# LOGGING
############################################
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE_PATH), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

############################################
# CONFIG
############################################
config = configparser.ConfigParser()
config.read(BASE_DIR / "config.ini")

GOOGLE_EMAIL = config["DEFAULT"].get("GOOGLE_EMAIL", os.getenv("GOOGLE_EMAIL", ""))
GOOGLE_PASSWORD = config["DEFAULT"].get("GOOGLE_PASSWORD", os.getenv("GOOGLE_PASSWORD", ""))
MAIN_WEBHOOK = config["DEFAULT"].get("MAIN_WEBHOOK", os.getenv("MAIN_WEBHOOK", ""))
ALERT_WEBHOOK = config["DEFAULT"].get("ALERT_WEBHOOK", os.getenv("ALERT_WEBHOOK", ""))

# Optional CI run link injected by workflow
CI_RUN_URL = os.getenv("CI_RUN_URL", "")

if not GOOGLE_EMAIL or not GOOGLE_PASSWORD:
    logger.warning("Google credentials missing (config.ini or env).")
if not MAIN_WEBHOOK:
    logger.warning("Main Chat webhook missing (config.ini or env).")
if not ALERT_WEBHOOK:
    logger.warning("Alert Chat webhook missing (config.ini or env).")

############################################
# CHAT HELPERS
############################################
def _post_with_backoff(url: str, payload: dict) -> bool:
    backoff = BASE_BACKOFF
    while True:
        try:
            r = requests.post(url, json=payload, timeout=20)
            if r.status_code == 200:
                return True
            if r.status_code == 429:
                delay = min(float(r.headers.get("Retry-After", backoff)), MAX_BACKOFF)
                logger.error(f"429 from webhook — sleeping {delay:.1f}s")
                time.sleep(delay)
                backoff = min(backoff * 1.7, MAX_BACKOFF)
                continue
            logger.error(f"Webhook error {r.status_code}: {r.text[:300]}")
            return False
        except Exception as e:
            logger.error(f"Webhook exception: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 1.7, MAX_BACKOFF)

def alert(text_lines: List[str]):
    if not ALERT_WEBHOOK or "chat.googleapis.com" not in ALERT_WEBHOOK:
        logger.warning("No valid ALERT_WEBHOOK configured.")
        return
    if CI_RUN_URL:
        text_lines.append(f"• CI run: {CI_RUN_URL}")
    _post_with_backoff(ALERT_WEBHOOK, {"text": "\n".join(text_lines)})

############################################
# DEBUG DUMP
############################################
def dump_debug(page, tag):
    try:
        ts = int(time.time())
        SCREENS_DIR.mkdir(parents=True, exist_ok=True)
        png = SCREENS_DIR / f"{ts}_{tag}.png"
        html = SCREENS_DIR / f"{ts}_{tag}.html"
        page.screenshot(path=str(png), full_page=True)
        html.write_text(page.content(), encoding="utf-8")
        logger.info(f"Saved debug snapshot → {png.name}, {html.name}")
    except Exception as e:
        logger.warning(f"Failed to save debug snapshot: {e}")

############################################
# 2FA EXTRACTION
############################################
RE_TWO_OR_THREE = re.compile(r"(?<!\d)(\d{2,3})(?!\d)")

def _extract_numbers_from_buttons(page) -> List[str]:
    # Gather digits from obvious clickable elements first
    numbers = []
    try:
        # Buttons commonly used for the three choices
        for locator in [
            "button",
            "[role='button']",
            "div[role='button']",
            "span[role='button']",
        ]:
            for txt in page.locator(locator).all_text_contents():
                txt = (txt or "").strip()
                for m in RE_TWO_OR_THREE.findall(txt):
                    if m not in numbers:
                        numbers.append(m)
        return numbers
    except Exception:
        return numbers

def _extract_number_from_body(page) -> Optional[str]:
    try:
        body = page.inner_text("body")
    except Exception:
        body = ""
    if not body:
        return None
    # Prefer two-digit match (most common), fallback to first 3-digit
    nums = RE_TWO_OR_THREE.findall(body)
    for n in nums:
        if len(n) == 2:
            return n
    return nums[0] if nums else None

def wait_for_2fa_and_alert(page, max_wait_s: int = 120) -> None:
    """
    Polls for the 'Match the number' step and alerts when detected.
    Sends the first candidate as the code hint and lists all options.
    """
    first_alert_sent = False
    start = time.time()
    while time.time() - start < max_wait_s:
        # Look for trigger text to confirm we're on the challenge screen
        txt = ""
        try:
            txt = page.inner_text("body")
        except Exception:
            pass
        lower = (txt or "").lower()

        challenge = any(
            p in lower for p in [
                "match the number",
                "tap the number shown",
                "verify it’s you",
                "verify it's you",
                "check your phone"
            ]
        )
        if challenge:
            if not first_alert_sent:
                dump_debug(page, "2fa_detected")
                alert(["⚠️ Google login needs approval.",
                       "• 2FA screen detected — parsing the code now…"])
                first_alert_sent = True

            # Primary: harvest button numbers
            btn_nums = _extract_numbers_from_buttons(page)
            code_hint = btn_nums[0] if btn_nums else None

            # Fallback: sniff body text
            if not code_hint:
                code_hint = _extract_number_from_body(page)

            if code_hint:
                options_note = f"• Options on screen: {', '.join(btn_nums)}" if btn_nums else None
                lines = [f"🔐 2FA code to tap on your phone: **{code_hint}**"]
                if options_note:
                    lines.append(options_note)
                alert(lines)
                return
        time.sleep(1.0)

    # Timeout — couldn’t parse the number
    dump_debug(page, "2fa_timeout")
    alert(["⏳ 2FA challenge likely, but I couldn't parse the code in time.",
           "• Please approve on your phone; if you see 3 numbers, tap the one that matches on screen."])

############################################
# LOOKER FETCH
############################################
def fetch_looker_text(page, url, tag):
    logger.info("Navigating to Looker Studio (hybrid mode)...")
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=120000)
    except PlaywrightTimeoutError:
        logger.error("Navigation timeout.")
        dump_debug(page, f"{tag}_goto_timeout")
        return []

    if "accounts.google.com" in page.url:
        logger.warning("Redirected to login → auth required.")
        return None

    logger.info("Waiting 15s for Looker Studio content to load...")
    time.sleep(15)

    inner_text = ""
    try:
        inner_text = page.inner_text("body")
    except Exception:
        pass

    if not inner_text.strip():
        try:
            iframe = page.query_selector("iframe[src*='lookerstudio.google.com']")
            if iframe:
                frame = iframe.content_frame()
                logger.info("Found iframe → waiting 10s...")
                time.sleep(10)
                inner_text = frame.inner_text("body")
        except Exception:
            inner_text = ""

    if not inner_text.strip():
        logger.error("No text extracted even after waiting.")
        dump_debug(page, f"{tag}_no_text")
        return []

    lines = inner_text.splitlines()
    logger.info(f"✅ Fetched {len(lines)} lines ({len(inner_text)} chars) from Looker Studio.")

    try:
        SCREENS_DIR.mkdir(parents=True, exist_ok=True)
        dump_file = SCREENS_DIR / f"{int(time.time())}_{tag}_text.txt"
        dump_file.write_text(inner_text, encoding="utf-8")
        logger.info(f"Full text saved to {dump_file.name}")
    except Exception as e:
        logger.warning(f"Failed to save fetched text: {e}")

    return lines

############################################
# PARSER
############################################
DATE_PATTERN  = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SCORE_PATTERN = re.compile(r"^(10|[0-9])$")
STORE_PATTERN = re.compile(r"^\d+\s+.+")
SKIP_PATTERN = re.compile(
    r"^(This|Last|Yesterday|go back|regional_manager|Privacy$|By |Lighthouse|You are about to|"
    r"Highly$|Satisfied$|Dissatisfied$|The data on this report|Showing results|Record Count|ⓘ|Net Promoter Score|"
    r"Your weighted NPS|Satisfaction is the percentage|If no email survey responses|"
    r"The last \d+ (days|weeks)|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|"
    r"This Year|This Quarter|This Period|This Week|Google Home|Terms of Service|Privacy Policy)$",
    re.IGNORECASE
)

def _norm(s: str) -> str:
    if s is None:
        return ""
    s = s.replace("\u00A0", " ")
    s = s.replace("\u200B", "")
    s = unicodedata.normalize("NFKC", s)
    return s.strip()

def parse_comments_from_lines(lines: List[str]) -> List[dict]:
    if not lines:
        return []
    L = [_norm(x) for x in lines if _norm(x)]
    n = len(L)
    out: List[dict] = []
    i = 0
    while i < n:
        line = L[i]
        if line.startswith("Submission via:"):
            date_line = ""
            store_line = ""
            for j in range(max(0, i-8), i):
                lj = L[j]
                if not date_line and DATE_PATTERN.match(lj):
                    date_line = lj
                if not store_line and STORE_PATTERN.match(lj):
                    store_line = lj
                if date_line and store_line:
                    break
            comment_lines: List[str] = []
            score_line = ""
            k = i + 1
            while k < n:
                lk = L[k]
                if SCORE_PATTERN.match(lk):
                    score_line = lk
                    k += 1
                    break
                if DATE_PATTERN.match(lk) and comment_lines:
                    break
                if not SKIP_PATTERN.search(lk) and not lk.startswith("Submission via:"):
                    comment_lines.append(lk)
                k += 1
            if store_line and date_line and score_line and DATE_PATTERN.match(date_line) and STORE_PATTERN.match(store_line):
                out.append({
                    "store": store_line,
                    "timestamp": date_line,
                    "comment": ("\n".join(comment_lines).strip() or "[No text]"),
                    "score": score_line,
                })
            i = k; continue
        i += 1
    logger.info(f"Parsed {len(out)} comments from text (after noise filtering).")
    return out

############################################
# COMMENT LOG
############################################
def read_existing_comments():
    seen = set()
    if not COMMENTS_LOG_PATH.exists():
        return seen
    with open(COMMENTS_LOG_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, fieldnames=["store", "timestamp", "comment", "score"])
        for r in reader:
            seen.add((r["store"], r["timestamp"], r["comment"]))
    return seen

def append_new_comments(new_comments):
    with open(COMMENTS_LOG_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for c in new_comments:
            w.writerow([c["store"], c["timestamp"], c["comment"], c["score"]])

############################################
# SEND TO CHAT
############################################
def _score_to_label(score_str: str) -> Tuple[str, str]:
    try:
        v = int(score_str)
    except Exception:
        v = 0
    if v <= 4:  return "🔴", "Detractor"
    if v <= 7:  return "🟠", "Passive"
    return "🟢", "Promoter"

def send_comments_batched_to_chat(comments: List[dict]) -> None:
    if not MAIN_WEBHOOK or "chat.googleapis.com" not in MAIN_WEBHOOK:
        logger.warning("No valid MAIN_WEBHOOK configured.")
        return
    total = len(comments)
    sent = 0
    for start in range(0, total, BATCH_SIZE):
        batch = comments[start:start+BATCH_SIZE]
        sections = []
        for c in batch:
            emoji, label = _score_to_label(c.get("score", "0"))
            sections.append({
                "widgets": [
                    {"keyValue": {"topLabel": "Store", "content": f"{emoji} {c.get('store','')} ({label})"}},
                    {"keyValue": {"topLabel": "Timestamp", "content": c.get("timestamp","")}},
                    {"keyValue": {"topLabel": "Score", "content": str(c.get("score",""))}},
                    {"textParagraph": {"text": (c.get("comment") or "[No comment]").replace("\n","<br>")}}
                ]
            })
        payload = {"cards": [{
            "header": {"title": f"NPS Comments ({start+1}-{start+len(batch)} of {total})", "subtitle": "Automated report"},
            "sections": sections
        }]}
        _post_with_backoff(MAIN_WEBHOOK, payload)
        sent += len(batch)
        logger.info(f"✅ Sent batch {start+1}-{start+len(batch)} (total sent: {sent}/{total})")

############################################
# HOUSEKEEPING
############################################
def cleanup_old_screens(days: int = 14):
    if not SCREENS_DIR.exists():
        return
    cutoff = time.time() - days * 86400
    deleted = 0
    for f in SCREENS_DIR.glob("*"):
        try:
            if f.is_file() and f.stat().st_mtime < cutoff:
                f.unlink(); deleted += 1
        except Exception:
            pass
    if deleted:
        logger.info(f"🧹 Cleaned {deleted} old debug files (> {days} days).")

############################################
# LOGIN & MAIN
############################################
def login_and_save_state(page) -> bool:
    import re as _re
    logger.info("Starting manual login flow...")
    page.goto("https://accounts.google.com/")

    try:
        page.wait_for_selector("input[type='email']", timeout=15000)
    except PlaywrightTimeoutError:
        logger.error("Email input not found."); return False

    page.fill("input[type='email']", GOOGLE_EMAIL); page.keyboard.press("Enter")

    try:
        page.wait_for_selector("input[type='password']", timeout=30000)
    except PlaywrightTimeoutError:
        logger.error("Password input not found."); return False

    page.fill("input[type='password']", GOOGLE_PASSWORD); page.keyboard.press("Enter")

    # Watch for 2FA screen and alert with code/options
    wait_for_2fa_and_alert(page, max_wait_s=120)

    logger.info("If 2FA is enabled, approve the prompt on your phone...")
    try:
        page.wait_for_url(_re.compile(r"https://myaccount\.google\.com/.*"), timeout=180000)
        page.goto("https://lookerstudio.google.com/", timeout=60000, wait_until="domcontentloaded")
        page.context.storage_state(path=AUTH_STATE_PATH)
        logger.info("✅ Login successful and auth_state.json saved.")
        return True
    except PlaywrightTimeoutError:
        dump_debug(page, "2fa_final_timeout")
        alert(["❌ Login timed out after waiting for approval."])
        return False

def run_scrape(retry_on_auth_fail=True):
    if LOCK_FILE.exists():
        logger.warning("Another scrape already running — skipping this run.")
        return
    try:
        LOCK_FILE.write_text(str(os.getpid()))
        cleanup_old_screens(days=14)

        def _scrape_internal():
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(storage_state=AUTH_STATE_PATH if AUTH_STATE_PATH.exists() else None)
                page = context.new_page()

                lines = fetch_looker_text(page, LOOKER_STUDIO_URL, "scrape")
                login_wall = (lines is None)
                if not login_wall and lines:
                    sample = " ".join(lines[:80]).lower()
                    if "sign in" in sample or "can't access report" in sample or "please sign in" in sample:
                        login_wall = True

                if login_wall:
                    context.close(); browser.close()
                    return "RELOGIN_REQUIRED", []

                if not lines:
                    context.close(); browser.close()
                    return "NO_TEXT", []

                comments = parse_comments_from_lines(lines)
                context.close(); browser.close()
                return ("OK" if comments else "NO_COMMENTS"), comments

        status, comments = _scrape_internal()

        if status == "RELOGIN_REQUIRED":
            if not retry_on_auth_fail:
                alert(["⚠️ Repeated login failure — human attention required."]); return
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)  # works with xvfb in CI
                context = browser.new_context()
                page = context.new_page()
                ok = login_and_save_state(page)
                context.close(); browser.close()
            if not ok:
                alert(["❌ Manual login failed."]); return
            logger.info("Re-login successful — retrying scrape once.")
            run_scrape(retry_on_auth_fail=False); return

        if status in ("NO_TEXT", "NO_COMMENTS"):
            logger.warning("Nothing to send this run."); return

        # Dedupe + rate safety
        seen = read_existing_comments()
        new_comments = [c for c in comments if (c["store"], c["timestamp"], c["comment"]) not in seen]
        if not new_comments:
            logger.info("No new comments to send."); return
        capped = new_comments[:MAX_COMMENTS_PER_RUN]
        leftover = max(0, len(new_comments) - len(capped))
        if leftover > 0:
            logger.info(f"Rate safety: sending {len(capped)} now, deferring {leftover} later.")

        send_comments_batched_to_chat(capped)
        append_new_comments(capped)

        if leftover > 0 and MAIN_WEBHOOK and "chat.googleapis.com" in MAIN_WEBHOOK:
            _post_with_backoff(MAIN_WEBHOOK, {"text": f"ℹ️ {leftover} additional comments deferred to next runs (rate safety)."})

        logger.info("✅ Scrape complete.")
    finally:
        if LOCK_FILE.exists():
            try: LOCK_FILE.unlink()
            except Exception: pass

############################################
# SCHEDULER
############################################
def schedule_scrapes():
    schedule.every().day.at("08:00").do(run_scrape)
    schedule.every().day.at("11:00").do(run_scrape)
    schedule.every().day.at("14:00").do(run_scrape)
    schedule.every().day.at("17:00").do(run_scrape)
    schedule.every().day.at("20:00").do(run_scrape)
    logger.info("Scheduler started. Running every few hours.")
    while True:
        schedule.run_pending(); time.sleep(30)

############################################
# ENTRY
############################################
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() in ("now", "once", "manual"):
        logger.info("Manual trigger → running scrape now...")
        run_scrape()
        logger.info("Done.")
    else:
        schedule_scrapes()
