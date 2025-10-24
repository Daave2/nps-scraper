#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrapes a Looker Studio report for NPS comments and posts them to Google Chat.

Key features:
- Auto-detects login wall and opens a headed login window once; saves auth_state.json.
- Robust parser (anchors on "Submission via:", requires DATE + STORE + SCORE).
- Batching + exponential backoff for Chat webhooks to avoid 429s.
- Per-run cap (MAX_COMMENTS_PER_RUN) to keep within quotas.
- Lock file to prevent overlapping runs.
- Auto-cleans old debug screenshots/HTMLs from /screens.
"""

import os
import sys
import csv
import time
import logging
import re
import math
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
BATCH_SIZE = 10            # number of comments per webhook card
MAX_COMMENTS_PER_RUN = 30  # hard cap to avoid limits; remaining roll to next run
BASE_BACKOFF = 2.0         # seconds
MAX_BACKOFF = 30.0         # seconds

############################################
# LOGGING
############################################
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler(sys.stdout)
    ]
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

if not GOOGLE_EMAIL or not GOOGLE_PASSWORD:
    logger.warning("Google credentials missing (config.ini or env).")
if not MAIN_WEBHOOK:
    logger.warning("Main Chat webhook missing (config.ini or env).")
if not ALERT_WEBHOOK:
    logger.warning("Alert Chat webhook missing (config.ini or env).")

############################################
# ALERT HANDLER
############################################
def alert_login_needed(reason="Unknown reason"):
    if not ALERT_WEBHOOK or "chat.googleapis.com" not in ALERT_WEBHOOK:
        logger.warning("No valid ALERT_WEBHOOK configured.")
        return
    payload = {"text": f"⚠️ Google login needed. Reason: {reason}"}
    try:
        resp = requests.post(ALERT_WEBHOOK, json=payload, timeout=20)
        if resp.status_code == 200:
            logger.info("Login alert posted successfully.")
        else:
            logger.error(f"Login alert failed ({resp.status_code}): {resp.text[:300]}")
    except Exception as e:
        logger.error(f"Exception posting alert: {e}")

############################################
# DEBUG DUMP
############################################
def dump_debug(page, tag):
    try:
        ts = int(time.time())
        SCREENS_DIR.mkdir(parents=True, exist_ok=True)
        png = SCREENS_DIR / f"{ts}_{tag}.png"
        html = SCREENS_DIR / f"{ts}_{tag}.html"
        page.screenshot(path=png.as_posix(), full_page=True)
        html.write_text(page.content(), encoding="utf-8")
        logger.info(f"Saved debug snapshot → {png.name}, {html.name}")
    except Exception as e:
        logger.warning(f"Failed to save debug snapshot: {e}")

############################################
# LOGIN HANDLER
############################################
def login_and_save_state(page):
    """
    Opens Google login (headed), signs in with config.ini creds, visits Looker Studio
    so its domain cookies persist, and saves storage state to auth_state.json.
    """
    import re as _re
    logger.info("Starting manual login flow...")
    page.goto("https://accounts.google.com/")

    try:
        page.wait_for_selector("input[type='email']", timeout=15000)
    except PlaywrightTimeoutError:
        logger.error("Email input not found.")
        return False

    page.fill("input[type='email']", GOOGLE_EMAIL)
    page.keyboard.press("Enter")

    try:
        page.wait_for_selector("input[type='password']", timeout=30000)
    except PlaywrightTimeoutError:
        logger.error("Password input not found.")
        return False

    page.fill("input[type='password']", GOOGLE_PASSWORD)
    page.keyboard.press("Enter")

    logger.info("If 2FA is enabled, approve it in the browser window...")

    try:
        page.wait_for_url(_re.compile(r"https://myaccount\.google\.com/.*"), timeout=120000)
        # Visit Looker Studio domain once so its cookies also persist
        page.goto("https://lookerstudio.google.com/", timeout=60000, wait_until="domcontentloaded")
        page.context.storage_state(path=AUTH_STATE_PATH)
        logger.info("✅ Login successful and auth_state.json saved.")
        return True
    except PlaywrightTimeoutError:
        logger.error("Login timed out.")
        return False

############################################
# FETCH LOOKER STUDIO TEXT
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
# PARSER (anchor on "Submission via:")
############################################
DATE_PATTERN  = re.compile(r"^\d{4}-\d{2}-\d{2}$")  # 2025-10-22
SCORE_PATTERN = re.compile(r"^(10|[0-9])$")         # 0..10
STORE_PATTERN = re.compile(r"^\d+\s+.+")            # "218 Thornton Cleveleys"

# Noise / menu lines to skip from comments
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
    s = s.replace("\u00A0", " ")   # NBSP
    s = s.replace("\u200B", "")    # zero-width space
    s = unicodedata.normalize("NFKC", s)
    return s.strip()

def parse_comments_from_lines(lines: List[str]) -> List[dict]:
    """
    Robust parser for Looker Studio NPS comments:
    - Anchor on 'Submission via:'
    - Backward find nearest DATE and STORE
    - Forward collect comment lines until SCORE (0..10)
    - Skip menu noise
    """
    if not lines:
        return []

    L = [_norm(x) for x in lines if _norm(x)]
    n = len(L)
    comments: List[dict] = []

    i = 0
    while i < n:
        line = L[i]
        if line.startswith("Submission via:"):
            # Backwards: find date + store within last ~8 lines
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

            # Forwards: collect comment until score
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
                    # likely start of next block
                    break
                if not SKIP_PATTERN.search(lk) and not lk.startswith("Submission via:"):
                    comment_lines.append(lk)
                k += 1

            # Validate block — ensure timestamp is actually a date and store looks right
            if store_line and date_line and score_line and DATE_PATTERN.match(date_line) and STORE_PATTERN.match(store_line):
                comments.append({
                    "store": store_line,
                    "timestamp": date_line,
                    "comment": ("\n".join(comment_lines).strip() or "[No text]"),
                    "score": score_line,
                })
            i = k
            continue
        i += 1

    logger.info(f"Parsed {len(comments)} comments from text (after noise filtering).")
    return comments

############################################
# COMMENT LOGGING
############################################
def read_existing_comments():
    existing = set()
    if not COMMENTS_LOG_PATH.exists():
        return existing
    with open(COMMENTS_LOG_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, fieldnames=["store", "timestamp", "comment", "score"])
        for row in reader:
            existing.add((row["store"], row["timestamp"], row["comment"]))
    return existing

def append_new_comments(new_comments):
    with open(COMMENTS_LOG_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for c in new_comments:
            writer.writerow([c["store"], c["timestamp"], c["comment"], c["score"]])

############################################
# GOOGLE CHAT SENDER (BATCHED)
############################################
def _score_to_label(score_str: str) -> Tuple[str, str]:
    try:
        v = int(score_str)
    except Exception:
        v = 0
    if v <= 4:  return "🔴", "Detractor"
    if v <= 7:  return "🟠", "Passive"
    return "🟢", "Promoter"

def _post_with_backoff(url: str, payload: dict) -> bool:
    backoff = BASE_BACKOFF
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.post(url, json=payload, timeout=20)
            if resp.status_code == 200:
                return True
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else backoff
                except Exception:
                    delay = backoff
                delay = min(delay, MAX_BACKOFF)
                logger.error(f"429 RESOURCE_EXHAUSTED on attempt {attempt} — sleeping {delay:.1f}s")
                time.sleep(delay)
                backoff = min(backoff * 1.7, MAX_BACKOFF)
                continue
            logger.error(f"Webhook post failed ({resp.status_code}): {resp.text[:300]}")
            return False
        except Exception as e:
            logger.error(f"Webhook post exception: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 1.7, MAX_BACKOFF)

def send_comments_batched_to_chat(comments: List[dict]) -> None:
    """
    Sends comments in batches of BATCH_SIZE in ONE message per batch.
    Each comment is its own section inside the card to avoid per-comment requests.
    """
    if not MAIN_WEBHOOK or "chat.googleapis.com" not in MAIN_WEBHOOK:
        logger.warning("No valid MAIN_WEBHOOK configured.")
        return

    total = len(comments)
    sent = 0

    for start in range(0, total, BATCH_SIZE):
        batch = comments[start:start+BATCH_SIZE]
        # Build a single card with multiple sections
        sections = []
        for c in batch:
            emoji, label = _score_to_label(c.get("score", "0"))
            sections.append({
                "widgets": [
                    {"keyValue": {"topLabel": "Store", "content": f"{emoji} {c.get('store','') } ({label})"}},
                    {"keyValue": {"topLabel": "Timestamp", "content": c.get("timestamp","")}},
                    {"keyValue": {"topLabel": "Score", "content": str(c.get("score",""))}},
                    {"textParagraph": {"text": (c.get("comment") or "[No comment]").replace("\n","<br>")}}
                ]
            })

        payload = {
            "cards": [{
                "header": {
                    "title": f"NPS Comments ({start+1}-{start+len(batch)} of {total})",
                    "subtitle": "Automated report"
                },
                "sections": sections
            }]}
        ok = _post_with_backoff(MAIN_WEBHOOK, payload)
        if ok:
            sent += len(batch)
            logger.info(f"✅ Sent batch {start+1}-{start+len(batch)} (total sent: {sent}/{total})")
        else:
            logger.error("Stopping further sends due to webhook error.")
            break

############################################
# HOUSEKEEPING
############################################
def cleanup_old_screens(days: int = 14):
    """Delete old screenshots/debug HTMLs older than N days."""
    if not SCREENS_DIR.exists():
        return
    cutoff = time.time() - days * 86400
    deleted = 0
    for f in SCREENS_DIR.glob("*"):
        try:
            if f.is_file() and f.stat().st_mtime < cutoff:
                f.unlink()
                deleted += 1
        except Exception:
            pass
    if deleted:
        logger.info(f"🧹 Cleaned {deleted} old debug files (> {days} days).")

############################################
# MAIN SCRAPER (SAFE RELOGIN & BATCH SEND + LOCK)
############################################
def run_scrape(retry_on_auth_fail=True):
    """Main scraper with lock-file and cleanup; safe re-login; batched sends."""
    # ───── Lock file to prevent overlaps ─────
    if LOCK_FILE.exists():
        logger.warning("Another scrape already running — skipping this run.")
        return
    try:
        LOCK_FILE.write_text(str(os.getpid()))

        # Clean old debug dumps occasionally
        cleanup_old_screens(days=14)

        def _scrape_internal():
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(storage_state=AUTH_STATE_PATH if AUTH_STATE_PATH.exists() else None)
                page = context.new_page()

                lines = fetch_looker_text(page, LOOKER_STUDIO_URL, "scrape")

                login_wall_detected = False
                if lines is None:
                    login_wall_detected = True
                else:
                    sample = " ".join(lines[:80]).lower()
                    if "sign in" in sample or "can't access report" in sample or "please sign in" in sample:
                        login_wall_detected = True

                if login_wall_detected:
                    context.close()
                    browser.close()
                    return "RELOGIN_REQUIRED", []

                if not lines:
                    context.close()
                    browser.close()
                    return "NO_TEXT", []

                comments = parse_comments_from_lines(lines)
                context.close()
                browser.close()
                if not comments:
                    return "NO_COMMENTS", []

                return "OK", comments

        # Run main scrape
        status, comments = _scrape_internal()

        # Handle login wall
        if status == "RELOGIN_REQUIRED":
            if not retry_on_auth_fail:
                alert_login_needed("Repeated login failure — human attention required.")
                return
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)
                context = browser.new_context()
                page = context.new_page()
                success = login_and_save_state(page)
                context.close(); browser.close()
            if not success:
                alert_login_needed("Manual login failed.")
                return
            logger.info("Re-login successful — retrying scrape once cleanly.")
            run_scrape(retry_on_auth_fail=False)
            return

        if status in ("NO_TEXT", "NO_COMMENTS"):
            logger.warning("Nothing to send this run.")
            return

        # De-dupe against local log and apply run cap
        existing = read_existing_comments()
        new_comments = [c for c in comments if (c["store"], c["timestamp"], c["comment"]) not in existing]

        if not new_comments:
            logger.info("No new comments to send.")
            return

        # Apply per-run cap to avoid rate limit spikes
        capped = new_comments[:MAX_COMMENTS_PER_RUN]
        leftover = max(0, len(new_comments) - len(capped))
        if leftover > 0:
            logger.info(f"Rate safety: sending {len(capped)} now, deferring {leftover} later.")

        # Send in batches and persist
        send_comments_batched_to_chat(capped)
        append_new_comments(capped)

        # If we deferred some, send a tiny digest
        if leftover > 0 and MAIN_WEBHOOK and "chat.googleapis.com" in MAIN_WEBHOOK:
            payload = {"text": f"ℹ️ {leftover} additional comments deferred to next runs (rate safety)."}
            _post_with_backoff(MAIN_WEBHOOK, payload)

        logger.info("✅ Scrape complete.")

    finally:
        if LOCK_FILE.exists():
            try:
                LOCK_FILE.unlink()
            except Exception:
                pass

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
        schedule.run_pending()
        time.sleep(30)

############################################
# MAIN ENTRY
############################################
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() in ("now", "once", "manual"):
        logger.info("Manual trigger → running scrape now...")
        run_scrape()
        logger.info("Done.")
    else:
        schedule_scrapes()
