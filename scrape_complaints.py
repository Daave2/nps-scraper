# -*- coding: utf-8 -*-
# Filename: scrape_complaints.py

import os
import sys
import csv
import time
import logging
import re
import requests
import schedule
import configparser
from getpass import getpass
from pathlib import Path
from typing import List, Dict, Any, Optional
from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError
)

try:
    import pytz  # optional; used for timezone-aware schedule
except ImportError:
    pytz = None

# ──────────────────────────────────────────────────────────────────────────────
# PATHS & LOGGING
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
AUTH_STATE_PATH = BASE_DIR / "auth_state.json"       # shared with NPS scraper
COMPLAINTS_LOG_PATH = BASE_DIR / "complaints_log.csv"

COMPLAINT_CSV_HEADERS = [
    "case_number", "opened_date", "store", "dashboard_business_area",
    "case_type", "case_category", "case_reason", "detailed_case_reason",
    "description", "store_response"
]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(BASE_DIR / "scrape_complaints.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("complaints")

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG (file + env fallbacks)
# ──────────────────────────────────────────────────────────────────────────────
config = configparser.ConfigParser()
cfg_path = BASE_DIR / "config.ini"
if cfg_path.exists():
    try:
        config.read(cfg_path, encoding='utf-8')
    except configparser.Error as e:
        logger.critical(f"CRITICAL: Error reading config.ini: {e}")
        sys.exit(1)
else:
    logger.warning("config.ini not found — will rely on environment variables.")

GOOGLE_EMAIL       = config["DEFAULT"].get("GOOGLE_EMAIL", os.getenv("GOOGLE_EMAIL", ""))
GOOGLE_PASSWORD    = config["DEFAULT"].get("GOOGLE_PASSWORD", os.getenv("GOOGLE_PASSWORD", ""))
MAIN_WEBHOOK       = config["DEFAULT"].get("MAIN_WEBHOOK", os.getenv("MAIN_WEBHOOK", ""))
ALERT_WEBHOOK      = config["DEFAULT"].get("ALERT_WEBHOOK", os.getenv("ALERT_WEBHOOK", ""))
COMPLAINTS_WEBHOOK = (
    config["DEFAULT"].get("COMPLAINTS_WEBHOOK", os.getenv("COMPLAINTS_WEBHOOK", "")) or MAIN_WEBHOOK
)

def _redact(url: str) -> str:
    if not url:
        return ""
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        tail = url[-6:]
        return f"{p.netloc}…{tail}"
    except Exception:
        return url[:3] + "…"

logger.info(f"Complaints webhook in use: {_redact(COMPLAINTS_WEBHOOK) or '[MISSING]'}")
logger.info(f"Main webhook in use: {_redact(MAIN_WEBHOOK) or '[MISSING]'}")

if not COMPLAINTS_WEBHOOK:
    logger.critical("No COMPLAINTS_WEBHOOK or MAIN_WEBHOOK configured — cannot post complaints.")
    sys.exit(1)

# Complaints report URL (adjust if needed)
LOOKER_STUDIO_URL = "https://lookerstudio.google.com/embed/reporting/d93a03c7-25dc-439d-abaa-dd2f3780daa5/page/p_qwk7izlsld"

# Timeouts (ms)
DEFAULT_NAVIGATION_TIMEOUT = 60_000
DEFAULT_SELECTOR_TIMEOUT   = 30_000
LOGIN_SUCCESS_TIMEOUT      = 120_000
POST_NAVIGATION_WAIT       = 15_000

# Optional CI run URL injected by workflow
CI_RUN_URL = os.getenv("CI_RUN_URL", "")

# ──────────────────────────────────────────────────────────────────────────────
# ALERT HELPERS
# ──────────────────────────────────────────────────────────────────────────────
def _post_with_backoff(url: str, payload: Dict[str, Any]) -> bool:
    backoff = 2.0
    max_backoff = 30.0
    while True:
        try:
            r = requests.post(url, json=payload, timeout=20)
            if r.status_code == 200:
                return True
            if r.status_code == 429:
                delay = min(float(r.headers.get("Retry-After", backoff)), max_backoff)
                logger.error(f"429 from webhook — sleeping {delay:.1f}s")
                time.sleep(delay)
                backoff = min(backoff * 1.7, max_backoff)
                continue
            logger.error(f"Webhook error {r.status_code}: {r.text[:300]}")
            return False
        except Exception as e:
            logger.error(f"Webhook exception: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 1.7, max_backoff)

def send_alert(webhook_url: str, message: str):
    if not webhook_url or "chat.googleapis.com" not in webhook_url:
        logger.warning("Cannot send alert: invalid/missing webhook URL.")
        return
    lines = [message]
    if CI_RUN_URL:
        lines.append(f"• CI run: {CI_RUN_URL}")
    _post_with_backoff(webhook_url, {"text": "\n".join(lines)})

def alert_login_needed(reason="Unknown reason"):
    send_alert(ALERT_WEBHOOK, f"🚨 LOGIN REQUIRED (Complaints): {reason}")

def remove_auth_file():
    try:
        if AUTH_STATE_PATH.exists():
            AUTH_STATE_PATH.unlink()
            logger.info(f"Removed auth state file: {AUTH_STATE_PATH}")
    except Exception as e:
        logger.error(f"Failed to remove auth file: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# LOGIN (SHARED AUTH)
# ──────────────────────────────────────────────────────────────────────────────
def login_and_save_state(page) -> bool:
    """Manual Google login; saves shared auth_state.json."""
    import re as _re
    logger.info("Starting manual Google login (complaints)...")
    try:
        page.goto("https://accounts.google.com/", timeout=DEFAULT_NAVIGATION_TIMEOUT)

        try:
            page.wait_for_selector("input[type='email']", timeout=DEFAULT_SELECTOR_TIMEOUT)
        except PlaywrightTimeoutError:
            logger.error("Email input not found.")
            return False
        page.fill("input[type='email']", GOOGLE_EMAIL or input("Google Email: "))
        page.keyboard.press("Enter")

        try:
            page.wait_for_selector("input[type='password']", timeout=DEFAULT_SELECTOR_TIMEOUT)
        except PlaywrightTimeoutError:
            logger.error("Password input not found.")
            return False
        pw = GOOGLE_PASSWORD or getpass("Google Password: ")
        page.fill("input[type='password']", pw)
        page.keyboard.press("Enter")

        logger.info("Waiting for account page (complete 2FA if prompted)...")
        page.wait_for_url(_re.compile(r"https://myaccount\.google\.com/.*"), timeout=LOGIN_SUCCESS_TIMEOUT)

        # Touch Looker Studio domain so cookies persist for that domain too
        page.goto("https://lookerstudio.google.com/", timeout=60_000, wait_until="domcontentloaded")
        page.context.storage_state(path=str(AUTH_STATE_PATH))
        logger.info("✅ Login complete; saved auth_state.json")
        return True
    except PlaywrightTimeoutError:
        logger.error("Login timed out — did not reach Google account page.")
        return False
    except Exception as e:
        logger.error(f"Unexpected login error: {e}", exc_info=True)
        return False

# ──────────────────────────────────────────────────────────────────────────────
# FETCH TEXT FROM LOOKER STUDIO
# ──────────────────────────────────────────────────────────────────────────────
def copy_looker_studio_text(page, target_url: str) -> Optional[List[str]]:
    logger.info(f"Navigating to Complaints report: {target_url}")
    page_text = ""
    try:
        resp = page.goto(target_url, timeout=DEFAULT_NAVIGATION_TIMEOUT, wait_until='load')
        logger.info(f"Initial HTTP status: {resp.status if resp else 'N/A'}")

        if any(s in page.url for s in ("accounts.google.com", "/signin/", "ServiceLogin")):
            logger.warning("Redirected to Google login — auth invalid.")
            return None

        logger.info(f"Waiting {POST_NAVIGATION_WAIT//1000}s for dynamic content…")
        page.wait_for_timeout(POST_NAVIGATION_WAIT)

        if any(s in page.url for s in ("accounts.google.com", "/signin/", "ServiceLogin")):
            logger.warning("Redirected to Google login after wait — auth invalid.")
            return None

        # Try main body
        try:
            page.wait_for_selector("body", timeout=10_000)
            page_text = page.locator("body").inner_text(timeout=15_000)
            logger.info(f"Main body lines: {len(page_text.splitlines())}")
        except Exception as e:
            logger.warning(f"Main body text not available ({e}); trying frames.")
            page_text = ""

        if any(x in page_text for x in ("Please sign in", "Can't access report", "You need permission")):
            logger.warning("Login/permission prompt detected in body.")
            return None

        # Try frames — prefer the longest plausible one
        best = page_text
        best_len = len(best.splitlines()) if best else 0
        for i, frame in enumerate(page.frames):
            if i == 0:
                continue
            try:
                if frame.is_detached():
                    continue
                f_url = frame.url
                plausible = any(k in f_url for k in ("lookerstudio.google.com", "datastudio.google.com", "apphosting", "sandbox")) or f_url == "about:blank"
                if "google.com/recaptcha" in f_url:
                    plausible = False
                if not plausible:
                    continue
                try:
                    frame.wait_for_selector("body", timeout=5_000)
                    f_text = frame.locator("body").inner_text(timeout=30_000)
                except PlaywrightTimeoutError:
                    continue

                if any(x in f_text for x in ("Please sign in", "Can't access report", "You need permission")):
                    logger.warning("Login/permission prompt detected in a frame.")
                    return None

                f_len = len(f_text.splitlines())
                if f_len > best_len + 10 or (f_len > 5 and best_len == 0):
                    best, best_len = f_text, f_len
            except Exception:
                continue

        if not best:
            logger.warning("No text extracted from page or frames.")
            try:
                page.screenshot(path=str(BASE_DIR / "screenshot_complaints_no_content.png"))
            except Exception:
                pass
            return []

        lines = best.splitlines()
        logger.info(f"Final extracted text lines: {len(lines)}")
        try:
            (BASE_DIR / "screens").mkdir(exist_ok=True)
            (BASE_DIR / "screens" / f"{int(time.time())}_complaints_text.txt").write_text(best, encoding="utf-8")
        except Exception:
            pass
        return lines

    except Exception as e:
        logger.error(f"Unexpected navigation/extract error: {e}", exc_info=True)
        try:
            page.screenshot(path=str(BASE_DIR / "screenshot_complaints_unexpected_error.png"))
        except Exception:
            pass
        return []

# ──────────────────────────────────────────────────────────────────────────────
# PARSER
# ──────────────────────────────────────────────────────────────────────────────
def parse_complaints_from_lines(lines: List[str]) -> List[Dict[str, str]]:
    if not lines:
        return []

    date_re = re.compile(r"^\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4},\s+\d{2}:\d{2}:\d{2}$", re.I)
    case_num_re = re.compile(r"^\d+$")
    list_item_re = re.compile(r"^\d+\.$")
    end_marker_re = re.compile(r"^(Respond|under review|null)$", re.I)
    pagination_re = re.compile(r"^\d+\s+-\s+\d+\s*/\s*\d+")
    header_re = re.compile(r"^(opened_date|store|case_number|dashboard_business_area|case_type|case_category|case_reason|detailed_case_reason|description|response_url|store_response)$", re.I)

    out: List[Dict[str, str]] = []
    n = len(lines)
    i = 0
    state = "LOOKING_FOR_START"
    cur: Dict[str, str] = {}
    desc: List[str] = []
    resp: List[str] = []

    while i < n:
        line = lines[i].strip()
        i += 1

        if not line or pagination_re.match(line) or header_re.match(line):
            continue

        if state == "LOOKING_FOR_START":
            if list_item_re.match(line):
                state = "FOUND_LIST_ITEM"
            continue

        if state == "FOUND_LIST_ITEM":
            if date_re.match(line):
                cur = {"opened_date": line}
                desc, resp = [], []
                state = "FOUND_DATE"
            else:
                state = "LOOKING_FOR_START"
            continue

        if state == "FOUND_DATE":
            cur["store"] = line
            state = "FOUND_STORE"
            continue

        if state == "FOUND_STORE":
            if case_num_re.match(line):
                cur["case_number"] = line
                state = "FOUND_CASE"
            else:
                state = "LOOKING_FOR_START"
                cur = {}
            continue

        if state == "FOUND_CASE":
            cur["dashboard_business_area"] = line; state = "FOUND_AREA"; continue
        if state == "FOUND_AREA":
            cur["case_type"] = line; state = "FOUND_TYPE"; continue
        if state == "FOUND_TYPE":
            cur["case_category"] = line; state = "FOUND_CAT"; continue
        if state == "FOUND_CAT":
            cur["case_reason"] = line; state = "FOUND_REASON"; continue
        if state == "FOUND_REASON":
            cur["detailed_case_reason"] = line; state = "READING_DESC"; continue

        if state == "READING_DESC":
            if end_marker_re.match(line) or list_item_re.match(line) or date_re.match(line):
                cur["description"] = "\n".join(desc).strip()
                if list_item_re.match(line) or date_re.match(line):
                    cur["store_response"] = "[No response recorded]"
                    out.append(cur)
                    cur = {}
                    i -= 1
                    state = "LOOKING_FOR_START"
                else:
                    state = "READING_RESPONSE"
            else:
                desc.append(line)
            continue

        if state == "READING_RESPONSE":
            if list_item_re.match(line) or date_re.match(line):
                cur["store_response"] = ("\n".join(resp).strip() or "[No response recorded]")
                out.append(cur)
                cur = {}
                i -= 1
                state = "LOOKING_FOR_START"
            else:
                if line.lower() != "null":
                    resp.append(line)
            continue

    if state == "READING_RESPONSE" and cur.get("case_number"):
        cur["store_response"] = ("\n".join(resp).strip() or "[No response recorded]")
        out.append(cur)

    logger.info(f"Parsed {len(out)} complaint(s).")
    return out

# ──────────────────────────────────────────────────────────────────────────────
# CSV LOG
# ──────────────────────────────────────────────────────────────────────────────
def read_existing_complaints() -> set:
    existing = set()
    if not COMPLAINTS_LOG_PATH.exists():
        try:
            with open(COMPLAINTS_LOG_PATH, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(COMPLAINT_CSV_HEADERS)
            logger.info(f"Created {COMPLAINTS_LOG_PATH} with headers.")
        except Exception as e:
            logger.error(f"Failed to create {COMPLAINTS_LOG_PATH}: {e}")
        return existing

    try:
        with open(COMPLAINTS_LOG_PATH, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                num = (row.get("case_number") or "").strip()
                if num:
                    existing.add(num)
    except Exception as e:
        logger.error(f"Error reading {COMPLAINTS_LOG_PATH}: {e}")
    return existing

def append_new_complaints(new_rows: List[Dict[str, str]]):
    if not new_rows:
        return
    try:
        write_header = not COMPLAINTS_LOG_PATH.exists() or COMPLAINTS_LOG_PATH.stat().st_size == 0
        with open(COMPLAINTS_LOG_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COMPLAINT_CSV_HEADERS, quoting=csv.QUOTE_MINIMAL)
            if write_header:
                writer.writeheader()
            for r in new_rows:
                writer.writerow({k: r.get(k, "") for k in COMPLAINT_CSV_HEADERS})
        logger.info(f"Appended {len(new_rows)} complaint(s) to {COMPLAINTS_LOG_PATH}.")
    except Exception as e:
        logger.error(f"Failed to write complaints CSV: {e}")
        send_alert(ALERT_WEBHOOK, f"🚨 ERROR writing {COMPLAINTS_LOG_PATH}: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# SENDER
# ──────────────────────────────────────────────────────────────────────────────
def send_complaint_to_google_chat(complaint: Dict[str, str]) -> bool:
    if not COMPLAINTS_WEBHOOK or "chat.googleapis.com" not in COMPLAINTS_WEBHOOK:
        logger.error("Complaints webhook invalid.")
        return False

    def esc(s: str) -> str:
        return (s or "").replace("<", "&lt;").replace(">", "&gt;")

    case_num    = esc(complaint.get("case_number", "[Unknown Case]"))
    opened_date = esc(complaint.get("opened_date", "[Unknown Date]"))
    store       = esc(complaint.get("store", "[Unknown Store]"))
    category    = esc(complaint.get("case_category", "N/A"))
    reason      = esc(complaint.get("detailed_case_reason") or complaint.get("case_reason", "N/A"))
    description = esc(complaint.get("description", "[No Description]"))
    response    = esc(complaint.get("store_response", "")).strip()

    if len(description) > 700:
        description = description[:700] + "... (truncated)"
    if response:
        if len(response) > 500:
            response = response[:500] + "... (truncated)"
    else:
        response = "<i>No store response recorded yet.</i>"

    payload = {
        "cardsV2": [{
            "cardId": f"complaint_{case_num}_{int(time.time())}",
            "card": {
                "header": {
                    "title": f"🚨 New Customer Complaint (#{case_num})",
                    "subtitle": store
                },
                "sections": [{
                    "widgets": [
                        {"decoratedText": {"topLabel": "Date Opened", "text": opened_date, "startIcon": {"knownIcon": "CLOCK"}}},
                        {"decoratedText": {"topLabel": "Category", "text": category, "startIcon": {"knownIcon": "DESCRIPTION"}}},
                        {"decoratedText": {"topLabel": "Reason", "text": reason, "startIcon": {"knownIcon": "TICKET"}}},
                        {"textParagraph": {"text": f"<b>Description:</b><br>{description.replace(chr(10), '<br>')}"}},
                        {"textParagraph": {"text": f"<b>Store Response:</b><br>{response.replace(chr(10), '<br>')}" }}
                    ]
                }]
            }
        }]
    }

    return _post_with_backoff(COMPLAINTS_WEBHOOK, payload)

# ──────────────────────────────────────────────────────────────────────────────
# WORKFLOW
# ──────────────────────────────────────────────────────────────────────────────
def perform_scrape_workflow():
    auth_ok = AUTH_STATE_PATH.exists()
    if not auth_ok:
        logger.info("auth_state.json missing — manual login needed for complaints.")
        alert_login_needed("Auth state missing for complaints.")

        with sync_playwright() as p:
            browser = context = page = None
            try:
                browser = p.chromium.launch(headless=False)
                context = browser.new_context(viewport={'width': 1280, 'height': 800})
                page = context.new_page()
                if not login_and_save_state(page):
                    logger.critical("Manual login failed.")
                    return
            except Exception as e:
                logger.error(f"Login setup error: {e}", exc_info=True)
                return
            finally:
                try:
                    if context: context.close()
                except Exception: pass
                try:
                    if browser: browser.close()
                except Exception: pass

    # Headless scrape
    lines: Optional[List[str]] = None
    with sync_playwright() as p:
        browser = context = page = None
        try:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(storage_state=str(AUTH_STATE_PATH), viewport={'width': 1366, 'height': 768})
            context.set_default_navigation_timeout(DEFAULT_NAVIGATION_TIMEOUT)
            context.set_default_timeout(DEFAULT_SELECTOR_TIMEOUT)
            page = context.new_page()
            lines = copy_looker_studio_text(page, LOOKER_STUDIO_URL)
        except Exception as e:
            logger.error(f"Headless scrape error: {e}", exc_info=True)
            send_alert(ALERT_WEBHOOK, f"🚨 Complaints scrape error: {e}")
        finally:
            try:
                if context: context.close()
            except Exception: pass
            try:
                if browser: browser.close()
            except Exception: pass

    if lines is None:
        logger.error("Auth required or invalid during complaints scrape.")
        if AUTH_STATE_PATH.exists():
            remove_auth_file()
        alert_login_needed("Saved session invalid during complaints scrape.")
        return

    if not lines:
        logger.info("No text extracted from complaints report.")
        return

    complaints = parse_complaints_from_lines(lines)
    if not complaints:
        logger.info("No complaint blocks parsed.")
        return

    existing = read_existing_complaints()
    new_items = [c for c in complaints if c.get("case_number", "").strip() and c["case_number"] not in existing]
    if not new_items:
        logger.info("No new complaints to send.")
        return

    sent = 0
    for idx, c in enumerate(new_items, 1):
        ok = send_complaint_to_google_chat(c)
        logger.info(f"Send {idx}/{len(new_items)} -> {'OK' if ok else 'FAIL'}")
        if ok:
            sent += 1
            time.sleep(1.5)
    if sent:
        append_new_complaints([c for c in new_items])

    logger.info(f"Complaints scrape finished — sent {sent}/{len(new_items)} new complaints.")

# ──────────────────────────────────────────────────────────────────────────────
# SCHEDULER
# ──────────────────────────────────────────────────────────────────────────────
def schedule_complaint_scrapes():
    times = ["08:15", "11:15", "14:15", "17:15", "20:15"]  # offset from NPS
    for t in times:
        schedule.every().day.at(t).do(perform_scrape_workflow)
    logger.info(f"Complaint scheduler configured for: {', '.join(times)}")
    while True:
        try:
            schedule.run_pending()
            time.sleep(30)
        except KeyboardInterrupt:
            logger.info("Scheduler stopped.")
            break
        except Exception as e:
            logger.error(f"Scheduler loop error: {e}", exc_info=True)
            send_alert(ALERT_WEBHOOK, f"🚨 Complaints scheduler error: {e}")
            time.sleep(60)

# ──────────────────────────────────────────────────────────────────────────────
# ENTRY
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() in ("now", "once", "run"):
        logger.info("Manual complaints run triggered.")
        perform_scrape_workflow()
    elif len(sys.argv) > 1 and sys.argv[1].lower() == "login":
        logger.info("Manual login requested — removing auth and starting login flow.")
        remove_auth_file()
        perform_scrape_workflow()
    else:
        logger.info("Starting complaints scheduler…")
        schedule_complaint_scrapes()
