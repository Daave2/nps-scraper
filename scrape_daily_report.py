#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary → Google Chat

Pulls as many metrics as possible from the Retail Performance Dashboard page and
sends a single daily summary card. Also logs a CSV row for history.

- Reuses the same Playwright auth_state.json (shared Google login)
- Extracts values with robust regexes + table-aware snippets
- Emits ALL sections even if some values are missing (shown as "—")
"""

import os
import re
import csv
import time
import logging
import configparser
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ──────────────────────────────────────────────────────────────────────────────
# Paths / constants
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
AUTH_STATE_PATH = BASE_DIR / "auth_state.json"
LOG_FILE_PATH = BASE_DIR / "scrape_daily.log"
DAILY_LOG_CSV = BASE_DIR / "daily_report_log.csv"
SCREENS_DIR = BASE_DIR / "screens"

DASHBOARD_URL = (
    "https://lookerstudio.google.com/embed/u/0/reporting/"
    "d93a03c7-25dc-439d-abaa-dd2f3780daa5/page/BLfDE"
    "?params=%7B%22f20f0n9kld%22:%22include%25EE%2580%25803%25EE%2580%2580T%22%7D"
)

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE_PATH),],
)
logger = logging.getLogger("daily")

# Also echo to stdout in CI/local
stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.INFO)
stdout_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(stdout_handler)

# ──────────────────────────────────────────────────────────────────────────────
# Config (file first, env fallback)
# ──────────────────────────────────────────────────────────────────────────────
config = configparser.ConfigParser()
config.read(BASE_DIR / "config.ini")

MAIN_WEBHOOK   = config["DEFAULT"].get("MAIN_WEBHOOK",   os.getenv("MAIN_WEBHOOK", ""))
ALERT_WEBHOOK  = config["DEFAULT"].get("ALERT_WEBHOOK",  os.getenv("ALERT_WEBHOOK", ""))
GOOGLE_EMAIL   = config["DEFAULT"].get("GOOGLE_EMAIL",   os.getenv("GOOGLE_EMAIL", ""))
GOOGLE_PASSWORD= config["DEFAULT"].get("GOOGLE_PASSWORD",os.getenv("GOOGLE_PASSWORD", ""))

CI_RUN_URL = os.getenv("CI_RUN_URL", "")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers (Chat + debug)
# ──────────────────────────────────────────────────────────────────────────────
def _post_with_backoff(url: str, payload: dict) -> bool:
    backoff, max_backoff = 2.0, 30.0
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

def alert(lines: List[str]):
    if not ALERT_WEBHOOK or "chat.googleapis.com" not in ALERT_WEBHOOK:
        logger.warning("No valid ALERT_WEBHOOK configured.")
        return
    if CI_RUN_URL:
        lines.append(f"• CI run: {CI_RUN_URL}")
    _post_with_backoff(ALERT_WEBHOOK, {"text": "\n".join(lines)})

def dump_debug(page, tag: str):
    try:
        ts = int(time.time())
        SCREENS_DIR.mkdir(parents=True, exist_ok=True)
        png = SCREENS_DIR / f"{ts}_{tag}.png"
        html = SCREENS_DIR / f"{ts}_{tag}.html"
        page.screenshot(path=str(png), full_page=True)
        html.write_text(page.content(), encoding="utf-8")
        logger.info(f"Saved debug snapshot → {png.name}, {html.name}")
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────────────────────
# Navigation + text capture
# ──────────────────────────────────────────────────────────────────────────────
def fetch_text_from_dashboard(page, url: str) -> Optional[str]:
    logger.info("Opening Retail Performance Dashboard…")
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=120_000)
    except TimeoutError:
        logger.error("Timeout loading dashboard.")
        dump_debug(page, "dashboard_timeout")
        return ""

    if "accounts.google.com" in page.url:
        logger.warning("Redirected to login — auth state missing/invalid.")
        return None

    # Give charts/frames time to render some text
    logger.info("Waiting 15s for dynamic content…")
    page.wait_for_timeout(15_000)

    # Try page body first
    text = ""
    try:
        text = page.inner_text("body")
    except Exception:
        pass

    # If very short, dive into frames and pick the longest meaningful text
    if not text or len(text) < 200:
        best = text
        best_len = len(best) if best else 0
        for fr in page.frames:
            try:
                fr.wait_for_selector("body", timeout=5_000)
                t = fr.locator("body").inner_text(timeout=20_000)
                if t and len(t) > best_len:
                    best, best_len = t, len(t)
            except Exception:
                continue
        text = best or text

    if not text:
        logger.error("No text content found.")
        dump_debug(page, "no_text")
        return ""

    try:
        (SCREENS_DIR / f"{int(time.time())}_daily_text.txt").write_text(text, encoding="utf-8")
    except Exception:
        pass

    return text

# ──────────────────────────────────────────────────────────────────────────────
# Parsing utilities
# ──────────────────────────────────────────────────────────────────────────────
def _search_first(patterns: List[str], text: str, flags=0, group: int = 1) -> str:
    for pat in patterns:
        m = re.search(pat, text, flags)
        if m:
            try:
                return m.group(group).strip()
            except Exception:
                return m.group(0).strip()
    return "—"

def _num(text: str) -> str:
    # compact normaliser for tokens like 465.1K / -1.4K / 83% / 07:57
    return text.strip()

def parse_metrics(text: str) -> Dict[str, str]:
    """
    Pulls as many values as possible from the dashboard text.
    Returns a flat dict of metric -> string value. Missing = "—".
    """
    out: Dict[str, str] = {}

    # Header context / period
    out["period_range"] = _search_first(
        [r"The data on this report is from:\s*([^\n]+)"], text
    )
    out["page_timestamp"] = _search_first(
        [r"\b(\d{1,2}\s+[A-Za-z]{3}\s+\d{4},\s*\d{2}:\d{2}:\d{2})\b"], text
    )
    out["store_line"] = _search_first(
        [r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}).*?(\|\s*.+?\s*\|\s*\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})"],
        text,
        flags=re.S,
        group=0,
    )

    # Sales (Total row)
    # We try to capture "Total" row: value, LFL, vs Target
    sales_total = _search_first(
        [
            r"Total\s*\n\s*([£]?[0-9.,]+[KMB]?)\s*\n\s*([+-]?\d+%?)\s*\n\s*([+-]?[£]?[0-9.,]+[KMB]?)"
        ],
        text,
        flags=re.I,
    )
    if sales_total != "—":
        # If successful, split components back out
        m = re.search(
            r"Total\s*\n\s*([£]?[0-9.,]+[KMB]?)\s*\n\s*([+-]?\d+%?)\s*\n\s*([+-]?[£]?[0-9.,]+[KMB]?)",
            text,
            flags=re.I,
        )
        if m:
            out["sales_total"] = _num(m.group(1))
            out["sales_lfl"] = _num(m.group(2))
            out["sales_vs_target"] = _num(m.group(3))
    else:
        # Fallback: find a line that looks like the 465.1K / 5% / -2.7K near "Sales"
        m = re.search(
            r"Sales.*?\b([£]?[0-9.,]+[KMB]?)\b.*?\b([+-]?\d+%)\b.*?\b([+-]?[£]?[0-9.,]+[KMB]?)\b",
            text,
            flags=re.I | re.S,
        )
        if m:
            out["sales_total"] = _num(m.group(1))
            out["sales_lfl"] = _num(m.group(2))
            out["sales_vs_target"] = _num(m.group(3))
        else:
            out["sales_total"] = out["sales_lfl"] = out["sales_vs_target"] = "—"

    # NPS tiles (big dials often show a single number)
    out["supermarket_nps"] = _search_first([r"\bSupermarket NPS\b.*?\b(-?\d+)\b"], text, flags=re.I | re.S)
    out["colleague_happiness"] = _search_first([r"\bColleague Happiness\b.*?\b(-?\d+)\b"], text, flags=re.I | re.S)
    out["home_delivery_nps"] = _search_first([r"\bHome Delivery NPS\b.*?\b(-?\d+)\b"], text, flags=re.I | re.S)
    out["cafe_nps"] = _search_first([r"\bCafe NPS\b.*?\b(-?\d+)\b"], text, flags=re.I | re.S)
    out["click_collect_nps"] = _search_first([r"\bClick\s*&\s*Collect NPS\b.*?\b(-?\d+)\b"], text, flags=re.I | re.S)
    out["customer_toilet_nps"] = _search_first([r"\bCustomer Toilet NPS\b.*?\b(-?\d+)\b"], text, flags=re.I | re.S)

    # Front End Service
    out["sco_utilisation"] = _search_first([r"\bSco Utilisation\b\s*([0-9]+%)"], text, flags=re.I)
    out["efficiency"] = _search_first([r"\bEfficiency\b\s*([0-9]+%)"], text, flags=re.I)
    out["scan_rate"] = _search_first([r"\bScan Rate\b\s*([0-9]+)"], text, flags=re.I)
    out["scan_vs_target"] = _search_first([r"Scan Rate\s*[0-9]+\s*\n\s*vs Target\s*([+-]?[0-9.]+)"], text, flags=re.I)
    out["interventions"] = _search_first([r"\bInterventions\b\s*([0-9]+)"], text, flags=re.I)
    out["interventions_vs_target"] = _search_first([r"Interventions\s*[0-9]+\s*\n\s*vs Target\s*([+-]?[0-9.]+)"], text, flags=re.I)
    out["mainbank_closed"] = _search_first([r"\bMainbank Closed\b\s*([0-9]+)"], text, flags=re.I)
    out["mainbank_vs_target"] = _search_first([r"Mainbank Closed\s*[0-9]+\s*\n\s*vs Target\s*([+-]?[0-9.]+)"], text, flags=re.I)

    # Online
    out["availability_pct"] = _search_first([r"\bAvailability\b\s*([0-9]+%)"], text, flags=re.I)
    out["despatched_on_time"] = _search_first([r"\bDespatched on Time\b\s*([0-9]+%|No data)"], text, flags=re.I)
    out["delivered_on_time"] = _search_first([r"\bDelivered on Time\b\s*([0-9]+%|No data)"], text, flags=re.I)
    out["cc_avg_wait"] = _search_first([r"\bClick\s*&\s*Collect average wait\b\s*([0-9]{2}:[0-9]{2})"], text, flags=re.I)

    # Waste & Markdowns (Total row)
    # Expect:
    #   Total
    #   2.9K
    #   3K
    #   5.9K
    #   -1.2K
    #   -25.69%
    m = re.search(
        r"Waste\s*&\s*Markdowns.*?Total\s*\n\s*([£]?[0-9.,]+[KMB]?)\s*\n\s*([£]?[0-9.,]+[KMB]?)\s*\n\s*([£]?[0-9.,]+[KMB]?)\s*\n\s*([+-]?[£]?[0-9.,]+[KMB]?)\s*\n\s*([+-]?\d+\.?\d*%)",
        text,
        flags=re.I | re.S,
    )
    if m:
        out["waste_total"] = _num(m.group(1))
        out["markdowns_total"] = _num(m.group(2))
        out["wm_total"] = _num(m.group(3))
        out["wm_delta"] = _num(m.group(4))
        out["wm_delta_pct"] = _num(m.group(5))
    else:
        out.update({k: "—" for k in ["waste_total", "markdowns_total", "wm_total", "wm_delta", "wm_delta_pct"]})

    # Shrink (circle row of KPIs)
    out["moa"] = _search_first([r"\bMorrisons Order Adjustments\b\s*([£]?-?[0-9.,]+[KMB]?)"], text, flags=re.I)
    out["waste_validation"] = _search_first([r"\bWaste Validation\b\s*([0-9]+%)"], text, flags=re.I)
    out["unrecorded_waste_pct"] = _search_first([r"\bUnrecorded Waste %\b\s*([+-]?\d+\.?\d*%)"], text, flags=re.I)
    out["shrink_vs_budget_pct"] = _search_first([r"\bShrink vs Budget %\b\s*([+-]?\d+\.?\d*%)"], text, flags=re.I)

    # Payroll panel
    out["payroll_outturn"] = _search_first([r"\bPayroll Outturn\b\s*([+-]?[£]?[0-9.,]+[KMB]?)"], text, flags=re.I)
    out["absence_outturn"] = _search_first([r"\bAbsence Outturn\b\s*([+-]?[£]?[0-9.,]+[KMB]?)"], text, flags=re.I)
    out["productive_outturn"] = _search_first([r"\bProductive Outturn\b\s*([+-]?[£]?[0-9.,]+[KMB]?)"], text, flags=re.I)
    out["holiday_outturn"] = _search_first([r"\bHoliday Outturn\b\s*([+-]?[£]?[0-9.,]+[KMB]?)"], text, flags=re.I)
    out["current_base_cost"] = _search_first([r"\bCurrent Base Cost\b\s*([£]?[0-9.,]+[KMB]?)"], text, flags=re.I)

    # Card Engagement
    out["swipe_rate"] = _search_first([r"\bSwipe Rate\b\s*([0-9]+%)"], text, flags=re.I)
    out["swipes_wow_pct"] = _search_first([r"\bSwipes WOW %\b\s*([+-]?\d+%)"], text, flags=re.I)
    out["new_customers"] = _search_first([r"\bNew Customers\b\s*([0-9]+)"], text, flags=re.I)
    out["swipes_yoy_pct"] = _search_first([r"\bSwipes YOY %\b\s*([+-]?\d+%)"], text, flags=re.I)

    # Production Planning
    out["data_provided"] = _search_first([r"\bData Provided\b\s*([0-9]+%)"], text, flags=re.I)
    out["trusted_data"] = _search_first([r"\bTrusted Data\b\s*([0-9]+%)"], text, flags=re.I)

    # Misc
    out["complaints_key"] = _search_first([r"\bKey Customer Complaints\b\s*([0-9]+)"], text, flags=re.I)
    out["my_reports"] = _search_first([r"\bMy Reports\b\s*([0-9]+)"], text, flags=re.I)
    out["weekly_activity"] = _search_first([r"\bWeekly Activity %\b\s*([0-9]+%|No data)"], text, flags=re.I)

    return out

# ──────────────────────────────────────────────────────────────────────────────
# Card builder + sender
# ──────────────────────────────────────────────────────────────────────────────
def build_chat_card(metrics: Dict[str, str]) -> dict:
    title_date = metrics.get("page_timestamp", "Today")
    subtitle = metrics.get("store_line", "").replace("\n", "  ")

    def kv(label: str, val: str) -> dict:
        return {"decoratedText": {"topLabel": label, "text": val if val else "—"}}

    sections = [
        {
            "header": {"title": f"Retail Daily Summary — {title_date}", "subtitle": subtitle},
            "widgets": []
        },
        {
            "header": {"title": "Sales & NPS"},
            "widgets": [
                kv("Period", metrics.get("period_range", "—")),
                kv("Sales Total", metrics.get("sales_total", "—")),
                kv("LFL", metrics.get("sales_lfl", "—")),
                kv("vs Target", metrics.get("sales_vs_target", "—")),
                kv("Supermarket NPS", metrics.get("supermarket_nps", "—")),
                kv("Colleague Happiness", metrics.get("colleague_happiness", "—")),
                kv("Home Delivery NPS", metrics.get("home_delivery_nps", "—")),
                kv("Cafe NPS", metrics.get("cafe_nps", "—")),
                kv("Click & Collect NPS", metrics.get("click_collect_nps", "—")),
                kv("Customer Toilet NPS", metrics.get("customer_toilet_nps", "—")),
            ],
        },
        {
            "header": {"title": "Front End Service"},
            "widgets": [
                kv("SCO Utilisation", metrics.get("sco_utilisation", "—")),
                kv("Efficiency", metrics.get("efficiency", "—")),
                kv("Scan Rate", f"{metrics.get('scan_rate','—')} (vs {metrics.get('scan_vs_target','—')})"),
                kv("Interventions", f"{metrics.get('interventions','—')} (vs {metrics.get('interventions_vs_target','—')})"),
                kv("Mainbank Closed", f"{metrics.get('mainbank_closed','—')} (vs {metrics.get('mainbank_vs_target','—')})"),
            ],
        },
        {
            "header": {"title": "Online"},
            "widgets": [
                kv("Availability", metrics.get("availability_pct", "—")),
                kv("Despatched on Time", metrics.get("despatched_on_time", "—")),
                kv("Delivered on Time", metrics.get("delivered_on_time", "—")),
                kv("Click & Collect Avg Wait", metrics.get("cc_avg_wait", "—")),
            ],
        },
        {
            "header": {"title": "Waste & Markdowns (Total)"},
            "widgets": [
                kv("Waste", metrics.get("waste_total", "—")),
                kv("Markdowns", metrics.get("markdowns_total", "—")),
                kv("Total", metrics.get("wm_total", "—")),
                kv("+/−", metrics.get("wm_delta", "—")),
                kv("+/− %", metrics.get("wm_delta_pct", "—")),
            ],
        },
        {
            "header": {"title": "Payroll"},
            "widgets": [
                kv("Payroll Outturn", metrics.get("payroll_outturn", "—")),
                kv("Absence Outturn", metrics.get("absence_outturn", "—")),
                kv("Productive Outturn", metrics.get("productive_outturn", "—")),
                kv("Holiday Outturn", metrics.get("holiday_outturn", "—")),
                kv("Current Base Cost", metrics.get("current_base_cost", "—")),
            ],
        },
        {
            "header": {"title": "Shrink"},
            "widgets": [
                kv("Morrisons Order Adjustments", metrics.get("moa", "—")),
                kv("Waste Validation", metrics.get("waste_validation", "—")),
                kv("Unrecorded Waste %", metrics.get("unrecorded_waste_pct", "—")),
                kv("Shrink vs Budget %", metrics.get("shrink_vs_budget_pct", "—")),
            ],
        },
        {
            "header": {"title": "Card Engagement & Misc"},
            "widgets": [
                kv("Swipe Rate", metrics.get("swipe_rate", "—")),
                kv("Swipes WOW %", metrics.get("swipes_wow_pct", "—")),
                kv("New Customers", metrics.get("new_customers", "—")),
                kv("Swipes YOY %", metrics.get("swipes_yoy_pct", "—")),
                kv("Key Complaints", metrics.get("complaints_key", "—")),
                kv("Data Provided", metrics.get("data_provided", "—")),
                kv("Trusted Data", metrics.get("trusted_data", "—")),
                kv("My Reports", metrics.get("my_reports", "—")),
                kv("Weekly Activity %", metrics.get("weekly_activity", "—")),
            ],
        },
    ]

    return {"cardsV2": [{"cardId": f"daily_{int(time.time())}", "card": {"sections": sections}}]}

def send_daily_card(metrics: Dict[str, str]) -> bool:
    if not MAIN_WEBHOOK or "chat.googleapis.com" not in MAIN_WEBHOOK:
        logger.error("MAIN_WEBHOOK missing/invalid — cannot send daily report.")
        return False
    payload = build_chat_card(metrics)
    return _post_with_backoff(MAIN_WEBHOOK, payload)

# ──────────────────────────────────────────────────────────────────────────────
# Main flow
# ──────────────────────────────────────────────────────────────────────────────
def run_daily_scrape():
    # Must have an auth state (created by NPS/Complaints flow when you logged in).
    if not AUTH_STATE_PATH.exists():
        alert(["⚠️ Daily dashboard scrape needs login. Run `python scrape.py now` once to save auth_state.json."])
        logger.error("auth_state.json not found.")
        return

    with sync_playwright() as p:
        browser = context = page = None
        try:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(storage_state=str(AUTH_STATE_PATH))
            page = context.new_page()
            text = fetch_text_from_dashboard(page, DASHBOARD_URL)
        finally:
            try:
                if context: context.close()
            except Exception:
                pass
            try:
                if browser: browser.close()
            except Exception:
                pass

    if text is None:
        alert(["⚠️ Daily scrape blocked by login — please re-login (the NPS scraper will prompt you)."])
        return
    if not text:
        logger.error("No text extracted — skipping.")
        return

    metrics = parse_metrics(text)
    ok = send_daily_card(metrics)
    logger.info("Daily card send → %s", "OK" if ok else "FAIL")

    # CSV logging (append a wide row for history)
    headers = [
        "page_timestamp","period_range","store_line",
        "sales_total","sales_lfl","sales_vs_target",
        "supermarket_nps","colleague_happiness","home_delivery_nps","cafe_nps","click_collect_nps","customer_toilet_nps",
        "sco_utilisation","efficiency","scan_rate","scan_vs_target","interventions","interventions_vs_target",
        "mainbank_closed","mainbank_vs_target",
        "availability_pct","despatched_on_time","delivered_on_time","cc_avg_wait",
        "waste_total","markdowns_total","wm_total","wm_delta","wm_delta_pct",
        "moa","waste_validation","unrecorded_waste_pct","shrink_vs_budget_pct",
        "payroll_outturn","absence_outturn","productive_outturn","holiday_outturn","current_base_cost",
        "swipe_rate","swipes_wow_pct","new_customers","swipes_yoy_pct",
        "complaints_key","data_provided","trusted_data","my_reports","weekly_activity",
    ]
    row = [metrics.get(h, "—") for h in headers]

    write_header = not DAILY_LOG_CSV.exists() or DAILY_LOG_CSV.stat().st_size == 0
    with open(DAILY_LOG_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(headers)
        w.writerow(row)
    logger.info("Appended daily metrics row to %s", DAILY_LOG_CSV.name)

if __name__ == "__main__":
    run_daily_scrape()
