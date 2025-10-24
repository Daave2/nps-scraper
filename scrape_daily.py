#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary → Google Chat (Full-Page OCR)

Flow:
1) Open dashboard with saved auth_state.json
2) Click any Community Viz “PROCEED” overlays (with retries)
3) Take ONE full-page screenshot and OCR it into text
4) Parse all metrics from OCR text (robust regex windows around labels)
5) Send Google Chat card + append CSV row
6) Save screenshot + OCR text into screens/ for debugging

CSV schema is unchanged.
"""

import os
import re
import csv
import time
import logging
import configparser
from pathlib import Path
from typing import Dict, List, Optional

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# OCR deps
from PIL import Image
from io import BytesIO
import pytesseract

# ── Constants / Paths ────────────────────────────────────────────────────────
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

COMMUNITY_VIZ_PLACEHOLDER = (
    "You are about to interact with a community visualisation in this embedded report"
)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE_PATH)],
)
logger = logging.getLogger("daily_ocr")
logger.addHandler(logging.StreamHandler())

# ── Config (file first, env fallback) ────────────────────────────────────────
config = configparser.ConfigParser()
config.read(BASE_DIR / "config.ini")

MAIN_WEBHOOK  = (
    config["DEFAULT"].get("DAILY_WEBHOOK")
    or config["DEFAULT"].get("MAIN_WEBHOOK")
    or os.getenv("DAILY_WEBHOOK")
    or os.getenv("MAIN_WEBHOOK", "")
)
ALERT_WEBHOOK = config["DEFAULT"].get("ALERT_WEBHOOK", os.getenv("ALERT_WEBHOOK", ""))
CI_RUN_URL    = os.getenv("CI_RUN_URL", "")

# ── Chat helpers ─────────────────────────────────────────────────────────────
def _post_with_backoff(url: str, payload: dict) -> bool:
    backoff, max_backoff = 2.0, 30.0
    while True:
        try:
            r = requests.post(url, json=payload, timeout=25)
            if r.status_code == 200:
                return True
            if r.status_code == 429:
                delay = min(float(r.headers.get("Retry-After") or backoff), max_backoff)
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

# ── Debug helpers ────────────────────────────────────────────────────────────
def save_screenshot(page, tag: str):
    try:
        ts = int(time.time())
        SCREENS_DIR.mkdir(parents=True, exist_ok=True)
        pth = SCREENS_DIR / f"{ts}_{tag}.png"
        pth.write_bytes(page.screenshot(full_page=True))
        logger.info(f"Saved screenshot → {pth.name}")
    except Exception as e:
        logger.warning(f"Could not save screenshot ({tag}): {e}")

def save_text_dump(text: str, tag: str):
    try:
        ts = int(time.time())
        SCREENS_DIR.mkdir(parents=True, exist_ok=True)
        (SCREENS_DIR / f"{ts}_{tag}.txt").write_text(text, encoding="utf-8")
        logger.info(f"Saved text dump → {ts}_{tag}.txt")
    except Exception:
        pass

# ── Overlays ─────────────────────────────────────────────────────────────────
def click_proceed_overlays(page, retries: int = 3, waits: List[int] = [1500, 3000, 5000]) -> int:
    total_clicked = 0
    for attempt in range(retries):
        clicked = 0
        for fr in page.frames:
            try:
                btns = fr.get_by_text(re.compile(r"\bPROCEED\b", re.I))
                n = btns.count()
                for i in range(n):
                    try:
                        btns.nth(i).click(timeout=2000)
                        clicked += 1
                        fr.wait_for_timeout(250)
                    except Exception:
                        continue
            except Exception:
                continue
        total_clicked += clicked
        if clicked == 0:
            break
        wait_ms = waits[min(attempt, len(waits)-1)]
        page.wait_for_timeout(wait_ms)
    if total_clicked:
        logger.info(f"Clicked {total_clicked} 'PROCEED' overlay(s) in total.")
    return total_clicked

# ── Capture → OCR text ───────────────────────────────────────────────────────
def ocr_full_page(page) -> str:
    """
    Takes a single full-page screenshot and OCRs it.
    Returns OCR text (string). Saves PNG + TXT to screens/.
    """
    save_screenshot(page, "fullpage")
    png = page.screenshot(full_page=True)
    img = Image.open(BytesIO(png))

    # Light preprocessing – grayscale + mild upscale for crisper digits
    try:
        img = img.convert("L")
        w, h = img.size
        if max(w, h) < 1600:
            img = img.resize((int(w*1.5), int(h*1.5)))
    except Exception:
        pass

    text = pytesseract.image_to_string(
        img,
        # PSM 6: Assume a block of text; no aggressive whitelisting (we need labels)
        config="--psm 6",
    )
    save_text_dump(text, "ocr_text")
    return text

# ── Navigation ───────────────────────────────────────────────────────────────
def get_ocr_text_from_dashboard() -> Optional[str]:
    if not AUTH_STATE_PATH.exists():
        alert(["⚠️ Daily dashboard scrape needs login. Run `python scrape.py now` once to save auth_state.json."])
        logger.error("auth_state.json not found.")
        return None

    with sync_playwright() as p:
        browser = context = page = None
        try:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                storage_state=str(AUTH_STATE_PATH),
                viewport={"width": 1600, "height": 1200},
                device_scale_factor=2,                   # sharper
            )
            page = context.new_page()

            logger.info("Opening Retail Performance Dashboard…")
            try:
                page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
            except PlaywrightTimeoutError:
                logger.error("Timeout loading dashboard.")
                save_screenshot(page, "dashboard_timeout")
                return ""

            if "accounts.google.com" in page.url:
                logger.warning("Redirected to login — auth state missing/invalid.")
                return None

            logger.info("Waiting 12s for dynamic content…")
            page.wait_for_timeout(12_000)

            # Try to clear Community Viz overlays
            click_proceed_overlays(page)
            page.wait_for_timeout(1200)

            # If the placeholder text still exists in DOM HTML, wait once more
            html = page.content()
            if COMMUNITY_VIZ_PLACEHOLDER in (html or ""):
                logger.warning("Community Viz placeholder still present — waiting longer.")
                click_proceed_overlays(page, retries=1, waits=[5000])
                page.wait_for_timeout(1500)

            # One-shot full OCR
            text = ocr_full_page(page)
            return text

        finally:
            try:
                if context: context.close()
            except Exception:
                pass
            try:
                if browser: browser.close()
            except Exception:
                pass

# ── Parsing helpers for OCR text ─────────────────────────────────────────────
def _first(pats: List[str], text: str, flags=0, group=1, default="—") -> str:
    for pat in pats:
        m = re.search(pat, text, flags)
        if m:
            try:
                return m.group(group).strip()
            except Exception:
                return m.group(0).strip()
    return default

def _near(label: str, text: str, window: int = 120, num_pat: str = r"-?\d{1,3}") -> str:
    """
    Find the first integer near (after) a label within 'window' chars.
    Default captures e.g. NPS values (-99..99), but num_pat can be changed.
    """
    m = re.search(re.escape(label), text, flags=re.I)
    if not m:
        return "—"
    start = m.end()
    snippet = text[start : start + window]
    m2 = re.search(num_pat, snippet)
    return m2.group(0) if m2 else "—"

def _near_pct(label: str, text: str, window: int = 120) -> str:
    m = re.search(re.escape(label), text, flags=re.I)
    if not m:
        return "—"
    snippet = text[m.end() : m.end() + window]
    m2 = re.search(r"-?\d{1,3}(?:\.\d+)?%", snippet)
    return m2.group(0) if m2 else "—"

def _near_money(label: str, text: str, window: int = 140) -> str:
    m = re.search(re.escape(label), text, flags=re.I)
    if not m:
        return "—"
    snippet = text[m.end() : m.end() + window]
    m2 = re.search(r"[£-]?\s?[0-9.,]+[KMB]?", snippet, flags=re.I)
    return (m2.group(0).replace(" ", "") if m2 else "—")

def _near_time(label: str, text: str, window: int = 100) -> str:
    m = re.search(re.escape(label), text, flags=re.I)
    if not m:
        return "—"
    snippet = text[m.end() : m.end() + window]
    m2 = re.search(r"\b\d{2}:\d{2}\b", snippet)
    return m2.group(0) if m2 else "—"

# ── Parse all metrics from OCR text ──────────────────────────────────────────
def parse_metrics(ocr: str) -> Dict[str, str]:
    out: Dict[str, str] = {}

    # Context / header
    out["period_range"] = _first([r"The data on this report is from:\s*([^\n]+)"], ocr)
    out["page_timestamp"] = _first([r"\b(\d{1,2}\s+[A-Za-z]{3}\s+\d{4},\s*\d{2}:\d{2}:\d{2})\b"], ocr)
    out["store_line"] = _first(
        [r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}).*?\|\s*.*?\|\s*\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}"],
        ocr, flags=re.S, group=0
    )

    # Sales (Total row: value, LFL, vs Target)
    # OCR often collapses whitespace; accept either “Total 46.8M 1.73% -1.3M” or in lines
    m = re.search(
        r"Sales.*?Total\s+([£-]?\s?[0-9.,]+[KMB]?)\s+([+-]?\d+(?:\.\d+)?%)\s+([£+-]?\s?[0-9.,]+[KMB]?)",
        ocr, flags=re.I | re.S
    )
    if m:
        out["sales_total"]    = m.group(1).replace(" ", "")
        out["sales_lfl"]      = m.group(2)
        out["sales_vs_target"]= m.group(3).replace(" ", "")
    else:
        out["sales_total"] = out["sales_lfl"] = out["sales_vs_target"] = "—"

    # NPS gauges (numbers near labels)
    out["supermarket_nps"]     = _near("Supermarket NPS", ocr)
    out["colleague_happiness"] = _near("Colleague Happiness", ocr)
    out["home_delivery_nps"]   = _near("Home Delivery NPS", ocr)
    out["cafe_nps"]            = _near("Cafe NPS", ocr)
    out["click_collect_nps"]   = _near("Click & Collect NPS", ocr)
    out["customer_toilet_nps"] = _near("Customer Toilet NPS", ocr)

    # Front End Service
    out["sco_utilisation"] = _near_pct("Sco Utilisation", ocr)
    out["efficiency"]      = _near_pct("Efficiency", ocr)
    # Scan / Interventions / Mainbank include “vs Target”
    out["scan_rate"]       = _near("Scan Rate", ocr, num_pat=r"\d{1,4}")
    out["scan_vs_target"]  = _near_pct("vs Target", ocr, window=40)  # the first vs Target after Scan Rate window
    # For robustness, find the next explicit “Interventions” block separately:
    out["interventions"]   = _near("Interventions", ocr, num_pat=r"\d{1,4}")
    # pick the vs Target that appears closely after “Interventions”
    inter_pos = ocr.lower().find("interventions")
    if inter_pos != -1:
        inter_win = ocr[inter_pos: inter_pos+200]
        mt = re.search(r"vs Target\s+(-?\d+(?:\.\d+)?)", inter_win, flags=re.I)
        out["interventions_vs_target"] = mt.group(1) if mt else "—"
    else:
        out["interventions_vs_target"] = "—"

    out["mainbank_closed"] = _near("Mainbank Closed", ocr, num_pat=r"\d{1,4}")
    # vs Target after Mainbank Closed
    mb_pos = ocr.lower().find("mainbank closed")
    if mb_pos != -1:
        mb_win = ocr[mb_pos: mb_pos+200]
        mt = re.search(r"vs Target\s+(-?\d+(?:\.\d+)?)", mb_win, flags=re.I)
        out["mainbank_vs_target"] = mt.group(1) if mt else "—"
    else:
        out["mainbank_vs_target"] = "—"

    # Online
    out["availability_pct"]   = _near_pct("Availability", ocr)
    out["despatched_on_time"] = _near_pct("Despatched on Time", ocr)
    out["delivered_on_time"]  = _near_pct("Delivered on Time", ocr)
    out["cc_avg_wait"]        = _near_time("Click & Collect", ocr)

    # Waste & Markdowns (Total row: Waste, Markdowns, Total, +/- , +/- %)
    wm = re.search(
        r"Waste\s*&?\s*Markdowns.*?Total\s+([£-]?\s?[0-9.,]+[KMB]?)\s+([£-]?\s?[0-9.,]+[KMB]?)\s+([£-]?\s?[0-9.,]+[KMB]?)\s+([£+-]?\s?[0-9.,]+[KMB]?)\s+(-?\d+(?:\.\d+)?%)",
        ocr, flags=re.I | re.S
    )
    if wm:
        out["waste_total"]  = wm.group(1).replace(" ", "")
        out["markdowns_total"] = wm.group(2).replace(" ", "")
        out["wm_total"]     = wm.group(3).replace(" ", "")
        out["wm_delta"]     = wm.group(4).replace(" ", "")
        out["wm_delta_pct"] = wm.group(5)
    else:
        out.update({k: "—" for k in ["waste_total","markdowns_total","wm_total","wm_delta","wm_delta_pct"]})

    # Payroll
    out["payroll_outturn"]    = _near_money("Payroll Outturn", ocr)
    out["absence_outturn"]    = _near_money("Absence Outturn", ocr)
    out["productive_outturn"] = _near_money("Productive Outturn", ocr)
    out["holiday_outturn"]    = _near_money("Holiday Outturn", ocr)
    out["current_base_cost"]  = _near_money("Current Base Cost", ocr)

    # Shrink
    out["moa"]                  = _near_money("Morrisons Order Adjustments", ocr)
    out["waste_validation"]     = _near_pct("Waste Validation", ocr)
    out["unrecorded_waste_pct"] = _near_pct("Unrecorded Waste %", ocr)
    out["shrink_vs_budget_pct"] = _near_pct("Shrink vs Budget %", ocr)

    # Card Engagement
    out["swipe_rate"]    = _near_pct("Swipe Rate", ocr)
    out["swipes_wow_pct"]= _near_pct("Swipes WOW %", ocr)
    out["new_customers"] = _near("New Customers", ocr, num_pat=r"[0-9,]{1,8}")
    out["swipes_yoy_pct"]= _near_pct("Swipes YOY %", ocr)

    # Production Planning
    out["data_provided"] = _near_pct("Data Provided", ocr)
    out["trusted_data"]  = _near_pct("Trusted Data", ocr)

    # Misc
    out["complaints_key"] = _near("Key Customer Complaints", ocr, num_pat=r"\d{1,5}")
    out["my_reports"]     = _near("My Reports", ocr, num_pat=r"[0-9,]{1,6}")
    out["weekly_activity"]= _near_pct("Weekly Activity %", ocr)

    return out

# ── Card builder + sender ────────────────────────────────────────────────────
def build_chat_card(metrics: Dict[str, str]) -> dict:
    def title_widget(text: str) -> dict:
        return {"textParagraph": {"text": f"<b>{text}</b>"}}
    def kv(label: str, val: str) -> dict:
        return {"decoratedText": {"topLabel": label, "text": (val or "—")}}

    header = {
        "title": "📊 Retail Daily Summary (OCR)",
        "subtitle": (metrics.get("store_line") or "").replace("\n", "  "),
    }

    sections = [
        {"widgets": [kv("Report Time", metrics.get("page_timestamp", "—")),
                     kv("Period",      metrics.get("period_range", "—"))]},
        {"widgets": [title_widget("Sales & NPS"),
                     kv("Sales Total", metrics.get("sales_total", "—")),
                     kv("LFL",         metrics.get("sales_lfl", "—")),
                     kv("vs Target",   metrics.get("sales_vs_target", "—")),
                     kv("Supermarket NPS",     metrics.get("supermarket_nps", "—")),
                     kv("Colleague Happiness", metrics.get("colleague_happiness", "—")),
                     kv("Home Delivery NPS",   metrics.get("home_delivery_nps", "—")),
                     kv("Cafe NPS",            metrics.get("cafe_nps", "—")),
                     kv("Click & Collect NPS", metrics.get("click_collect_nps", "—")),
                     kv("Customer Toilet NPS", metrics.get("customer_toilet_nps", "—"))]},
        {"widgets": [title_widget("Front End Service"),
                     kv("SCO Utilisation", metrics.get("sco_utilisation", "—")),
                     kv("Efficiency",      metrics.get("efficiency", "—")),
                     kv("Scan Rate",       f"{metrics.get('scan_rate','—')} (vs {metrics.get('scan_vs_target','—')})"),
                     kv("Interventions",   f"{metrics.get('interventions','—')} (vs {metrics.get('interventions_vs_target','—')})"),
                     kv("Mainbank Closed", f"{metrics.get('mainbank_closed','—')} (vs {metrics.get('mainbank_vs_target','—')})")]},
        {"widgets": [title_widget("Online"),
                     kv("Availability",         metrics.get("availability_pct", "—")),
                     kv("Despatched on Time",   metrics.get("despatched_on_time", "—")),
                     kv("Delivered on Time",    metrics.get("delivered_on_time", "—")),
                     kv("Click & Collect Avg Wait", metrics.get("cc_avg_wait", "—"))]},
        {"widgets": [title_widget("Waste & Markdowns (Total)"),
                     kv("Waste",     metrics.get("waste_total", "—")),
                     kv("Markdowns", metrics.get("markdowns_total", "—")),
                     kv("Total",     metrics.get("wm_total", "—")),
                     kv("+/−",       metrics.get("wm_delta", "—")),
                     kv("+/− %",     metrics.get("wm_delta_pct", "—"))]},
        {"widgets": [title_widget("Payroll"),
                     kv("Payroll Outturn",   metrics.get("payroll_outturn", "—")),
                     kv("Absence Outturn",   metrics.get("absence_outturn", "—")),
                     kv("Productive Outturn",metrics.get("productive_outturn", "—")),
                     kv("Holiday Outturn",   metrics.get("holiday_outturn", "—")),
                     kv("Current Base Cost", metrics.get("current_base_cost", "—"))]},
        {"widgets": [title_widget("Shrink"),
                     kv("Morrisons Order Adjustments", metrics.get("moa", "—")),
                     kv("Waste Validation",            metrics.get("waste_validation", "—")),
                     kv("Unrecorded Waste %",          metrics.get("unrecorded_waste_pct", "—")),
                     kv("Shrink vs Budget %",          metrics.get("shrink_vs_budget_pct", "—"))]},
        {"widgets": [title_widget("Card Engagement & Misc"),
                     kv("Swipe Rate",      metrics.get("swipe_rate", "—")),
                     kv("Swipes WOW %",    metrics.get("swipes_wow_pct", "—")),
                     kv("New Customers",   metrics.get("new_customers", "—")),
                     kv("Swipes YOY %",    metrics.get("swipes_yoy_pct", "—")),
                     kv("Key Complaints",  metrics.get("complaints_key", "—")),
                     kv("Data Provided",   metrics.get("data_provided", "—")),
                     kv("Trusted Data",    metrics.get("trusted_data", "—")),
                     kv("My Reports",      metrics.get("my_reports", "—")),
                     kv("Weekly Activity %",metrics.get("weekly_activity", "—"))]},
    ]

    return {"cardsV2": [{"cardId": f"daily_{int(time.time())}", "card": {"header": header, "sections": sections}}]}

def send_daily_card(metrics: Dict[str, str]) -> bool:
    if not MAIN_WEBHOOK or "chat.googleapis.com" not in MAIN_WEBHOOK:
        logger.error("MAIN_WEBHOOK/DAILY_WEBHOOK missing or invalid — cannot send daily report.")
        return False
    payload = build_chat_card(metrics)
    return _post_with_backoff(MAIN_WEBHOOK, payload)

# ── Main ─────────────────────────────────────────────────────────────────────
def run_daily_scrape():
    text = get_ocr_text_from_dashboard()
    if text is None:
        # login missing/expired
        return
    if not text:
        logger.error("OCR returned empty text — skipping.")
        return

    metrics = parse_metrics(text)
    ok = send_daily_card(metrics)
    logger.info("Daily card send → %s", "OK" if ok else "FAIL")

    # CSV logging (same header order as before)
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
