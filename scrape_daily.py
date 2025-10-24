#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary → Google Chat
Block-scoped parsing + tile-aware extraction to avoid picking up “Target” values.
Keeps CSV schema and Chat card from your previous version.
"""

import os
import re
import csv
import time
import logging
import configparser
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
    handlers=[logging.FileHandler(LOG_FILE_PATH)],
)
logger = logging.getLogger("daily")
logger.addHandler(logging.StreamHandler())

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
config = configparser.ConfigParser()
config.read(BASE_DIR / "config.ini")

MAIN_WEBHOOK   = config["DEFAULT"].get("MAIN_WEBHOOK",   os.getenv("MAIN_WEBHOOK", ""))
ALERT_WEBHOOK  = config["DEFAULT"].get("ALERT_WEBHOOK",  os.getenv("ALERT_WEBHOOK", ""))
CI_RUN_URL     = os.getenv("CI_RUN_URL", "")

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
                delay = min(float(r.headers.get("Retry-After", backoff) or backoff), max_backoff)
                logger.error(f"429 from webhook — sleeping {delay:.1f}s")
                time.sleep(delay); backoff = min(backoff * 1.7, max_backoff)
                continue
            logger.error(f"Webhook error {r.status_code}: {r.text[:300]}")
            return False
        except Exception as e:
            logger.error(f"Webhook exception: {e}")
            time.sleep(backoff); backoff = min(backoff * 1.7, max_backoff)

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
        (SCREENS_DIR / f"{ts}_{tag}.png").write_bytes(page.screenshot(full_page=True))
        (SCREENS_DIR / f"{ts}_{tag}.html").write_text(page.content(), encoding="utf-8")
        logger.info(f"Saved debug snapshot → {ts}_{tag}.png/.html")
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

    logger.info("Waiting 15s for dynamic content…")
    page.wait_for_timeout(15_000)

    text = ""
    try:
        text = page.inner_text("body")
    except Exception:
        pass

    # Dive into frames for the longest text if body is short
    if not text or len(text) < 400:
        best = text; best_len = len(best) if best else 0
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
# Parsing utilities (block-scoped + tile-aware)
# ──────────────────────────────────────────────────────────────────────────────
def _block(text: str, start: str, ends: List[str]) -> str:
    """Slice text from first occurrence of `start` to the earliest next end marker."""
    s = text.find(start)
    if s == -1:
        return ""
    e_candidates = [text.find(end, s + len(start)) for end in ends]
    e_candidates = [e for e in e_candidates if e != -1]
    e = min(e_candidates) if e_candidates else len(text)
    return text[s:e]

def _search_first(patterns: List[str], text: str, flags=0, group: int = 1, default="—") -> str:
    for pat in patterns:
        m = re.search(pat, text, flags)
        if m:
            try:
                return m.group(group).strip()
            except Exception:
                return m.group(0).strip()
    return default

def _tile_value(page_text: str, label: str) -> str:
    """
    From the first occurrence of `label`, look ahead a small window and return the
    first token that looks like a number or '—' BEFORE we hit 'Target:' or another tile.
    """
    idx = page_text.find(label)
    if idx == -1:
        return "—"
    window = page_text[idx: idx + 400]  # small local window
    # Prefer a number or '—' that is followed by a line containing 'NPS' (the dial caption),
    # and make sure 'Target' is not between label and the value we pick.
    lines = window.splitlines()
    seen_label = False
    for i, line in enumerate(lines[:20]):
        if not seen_label:
            if label in line:
                seen_label = True
            continue
        # stop if we run into 'Target'
        if "Target" in line:
            break
        token = line.strip()
        if token in ("—", "-", "–"):
            return "—"
        if re.fullmatch(r"-?\d{1,3}", token):
            # ensure next few lines include 'NPS' (the dial caption), which confirms it's the tile value
            tail = "\n".join(lines[i:i+5])
            if "NPS" in tail:
                return token
    return "—"

def parse_metrics(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}

    # Global context
    out["period_range"] = _search_first([r"The data on this report is from:\s*([^\n]+)"], text)
    out["page_timestamp"] = _search_first([r"\b(\d{1,2}\s+[A-Za-z]{3}\s+\d{4},\s*\d{2}:\d{2}:\d{2})\b"], text)
    out["store_line"] = _search_first(
        [r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}).*?(\|\s*.+?\s*\|\s*\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})"],
        text, flags=re.S, group=0
    )

    # ── Sales block
    sales_b = _block(text, "Sales", ["Waste & Markdowns", "Colleague Happiness", "Payroll"])
    if sales_b:
        m = re.search(
            r"Total\s*\n\s*([£]?[0-9.,]+[KMB]?)\s*\n\s*([+-]?\d+%?)\s*\n\s*([+-]?[£]?[0-9.,]+[KMB]?)",
            sales_b, flags=re.I
        )
        if m:
            out["sales_total"], out["sales_lfl"], out["sales_vs_target"] = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
        else:
            out["sales_total"] = out["sales_lfl"] = out["sales_vs_target"] = "—"
    else:
        out["sales_total"] = out["sales_lfl"] = out["sales_vs_target"] = "—"

    # ── NPS / tiles (tile-aware so we don’t pick “Target”)
    out["supermarket_nps"]   = _tile_value(text, "Supermarket NPS")
    out["colleague_happiness"]= _tile_value(text, "Colleague Happiness")
    out["home_delivery_nps"] = _tile_value(text, "Home Delivery NPS")
    out["cafe_nps"]          = _tile_value(text, "Cafe NPS")
    out["click_collect_nps"] = _tile_value(text, "Click & Collect NPS")
    out["customer_toilet_nps"]= _tile_value(text, "Customer Toilet NPS")

    # ── Front End Service block
    fes_b = _block(text, "Front End Service", ["Production Planning", "More Card Engagement", "Stock Record NPS", "Privacy"])
    out["sco_utilisation"] = _search_first([r"Sco Utilisation\s*\n\s*([0-9]+%)"], fes_b, flags=re.I)
    out["efficiency"]      = _search_first([r"Efficiency\s*\n\s*([0-9]+%)"], fes_b, flags=re.I)
    # metric + vs Target
    m = re.search(r"Scan Rate\s*\n\s*([0-9]+)\s*\n\s*vs Target\s*\n\s*([+-]?[0-9.]+)", fes_b, flags=re.I)
    out["scan_rate"], out["scan_vs_target"] = (m.group(1).strip(), m.group(2).strip()) if m else ("—", "—")
    m = re.search(r"Interventions\s*\n\s*([0-9]+)\s*\n\s*vs Target\s*\n\s*([+-]?[0-9.]+)", fes_b, flags=re.I)
    out["interventions"], out["interventions_vs_target"] = (m.group(1).strip(), m.group(2).strip()) if m else ("—", "—")
    m = re.search(r"Mainbank Closed\s*\n\s*([0-9]+)\s*\n\s*vs Target\s*\n\s*([+-]?[0-9.]+)", fes_b, flags=re.I)
    out["mainbank_closed"], out["mainbank_vs_target"] = (m.group(1).strip(), m.group(2).strip()) if m else ("—", "—")

    # ── Online block
    online_b = _block(text, "Online", ["Front End Service", "Production Planning", "Privacy"])
    out["availability_pct"]  = _search_first([r"Availability\s*\n\s*([0-9]+%)"], online_b, flags=re.I)
    out["despatched_on_time"]= _search_first([r"Despatched on Time\s*\n\s*([0-9]+%|No data)"], online_b, flags=re.I)
    out["delivered_on_time"] = _search_first([r"Delivered on Time\s*\n\s*([0-9]+%|No data)"], online_b, flags=re.I)
    out["cc_avg_wait"]       = _search_first([r"Click\s*&\s*Collect average wait\s*\n\s*([0-9]{2}:[0-9]{2})"], online_b, flags=re.I)

    # ── Waste & Markdowns block
    wm_b = _block(text, "Waste & Markdowns", ["My Reports", "Clean & Rotate", "Payroll", "Online"])
    m = re.search(
        r"Total\s*\n\s*([£]?[0-9.,]+[KMB]?)\s*\n\s*([£]?[0-9.,]+[KMB]?)\s*\n\s*([£]?[0-9.,]+[KMB]?)\s*\n\s*([+-]?[£]?[0-9.,]+[KMB]?)\s*\n\s*([+-]?\d+\.?\d*%)",
        wm_b, flags=re.I
    )
    if m:
        out["waste_total"], out["markdowns_total"], out["wm_total"], out["wm_delta"], out["wm_delta_pct"] = \
            m.group(1).strip(), m.group(2).strip(), m.group(3).strip(), m.group(4).strip(), m.group(5).strip()
    else:
        out.update({k: "—" for k in ["waste_total", "markdowns_total", "wm_total", "wm_delta", "wm_delta_pct"]})

    # ── Payroll block
    pay_b = _block(text, "Payroll", ["More Card Engagement", "Stock Record NPS", "Online", "Privacy"])
    out["payroll_outturn"]   = _search_first([r"Payroll Outturn\s*\n\s*([+-]?[£]?[0-9.,]+[KMB]?)"], pay_b, flags=re.I)
    out["absence_outturn"]   = _search_first([r"Absence Outturn\s*\n\s*([+-]?[£]?[0-9.,]+[KMB]?)"], pay_b, flags=re.I)
    out["productive_outturn"]= _search_first([r"Productive Outturn\s*\n\s*([+-]?[£]?[0-9.,]+[KMB]?)"], pay_b, flags=re.I)
    out["holiday_outturn"]   = _search_first([r"Holiday Outturn\s*\n\s*([+-]?[£]?[0-9.,]+[KMB]?)"], pay_b, flags=re.I)
    out["current_base_cost"] = _search_first([r"Current Base Cost\s*\n\s*([£]?[0-9.,]+[KMB]?)"], pay_b, flags=re.I)

    # ── Shrink block
    shr_b = _block(text, "Shrink", ["Online", "Production Planning", "Privacy"])
    out["moa"]                 = _search_first([r"Morrisons Order Adjustments\s*\n\s*([£]?-?[0-9.,]+[KMB]?)"], shr_b, flags=re.I)
    out["waste_validation"]    = _search_first([r"Waste Validation\s*\n\s*([0-9]+%)"], shr_b, flags=re.I)
    out["unrecorded_waste_pct"]= _search_first([r"Unrecorded Waste %\s*\n\s*([+-]?\d+\.?\d*%)"], shr_b, flags=re.I)
    out["shrink_vs_budget_pct"]= _search_first([r"Shrink vs Budget %\s*\n\s*([+-]?\d+\.?\d*%)"], shr_b, flags=re.I)

    # ── Card Engagement block
    ce_b = _block(text, "More Card Engagement", ["Stock Record NPS", "Production Planning", "Privacy"])
    out["swipe_rate"]   = _search_first([r"Swipe Rate\s*\n\s*([0-9]+%)"], ce_b, flags=re.I)
    out["swipes_wow_pct"]= _search_first([r"Swipes WOW %\s*\n\s*([+-]?\d+%)"], ce_b, flags=re.I)
    out["new_customers"]= _search_first([r"New Customers\s*\n\s*([0-9]+)"], ce_b, flags=re.I)
    out["swipes_yoy_pct"]= _search_first([r"Swipes YOY %\s*\n\s*([+-]?\d+%)"], ce_b, flags=re.I)

    # ── Production Planning (Data Provided / Trusted Data)
    pp_b = _block(text, "Production Planning", ["Shrink", "Online", "Privacy"])
    out["data_provided"] = _search_first([r"Data Provided\s*\n\s*([0-9]+%)"], pp_b, flags=re.I)
    out["trusted_data"]  = _search_first([r"Trusted Data\s*\n\s*([0-9]+%)"], pp_b, flags=re.I)

    # ── Misc
    out["complaints_key"] = _search_first([r"Key Customer Complaints\s*\n\s*([0-9]+)"], text, flags=re.I)
    out["my_reports"]     = _search_first([r"My Reports\s*\n\s*([0-9]+)"], text, flags=re.I)
    out["weekly_activity"]= _search_first([r"Weekly Activity %\s*\n\s*([0-9]+%|No data)"], text, flags=re.I)

    return out

# ──────────────────────────────────────────────────────────────────────────────
# Card builder + sender (Cards V2 valid)
# ──────────────────────────────────────────────────────────────────────────────
def build_chat_card(metrics: Dict[str, str]) -> dict:
    card_header = {
        "title": "📊 Retail Daily Summary",
        "subtitle": (metrics.get("store_line") or "").replace("\n", "  "),
    }

    def title_widget(text: str) -> dict:
        return {"textParagraph": {"text": f"<b>{text}</b>"}}

    def kv(label: str, val: str) -> dict:
        return {"decoratedText": {"topLabel": label, "text": (val or "—")}}

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
                     kv("Availability",          metrics.get("availability_pct", "—")),
                     kv("Despatched on Time",    metrics.get("despatched_on_time", "—")),
                     kv("Delivered on Time",     metrics.get("delivered_on_time", "—")),
                     kv("Click & Collect Avg Wait", metrics.get("cc_avg_wait", "—"))]},
        {"widgets": [title_widget("Waste & Markdowns (Total)"),
                     kv("Waste",      metrics.get("waste_total", "—")),
                     kv("Markdowns",  metrics.get("markdowns_total", "—")),
                     kv("Total",      metrics.get("wm_total", "—")),
                     kv("+/−",        metrics.get("wm_delta", "—")),
                     kv("+/− %",      metrics.get("wm_delta_pct", "—"))]},
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
                     kv("Weekly Activity %",metrics.get("weekly_activity", "—"))]}
    ]

    return {"cardsV2": [{"cardId": f"daily_{int(time.time())}", "card": {"header": card_header, "sections": sections}}]}

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
