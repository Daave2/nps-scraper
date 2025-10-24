#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary (OCR-first) → Google Chat

- Clicks "This Week" filter to normalise data
- Full-page screenshot + OCR text dump saved to ./screens
- Robust near-label parsing with alias fallbacks for common OCR glitches
- Sends a single Google Chat card + appends to daily_report_log.csv (same schema)
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

# OCR deps
try:
    from PIL import Image
    import pytesseract
    OCR_AVAILABLE = True
except Exception:
    OCR_AVAILABLE = False

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
    handlers=[logging.FileHandler(LOG_FILE_PATH, encoding="utf-8")]
)
logger = logging.getLogger("daily")
logger.addHandler(logging.StreamHandler())

# ──────────────────────────────────────────────────────────────────────────────
# Config (file first, env fallback)
# ──────────────────────────────────────────────────────────────────────────────
config = configparser.ConfigParser()
config.read(BASE_DIR / "config.ini")

MAIN_WEBHOOK   = config["DEFAULT"].get("DAILY_WEBHOOK") or config["DEFAULT"].get("MAIN_WEBHOOK", os.getenv("MAIN_WEBHOOK", ""))
ALERT_WEBHOOK  = config["DEFAULT"].get("ALERT_WEBHOOK",  os.getenv("ALERT_WEBHOOK", ""))
CI_RUN_URL     = os.getenv("CI_RUN_URL", "")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers (Chat + debug)
# ──────────────────────────────────────────────────────────────────────────────
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

def _save_bytes(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)

def _save_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

# ──────────────────────────────────────────────────────────────────────────────
# Navigation + capture (forces "This Week")
# ──────────────────────────────────────────────────────────────────────────────
def open_dashboard_and_normalise(page) -> bool:
    logger.info("Opening Retail Performance Dashboard…")
    try:
        page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
    except TimeoutError:
        logger.error("Timeout loading dashboard.")
        return False

    if "accounts.google.com" in page.url:
        logger.warning("Redirected to login — auth state missing/invalid.")
        return False

    # Give the page time to paint initial layout
    logger.info("Waiting 10s for initial content…")
    page.wait_for_timeout(10_000)

    # Try to click "This Week"
    try:
        tw = page.get_by_text(re.compile(r"^\s*This Week\s*$"))
        if tw.count() > 0:
            tw.first.click(timeout=3_000)
            page.wait_for_timeout(1200)
    except Exception:
        pass

    # Some community viz prompt a “PROCEED” overlay; try to click them
    clicked = 0
    for fr in page.frames:
        try:
            btns = fr.get_by_text(re.compile(r"^\s*PROCEED\s*$"))
            for i in range(min(5, btns.count())):
                try:
                    btns.nth(i).click(timeout=1200)
                    clicked += 1
                    fr.wait_for_timeout(400)
                except Exception:
                    continue
        except Exception:
            continue
    if clicked:
        logger.info(f"Clicked {clicked} 'PROCEED' overlay(s). Waiting for render…")
        page.wait_for_timeout(1500)

    return True

def capture_fullpage_ocr(page) -> Optional[str]:
    if not OCR_AVAILABLE:
        logger.error("pytesseract/Pillow not available — OCR cannot run.")
        return None

    # Full-page screenshot
    ts = int(time.time())
    png_path = SCREENS_DIR / f"{ts}_fullpage.png"
    ocr_txt_path = SCREENS_DIR / f"{ts}_ocr.txt"

    try:
        png_bytes = page.screenshot(full_page=True, type="png")
        _save_bytes(png_path, png_bytes)
        logger.info(f"Saved screenshot → {png_path.name}")
    except Exception as e:
        logger.error(f"Failed to take full-page screenshot: {e}")
        return None

    # OCR
    try:
        img = Image.open(png_path)
        # Slight upscale helps tesseract on small UI text
        w, h = img.size
        if max(w, h) < 1800:
            img = img.resize((int(w*1.5), int(h*1.5)))
        text = pytesseract.image_to_string(
            img,
            config="--psm 6",
            lang="eng"
        )
        _save_text(ocr_txt_path, text)
        logger.info(f"Saved OCR text → {ocr_txt_path.name}")
        return text
    except Exception as e:
        logger.error(f"OCR failed: {e}")
        return None

# ──────────────────────────────────────────────────────────────────────────────
# OCR parsing helpers
# ──────────────────────────────────────────────────────────────────────────────
ALIAS_MAP = {
    # mild OCR glitches we've seen
    "uilhsaiaon": "utilisation",
    "lniiervcenitons": "interventions",
    "se uilhsaiaon": "sco utilisation",
    "ulilisation": "utilisation",
    "sc0": "sco",
    "click & collect average wait": "click & collect average wait",
}

def _norm(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9%:+\-\s\.]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    for k, v in ALIAS_MAP.items():
        s = s.replace(k, v)
    return s

def _find_near(text: str, anchors: List[str], window: int = 260) -> str:
    """
    Find first numeric-looking token within a small window after any anchor.
    """
    norm_text = _norm(text)
    for a in anchors:
        a_norm = _norm(a)
        idx = norm_text.find(a_norm)
        if idx == -1:
            continue
        seg = norm_text[idx: idx + window]
        # prioritise e.g., 1-3 digits, %, K suffices, time mm:ss
        m = re.search(r"(-?\d{1,3}(?:\.\d+)?%?)\b", seg)
        if m:
            return m.group(1)
        m = re.search(r"([0-9]{2}:[0-9]{2})", seg)
        if m:
            return m.group(1)
        m = re.search(r"([£]?-?\d[\d,\.]*[KMB]?)", seg)
        if m:
            return m.group(1)
    return "—"

def _find_near_pair(text: str, anchors: List[str], labels: Tuple[str, str], window: int = 300) -> Tuple[str, str]:
    """
    For blocks that show: VALUE then 'vs Target' VALUE
    """
    norm_text = _norm(text)
    for a in anchors:
        a_norm = _norm(a)
        idx = norm_text.find(a_norm)
        if idx == -1:
            continue
        seg = norm_text[idx: idx + window]
        v1 = re.search(r"(-?\d{1,3}(?:\.\d+)?%?)\b", seg)
        vs_idx = seg.find(_norm(labels[1]))  # "vs target"
        v2 = None
        if vs_idx != -1:
            tail = seg[vs_idx: vs_idx + 120]
            v2 = re.search(r"(-?\d{1,3}(?:\.\d+)?%?)\b", tail)
        return (v1.group(1) if v1 else "—", v2.group(1) if v2 else "—")
    return ("—", "—")

def _sales_totals(text: str) -> Tuple[str, str, str]:
    """
    Extract Total row: value, LFL, vs Target
    Works because OCR preserves 'Total' line block fairly well.
    """
    block = re.search(
        r"total\s+([£]?-?[\d,\.]+[kmb]?)\s+([+-]?\d+%?)\s+([£]?-?[\d,\.]+[kmb]?)",
        _norm(text), flags=re.I
    )
    if block:
        return block.group(1), block.group(2), block.group(3)
    # fallback: search near 'Sales'
    sales = _norm(text)
    m = re.search(
        r"sales.*?total\s+([£]?-?[\d,\.]+[kmb]?).*?([+-]?\d+%?).*?([£]?-?[\d,\.]+[kmb]?)",
        sales, flags=re.S
    )
    if m:
        return m.group(1), m.group(2), m.group(3)
    return ("—", "—", "—")

# ──────────────────────────────────────────────────────────────────────────────
# Parse metrics from OCR text using mapped anchors
# ──────────────────────────────────────────────────────────────────────────────
def parse_metrics_ocr(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}

    # Context
    out["page_timestamp"] = _find_near(text, ["24 Oct", "Report Time", "07:"])  # captured separately below as well
    out["period_range"]  = _find_near(text, ["The data on this report is from:", "the data on this report is from:"])
    # Email / store / now line
    store_line_match = re.search(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}).{0,50}\|\s*([^\|]+?)\s*\|\s*\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}", text, flags=re.S)
    out["store_line"] = store_line_match.group(0).strip() if store_line_match else "—"

    # SALES total row
    out["sales_total"], out["sales_lfl"], out["sales_vs_target"] = _sales_totals(text)

    # NPS gauges (canvas) — near-label
    out["supermarket_nps"]     = _find_near(text, ["Supermarket NPS"])
    out["colleague_happiness"] = _find_near(text, ["Colleague Happiness"])
    out["home_delivery_nps"]   = _find_near(text, ["Home Delivery NPS"])
    out["cafe_nps"]            = _find_near(text, ["Cafe NPS"])
    out["click_collect_nps"]   = _find_near(text, ["Click & Collect NPS", "Click & Collect NPS"])
    out["customer_toilet_nps"] = _find_near(text, ["Customer Toilet NPS"])

    # FRONT END SERVICE
    out["sco_utilisation"] = _find_near(text, ["Sco Utilisation", "SCO Utilisation", "Se Utilisation", "Utilisation"])
    out["efficiency"]      = _find_near(text, ["Efficiency"])
    out["scan_rate"], out["scan_vs_target"] = _find_near_pair(text, ["Scan Rate"], ("Scan Rate", "vs Target"))
    out["interventions"], out["interventions_vs_target"] = _find_near_pair(text, ["Interventions"], ("Interventions", "vs Target"))
    out["mainbank_closed"], out["mainbank_vs_target"]     = _find_near_pair(text, ["Mainbank Closed"], ("Mainbank Closed", "vs Target"))

    # ONLINE
    out["availability_pct"]   = _find_near(text, ["Availability"])
    out["despatched_on_time"] = _find_near(text, ["Despatched on Time"])
    out["delivered_on_time"]  = _find_near(text, ["Delivered on Time"])
    out["cc_avg_wait"]        = _find_near(text, ["Click & Collect average wait", "Click & Collect Avg Wait"])

    # WASTE & MARKDOWNS (Total)
    # Search for table "Total" row again but scoped to W&M area: use nearby anchors
    out["waste_total"]     = _find_near(text, ["Waste & Markdowns", "Waste Markdowns", "Waste"], window=380)
    out["markdowns_total"] = _find_near(text, ["Markdowns", "Mark downs"], window=380)
    out["wm_total"]        = _find_near(text, ["Total", "Total "], window=240)
    out["wm_delta"]        = _find_near(text, ["(+/-)", "+/-"], window=280)
    out["wm_delta_pct"]    = _find_near(text, ["(+/-)%", "+/- %", "(+/-)%"], window=320)

    # PAYROLL
    out["payroll_outturn"]    = _find_near(text, ["Payroll Outturn"])
    out["absence_outturn"]    = _find_near(text, ["Absence Outturn"])
    out["productive_outturn"] = _find_near(text, ["Productive Outturn"])
    out["holiday_outturn"]    = _find_near(text, ["Holiday Outturn"])
    out["current_base_cost"]  = _find_near(text, ["Current Base Cost"])

    # SHRINK
    out["moa"]                  = _find_near(text, ["Morrisons Order Adjustments", "Morrisons Order", "Order Adjustments"])
    out["waste_validation"]     = _find_near(text, ["Waste Validation"])
    out["unrecorded_waste_pct"] = _find_near(text, ["Unrecorded Waste %", "Unrecorded Waste"])
    out["shrink_vs_budget_pct"] = _find_near(text, ["Shrink vs Budget %", "Shrink vs Budget"])

    # CARD ENGAGEMENT
    out["swipe_rate"]    = _find_near(text, ["Swipe Rate"])
    out["swipes_wow_pct"]= _find_near(text, ["Swipes WOW %", "Swipes WOW"])
    out["new_customers"] = _find_near(text, ["New Customers"])
    out["swipes_yoy_pct"]= _find_near(text, ["Swipes YOY %", "Swipes YOY"])

    # PRODUCTION PLANNING
    out["data_provided"] = _find_near(text, ["Data Provided"])
    out["trusted_data"]  = _find_near(text, ["Trusted Data"])

    # MISC
    out["complaints_key"] = _find_near(text, ["Key Customer Complaints"])
    out["my_reports"]     = _find_near(text, ["My Reports"])
    out["weekly_activity"]= _find_near(text, ["Weekly Activity %", "Weekly Activity"])

    # Normalise obvious empties to "—"
    for k, v in list(out.items()):
        if not v or v.strip() == "":
            out[k] = "—"
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Card builder + sender
# ──────────────────────────────────────────────────────────────────────────────
def _kv(label: str, val: str) -> dict:
    return {"decoratedText": {"topLabel": label, "text": val if val else "—"}}

def build_chat_card(metrics: Dict[str, str]) -> dict:
    header = {
        "title": "📊 Retail Daily Summary (OCR)",
        "subtitle": (metrics.get("store_line") or "").replace("\n", "  "),
    }
    sections = [
        {"widgets": [_kv("Report Time", metrics.get("page_timestamp", "—")),
                     _kv("Period",      metrics.get("period_range", "—"))]},
        {"widgets": [{"textParagraph": {"text": "<b>Sales & NPS</b>"}},
                     _kv("Sales Total", metrics.get("sales_total","—")),
                     _kv("LFL", metrics.get("sales_lfl","—")),
                     _kv("vs Target", metrics.get("sales_vs_target","—")),
                     _kv("Supermarket NPS", metrics.get("supermarket_nps","—")),
                     _kv("Colleague Happiness", metrics.get("colleague_happiness","—")),
                     _kv("Home Delivery NPS", metrics.get("home_delivery_nps","—")),
                     _kv("Cafe NPS", metrics.get("cafe_nps","—")),
                     _kv("Click & Collect NPS", metrics.get("click_collect_nps","—")),
                     _kv("Customer Toilet NPS", metrics.get("customer_toilet_nps","—"))]},
        {"widgets": [{"textParagraph": {"text": "<b>Front End Service</b>"}},
                     _kv("SCO Utilisation", metrics.get("sco_utilisation","—")),
                     _kv("Efficiency", metrics.get("efficiency","—")),
                     _kv("Scan Rate", f"{metrics.get('scan_rate','—')} (vs {metrics.get('scan_vs_target','—')})"),
                     _kv("Interventions", f"{metrics.get('interventions','—')} (vs {metrics.get('interventions_vs_target','—')})"),
                     _kv("Mainbank Closed", f"{metrics.get('mainbank_closed','—')} (vs {metrics.get('mainbank_vs_target','—')})")]},
        {"widgets": [{"textParagraph": {"text": "<b>Online</b>"}},
                     _kv("Availability", metrics.get("availability_pct","—")),
                     _kv("Despatched on Time", metrics.get("despatched_on_time","—")),
                     _kv("Delivered on Time", metrics.get("delivered_on_time","—")),
                     _kv("Click & Collect Avg Wait", metrics.get("cc_avg_wait","—"))]},
        {"widgets": [{"textParagraph": {"text": "<b>Waste & Markdowns (Total)</b>"}},
                     _kv("Waste", metrics.get("waste_total","—")),
                     _kv("Markdowns", metrics.get("markdowns_total","—")),
                     _kv("Total", metrics.get("wm_total","—")),
                     _kv("+/−", metrics.get("wm_delta","—")),
                     _kv("+/− %", metrics.get("wm_delta_pct","—"))]},
        {"widgets": [{"textParagraph": {"text": "<b>Payroll</b>"}},
                     _kv("Payroll Outturn", metrics.get("payroll_outturn","—")),
                     _kv("Absence Outturn", metrics.get("absence_outturn","—")),
                     _kv("Productive Outturn", metrics.get("productive_outturn","—")),
                     _kv("Holiday Outturn", metrics.get("holiday_outturn","—")),
                     _kv("Current Base Cost", metrics.get("current_base_cost","—"))]},
        {"widgets": [{"textParagraph": {"text": "<b>Shrink</b>"}},
                     _kv("Morrisons Order Adjustments", metrics.get("moa","—")),
                     _kv("Waste Validation", metrics.get("waste_validation","—")),
                     _kv("Unrecorded Waste %", metrics.get("unrecorded_waste_pct","—")),
                     _kv("Shrink vs Budget %", metrics.get("shrink_vs_budget_pct","—"))]},
        {"widgets": [{"textParagraph": {"text": "<b>Card Engagement & Misc</b>"}},
                     _kv("Swipe Rate", metrics.get("swipe_rate","—")),
                     _kv("Swipes WOW %", metrics.get("swipes_wow_pct","—")),
                     _kv("New Customers", metrics.get("new_customers","—")),
                     _kv("Swipes YOY %", metrics.get("swipes_yoy_pct","—")),
                     _kv("Key Complaints", metrics.get("complaints_key","—")),
                     _kv("Data Provided", metrics.get("data_provided","—")),
                     _kv("Trusted Data", metrics.get("trusted_data","—")),
                     _kv("My Reports", metrics.get("my_reports","—")),
                     _kv("Weekly Activity %", metrics.get("weekly_activity","—"))]},
    ]
    return {"cardsV2": [{"cardId": f"daily_{int(time.time())}", "card": {"header": header, "sections": sections}}]}

def send_daily_card(metrics: Dict[str, str]) -> bool:
    if not MAIN_WEBHOOK or "chat.googleapis.com" not in MAIN_WEBHOOK:
        logger.error("MAIN_WEBHOOK/DAILY_WEBHOOK missing or invalid — cannot send daily report.")
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

            if not open_dashboard_and_normalise(page):
                alert(["⚠️ Daily scrape blocked by login or load failure — please re-login."])
                return

            text = capture_fullpage_ocr(page)
            if not text:
                logger.error("No OCR text extracted — skipping.")
                return
            # (Optional) also dump raw HTML for forensics
            _save_text(SCREENS_DIR / f"{int(time.time())}_page.html", page.content())

            metrics = parse_metrics_ocr(text)

        finally:
            try:
                if context: context.close()
            except Exception:
                pass
            try:
                if browser: browser.close()
            except Exception:
                pass

    ok = send_daily_card(metrics)
    logger.info("Daily card send → %s", "OK" if ok else "FAIL")

    # CSV logging (same schema)
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
