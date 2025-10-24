#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary (OCR-first) → Google Chat

v2 highlights
- Hard-selects "Week" then "This Week" at the top strip (with retries)
- Clicks Community Viz "PROCEED" overlays if present
- Full-page screenshot → OCR → tolerant parsing near labels
- Normalises common OCR glitches (O/Q→0, l→1 in digit runs, funky dashes)
- Falls back to DOM-text for a few table-style totals (if OCR misses)
- Saves debug: PNG + extracted text + which source each metric used
- Same CSV schema / webhook payloads as before
"""

from __future__ import annotations
import os, re, csv, time, logging, configparser
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# OCR deps
from io import BytesIO
from PIL import Image, ImageOps
import pytesseract

# ──────────────────────────────────────────────────────────────────────────────
# Paths / constants
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
AUTH_STATE_PATH = BASE_DIR / "auth_state.json"
LOG_FILE_PATH    = BASE_DIR / "scrape_daily.log"
DAILY_LOG_CSV    = BASE_DIR / "daily_report_log.csv"
SCREENS_DIR      = BASE_DIR / "screens"

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
    handlers=[logging.FileHandler(LOG_FILE_PATH, encoding="utf-8"),
              logging.StreamHandler()],
)
logger = logging.getLogger("daily_ocr")

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
cfg = configparser.ConfigParser()
cfg.read(BASE_DIR / "config.ini")
MAIN_WEBHOOK  = cfg["DEFAULT"].get("DAILY_WEBHOOK") or cfg["DEFAULT"].get("MAIN_WEBHOOK", "")
ALERT_WEBHOOK = cfg["DEFAULT"].get("ALERT_WEBHOOK", "")
CI_RUN_URL    = os.getenv("CI_RUN_URL", "")

# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
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
                time.sleep(delay); backoff = min(backoff*1.7, max_backoff)
                continue
            logger.error(f"Webhook error {r.status_code}: {r.text[:300]}")
            return False
        except Exception as e:
            logger.error(f"Webhook exception: {e}")
            time.sleep(backoff); backoff = min(backoff*1.7, max_backoff)

def alert(lines: List[str]):
    if not ALERT_WEBHOOK or "chat.googleapis.com" not in ALERT_WEBHOOK:
        return
    if CI_RUN_URL:
        lines.append(f"• CI run: {CI_RUN_URL}")
    _post_with_backoff(ALERT_WEBHOOK, {"text": "\n".join(lines)})

def save_debug_blob(name_prefix: str, page=None, png_bytes: bytes | None = None, text: str | None = None):
    ts = int(time.time())
    SCREENS_DIR.mkdir(parents=True, exist_ok=True)
    if page and png_bytes is None:
        # full page
        try:
            png_bytes = page.screenshot(full_page=True)
        except Exception:
            png_bytes = None
    if png_bytes:
        (SCREENS_DIR / f"{ts}_{name_prefix}.png").write_bytes(png_bytes)
        logger.info(f"Saved screenshot → {ts}_{name_prefix}.png")
    if text is not None:
        (SCREENS_DIR / f"{ts}_{name_prefix}.txt").write_text(text, encoding="utf-8")
        logger.info(f"Saved text dump → {ts}_{name_prefix}.txt")

# ──────────────────────────────────────────────────────────────────────────────
# Page control
# ──────────────────────────────────────────────────────────────────────────────
def click_proceed_overlays(page) -> int:
    clicked = 0
    for fr in page.frames:
        try:
            btns = fr.get_by_text("PROCEED", exact=True)
            c = btns.count()
            for i in range(c):
                try:
                    btns.nth(i).click(timeout=1500)
                    clicked += 1
                    fr.wait_for_timeout(300)
                except Exception:
                    continue
        except Exception:
            continue
    if clicked:
        logger.info(f"Clicked {clicked} 'PROCEED' overlay(s).")
        page.wait_for_timeout(1200)
    return clicked

def ensure_this_week(page) -> None:
    """
    Clicks 'Week' then 'This Week' with a couple of strategies.
    """
    try:
        # Open the period granularity (Week)
        for sel in ["Week", " This Week "]:  # cover both toggles
            try:
                el = page.get_by_text(sel, exact=False).first
                if el and el.count():
                    el.click(timeout=2000)
                    page.wait_for_timeout(400)
            except Exception:
                pass

        # Explicitly open the quick range list and choose "This Week"
        opened = False
        for text in ["This Week", "ThisWeek"]:
            try:
                btn = page.get_by_text(text, exact=False)
                if btn.count():
                    btn.first.click(timeout=2000)
                    opened = True
                    break
            except Exception:
                continue

        if opened:
            page.wait_for_timeout(1500)

        # heuristic confirmation
        body = page.inner_text("body")
        if re.search(r"\bThis Week\b", body) or re.search(r"\bThisWeek\b", body):
            logger.info("Confirmed period selection includes 'This Week'.")
        else:
            logger.info("Could not confirm 'This Week' text — continuing anyway.")
    except Exception as e:
        logger.warning(f"ensure_this_week: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# OCR extraction
# ──────────────────────────────────────────────────────────────────────────────
def preprocess_for_ocr(img: Image.Image) -> Image.Image:
    # grayscale → contrast → binarize → upscale
    g = ImageOps.grayscale(img)
    g = ImageOps.autocontrast(g)
    # light binarization; keep thin red digits
    g = g.point(lambda x: 0 if x < 170 else 255, mode='1')
    w, h = g.size
    if max(w, h) < 1800:
        g = g.resize((int(w*1.8), int(h*1.8)))
    return g

def ocr_full_page(png_bytes: bytes) -> str:
    img = Image.open(BytesIO(png_bytes))
    proc = preprocess_for_ocr(img)
    txt = pytesseract.image_to_string(proc, config="--psm 6")
    return normalise_ocr_text(txt)

def normalise_ocr_text(txt: str) -> str:
    # fix common OCR glitches
    txt = txt.replace("—", "-").replace("–", "-")
    txt = txt.replace("O", "0").replace("o", "0")
    txt = txt.replace("Q", "0")
    # 1→l swaps inside digit runs
    txt = re.sub(r"(?<=\d)l(?=\d)", "1", txt)
    # collapse multiple spaces
    txt = re.sub(r"[ \t]+", " ", txt)
    return txt

# ──────────────────────────────────────────────────────────────────────────────
# Parsing (OCR-first, tolerant, near-label)
# ──────────────────────────────────────────────────────────────────────────────
NumToken = r"[-£]?\s?(?:\d{1,3}(?:[,\.\s]\d{3})+|\d+(?:\.\d+)?)\s*[KMBkmb]?"
PctToken = r"-?\d+(?:\.\d+)?%"

def _near_label(text: str, label: str, window: int = 180) -> str:
    idx = text.find(label)
    if idx == -1:
        return "—"
    seg = text[idx: idx + window]
    # first small integer for gauges (e.g., -79 / 41 / 60 / 12)
    m = re.search(r"\b-?\d{1,3}\b", seg)
    return m.group(0) if m else "—"

def _pick_three_after_total(block: str) -> Tuple[str, str, str]:
    # capture 3 numbers in sequence, used for Sales Total/LFL/vs Target
    nums = re.findall(NumToken, block, flags=re.I)
    pcts = re.findall(PctToken, block, flags=re.I)
    # Try typical order: VALUE, PCT, DELTA
    v = nums[0] if nums else "—"
    p = pcts[0] if pcts else "—"
    # delta as number w/ optional -K
    m = re.search(rf"{NumToken}", block, flags=re.I)
    delta = "—"
    if nums:
        # choose a later numeric after we consumed first
        delta = nums[1] if len(nums) > 1 else "—"
    return v.strip(), p.strip(), delta.strip()

def parse_metrics_ocr(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}

    # Context
    out["page_timestamp"] = _match1(text, r"\b(\d{1,2}\s+[A-Za-z]{3}\s+\d{4},\s*\d{2}:\d{2}:\d{2})\b")
    out["period_range"]  = _match1(text, r"The data on this report is from:\s*([^\n]+)")
    out["store_line"]    = _match0(text, r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\s*\|\s*[^|]+?\|\s*\d{4}-\d{2}-\d{2}\s*\d{2}:\d{2}:\d{2})")

    # Sales (block near "Sales" then "Total")
    sales_idx = text.find("Sales")
    if sales_idx != -1:
        sales_block = text[sales_idx:sales_idx+1200]
        # find line starting with Total
        total_idx = sales_block.find("Total")
        if total_idx != -1:
            tb = sales_block[total_idx: total_idx+400]
            v, p, d = _pick_three_after_total(tb)
            out["sales_total"], out["sales_lfl"], out["sales_vs_target"] = v, p, d
    out.setdefault("sales_total", "—")
    out.setdefault("sales_lfl", "—")
    out.setdefault("sales_vs_target", "—")

    # Gauges (community viz) — near-label integer
    for key, label in [
        ("supermarket_nps", "Supermarket NPS"),
        ("colleague_happiness", "Colleague Happiness"),
        ("home_delivery_nps", "Home Delivery NPS"),
        ("cafe_nps", "Cafe NPS"),
        ("click_collect_nps", "Click & Collect NPS"),
        ("customer_toilet_nps", "Customer Toilet NPS"),
    ]:
        out[key] = _near_label(text, label, window=220)

    # Front End Service
    out["sco_utilisation"] = _near_after(text, "Sco Utilisation", kind="pct")
    out["efficiency"]      = _near_after(text, "Efficiency", kind="pct")
    out["scan_rate"]       = _near_after(text, "Scan Rate", kind="int")
    out["scan_vs_target"]  = _near_after(text, "Scan Rate", kind="pct", after="vs Target")
    out["interventions"]   = _near_after(text, "Interventions", kind="int")
    out["interventions_vs_target"] = _near_after(text, "Interventions", kind="pct", after="vs Target")
    out["mainbank_closed"] = _near_after(text, "Mainbank Closed", kind="int")
    out["mainbank_vs_target"] = _near_after(text, "Mainbank Closed", kind="pct", after="vs Target")

    # Online
    out["availability_pct"]   = _near_after(text, "Availability", kind="pct")
    out["despatched_on_time"] = _near_after(text, "Despatched on Time", kind="pct_or_no")
    out["delivered_on_time"]  = _near_after(text, "Delivered on Time",   kind="pct_or_no")
    out["cc_avg_wait"]        = _match1_near(text, "Click & Collect average wait", r"\b\d{2}:\d{2}\b", 200)

    # Waste & Markdowns (Total row = 5 tokens)
    wm_idx = text.find("Waste & Markdowns")
    if wm_idx != -1:
        block = text[wm_idx: wm_idx+1200]
        # find "Total" row chunk
        t_idx = block.find("Total")
        if t_idx != -1:
            tb = block[t_idx: t_idx+400]
            nums = re.findall(NumToken, tb)
            pcts = re.findall(PctToken, tb)
            if len(nums) >= 4 and pcts:
                out["waste_total"]     = nums[0].strip()
                out["markdowns_total"] = nums[1].strip()
                out["wm_total"]        = nums[2].strip()
                out["wm_delta"]        = nums[3].strip()
                out["wm_delta_pct"]    = pcts[0].strip()
    for k in ["waste_total","markdowns_total","wm_total","wm_delta","wm_delta_pct"]:
        out.setdefault(k, "—")

    # Payroll
    for key, label in [
        ("payroll_outturn", "Payroll Outturn"),
        ("absence_outturn", "Absence Outturn"),
        ("productive_outturn", "Productive Outturn"),
        ("holiday_outturn", "Holiday Outturn"),
        ("current_base_cost", "Current Base Cost"),
    ]:
        out[key] = _match1_near(text, label, NumToken, 160)

    # Shrink
    out["moa"]                  = _match1_near(text, "Morrisons Order Adjustments", NumToken, 200)
    out["waste_validation"]     = _match1_near(text, "Waste Validation", r"\b\d{1,3}%\b", 120)
    out["unrecorded_waste_pct"] = _match1_near(text, "Unrecorded Waste %", PctToken, 200)
    out["shrink_vs_budget_pct"] = _match1_near(text, "Shrink vs Budget %", PctToken, 200)

    # Card Engagement
    out["swipe_rate"]    = _near_after(text, "Swipe Rate", kind="pct")
    out["swipes_wow_pct"]= _near_after(text, "Swipes WOW %", kind="pct")
    out["new_customers"] = _near_after(text, "New Customers", kind="int_or_commas")
    out["swipes_yoy_pct"]= _near_after(text, "Swipes YOY %", kind="pct")

    # Misc
    out["complaints_key"] = _near_after(text, "Key Customer Complaints", kind="int")
    out["data_provided"]  = _near_after(text, "Data Provided", kind="pct")
    out["trusted_data"]   = _near_after(text, "Trusted Data", kind="pct")
    out["my_reports"]     = _near_after(text, "My Reports", kind="int_or_commas")
    out["weekly_activity"]= _near_after(text, "Weekly Activity %", kind="pct_or_no")

    return out

def _match1(s: str, pat: str) -> str:
    m = re.search(pat, s)
    return m.group(1).strip() if m else "—"

def _match0(s: str, pat: str) -> str:
    m = re.search(pat, s)
    return m.group(0).strip() if m else "—"

def _match1_near(s: str, label: str, pat: str, win: int) -> str:
    idx = s.find(label)
    if idx == -1: return "—"
    seg = s[idx: idx+win]
    m = re.search(pat, seg, flags=re.I)
    return m.group(0).strip() if m else "—"

def _near_after(s: str, label: str, *, kind: str = "int", after: Optional[str] = None, win: int = 220) -> str:
    idx = s.find(label)
    if idx == -1:
        return "—"
    seg = s[idx: idx+win]
    if after:
        a = seg.find(after)
        if a != -1:
            seg = seg[a: a+win]
    if kind == "pct":
        m = re.search(PctToken, seg)
    elif kind == "pct_or_no":
        m = re.search(r"(?:No data|"+PctToken+")", seg, flags=re.I)
    elif kind == "int_or_commas":
        m = re.search(r"\b\d{1,3}(?:,\d{3})*\b", seg)
    elif kind == "int":
        m = re.search(r"\b\d{1,3}\b", seg)
    else:
        m = re.search(NumToken, seg)
    return m.group(0).strip() if m else "—"

# ──────────────────────────────────────────────────────────────────────────────
# DOM fallback (for a few table totals)
# ──────────────────────────────────────────────────────────────────────────────
def dom_fill_some(page, metrics: Dict[str, str]) -> Dict[str, str]:
    # Sales table: sometimes plain text in DOM (not canvas)
    try:
        txt = page.inner_text("body")
        if metrics.get("sales_total") in ("—","",None):
            m = re.search(r"Sales.*?Total.*?\b([£]?[0-9.,]+[KMB]?)\b.*?\b([+-]?\d+%)\b.*?\b([+-]?[£]?[0-9.,]+[KMB]?)\b",
                          txt, flags=re.S|re.I)
            if m:
                metrics["sales_total"] = m.group(1)
                metrics["sales_lfl"] = m.group(2)
                metrics["sales_vs_target"] = m.group(3)
        # Waste & Markdowns total
        if metrics.get("wm_total") in ("—","",None):
            m = re.search(r"Waste\s*&\s*Markdowns.*?Total\s*\n\s*([£]?[0-9.,]+[KMB]?)\s*\n\s*([£]?[0-9.,]+[KMB]?)\s*\n\s*([£]?[0-9.,]+[KMB]?)\s*\n\s*([+-]?[£]?[0-9.,]+[KMB]?)\s*\n\s*([+-]?\d+\.?\d*%)",
                          txt, flags=re.S|re.I)
            if m:
                metrics["waste_total"]=m.group(1); metrics["markdowns_total"]=m.group(2)
                metrics["wm_total"]=m.group(3); metrics["wm_delta"]=m.group(4); metrics["wm_delta_pct"]=m.group(5)
    except Exception:
        pass
    return metrics

# ──────────────────────────────────────────────────────────────────────────────
# Card + CSV
# ──────────────────────────────────────────────────────────────────────────────
def build_chat_card(metrics: Dict[str, str]) -> dict:
    def kv(label: str, val: str) -> dict:
        return {"decoratedText": {"topLabel": label, "text": val or "—"}}

    header = {
        "title": "📊 Retail Daily Summary (OCR)",
        "subtitle": (metrics.get("store_line") or "").replace("\n", "  "),
    }

    sections = [
        {"widgets": [kv("Report Time", metrics.get("page_timestamp","—")),
                     kv("Period", metrics.get("period_range","—"))]},
        {"widgets": [
            {"textParagraph": {"text": "<b>Sales & NPS</b>"}},
            kv("Sales Total", metrics.get("sales_total","—")),
            kv("LFL", metrics.get("sales_lfl","—")),
            kv("vs Target", metrics.get("sales_vs_target","—")),
            kv("Supermarket NPS", metrics.get("supermarket_nps","—")),
            kv("Colleague Happiness", metrics.get("colleague_happiness","—")),
            kv("Home Delivery NPS", metrics.get("home_delivery_nps","—")),
            kv("Cafe NPS", metrics.get("cafe_nps","—")),
            kv("Click & Collect NPS", metrics.get("click_collect_nps","—")),
            kv("Customer Toilet NPS", metrics.get("customer_toilet_nps","—")),
        ]},
        {"widgets": [
            {"textParagraph": {"text": "<b>Front End Service</b>"}},
            kv("SCO Utilisation", metrics.get("sco_utilisation","—")),
            kv("Efficiency", metrics.get("efficiency","—")),
            kv("Scan Rate", f"{metrics.get('scan_rate','—')} (vs {metrics.get('scan_vs_target','—')})"),
            kv("Interventions", f"{metrics.get('interventions','—')} (vs {metrics.get('interventions_vs_target','—')})"),
            kv("Mainbank Closed", f"{metrics.get('mainbank_closed','—')} (vs {metrics.get('mainbank_vs_target','—')})"),
        ]},
        {"widgets": [
            {"textParagraph": {"text": "<b>Online</b>"}},
            kv("Availability", metrics.get("availability_pct","—")),
            kv("Despatched on Time", metrics.get("despatched_on_time","—")),
            kv("Delivered on Time", metrics.get("delivered_on_time","—")),
            kv("Click & Collect Avg Wait", metrics.get("cc_avg_wait","—")),
        ]},
        {"widgets": [
            {"textParagraph": {"text": "<b>Waste & Markdowns (Total)</b>"}},
            kv("Waste", metrics.get("waste_total","—")),
            kv("Markdowns", metrics.get("markdowns_total","—")),
            kv("Total", metrics.get("wm_total","—")),
            kv("+/−", metrics.get("wm_delta","—")),
            kv("+/− %", metrics.get("wm_delta_pct","—")),
        ]},
        {"widgets": [
            {"textParagraph": {"text": "<b>Payroll</b>"}},
            kv("Payroll Outturn", metrics.get("payroll_outturn","—")),
            kv("Absence Outturn", metrics.get("absence_outturn","—")),
            kv("Productive Outturn", metrics.get("productive_outturn","—")),
            kv("Holiday Outturn", metrics.get("holiday_outturn","—")),
            kv("Current Base Cost", metrics.get("current_base_cost","—")),
        ]},
        {"widgets": [
            {"textParagraph": {"text": "<b>Shrink</b>"}},
            kv("Morrisons Order Adjustments", metrics.get("moa","—")),
            kv("Waste Validation", metrics.get("waste_validation","—")),
            kv("Unrecorded Waste %", metrics.get("unrecorded_waste_pct","—")),
            kv("Shrink vs Budget %", metrics.get("shrink_vs_budget_pct","—")),
        ]},
        {"widgets": [
            {"textParagraph": {"text": "<b>Card Engagement & Misc</b>"}},
            kv("Swipe Rate", metrics.get("swipe_rate","—")),
            kv("Swipes WOW %", metrics.get("swipes_wow_pct","—")),
            kv("New Customers", metrics.get("new_customers","—")),
            kv("Swipes YOY %", metrics.get("swipes_yoy_pct","—")),
            kv("Key Complaints", metrics.get("complaints_key","—")),
            kv("Data Provided", metrics.get("data_provided","—")),
            kv("Trusted Data", metrics.get("trusted_data","—")),
            kv("My Reports", metrics.get("my_reports","—")),
            kv("Weekly Activity %", metrics.get("weekly_activity","—")),
        ]},
    ]

    return {"cardsV2": [{"cardId": f"daily_{int(time.time())}",
                         "card": {"header": header, "sections": sections}}]}

def send_daily_card(metrics: Dict[str, str]) -> bool:
    if not MAIN_WEBHOOK or "chat.googleapis.com" not in MAIN_WEBHOOK:
        logger.error("DAILY_WEBHOOK/MAIN_WEBHOOK missing/invalid.")
        return False
    return _post_with_backoff(MAIN_WEBHOOK, build_chat_card(metrics))

def append_csv(metrics: Dict[str, str]) -> None:
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
    write_header = not DAILY_LOG_CSV.exists() or DAILY_LOG_CSV.stat().st_size == 0
    with open(DAILY_LOG_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header: w.writerow(headers)
        w.writerow([metrics.get(h, "—") for h in headers])
    logger.info(f"Appended daily metrics row to {DAILY_LOG_CSV.name}")

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
            browser = p.chromium.launch(headless=True, args=["--disable-web-security"])
            context = browser.new_context(storage_state=str(AUTH_STATE_PATH), viewport={"width": 1600, "height": 1000})
            page = context.new_page()

            logger.info("Opening Retail Performance Dashboard…")
            page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
            if "accounts.google.com" in page.url:
                alert(["⚠️ Daily scrape blocked by login — please re-login (run NPS scraper)."])
                return

            logger.info("Waiting 6s for initial paint…")
            page.wait_for_timeout(6000)

            # Ensure period = This Week; click viz overlays
            ensure_this_week(page)
            click_proceed_overlays(page)

            # Give charts a bit more time
            page.wait_for_timeout(3000)

            # If the “community visualisation” warning is still present, wait a bit more and click again
            body_text = page.inner_text("body")
            if "You are about to interact with a community visualisation" in body_text:
                logger.info("Community visualisation placeholders detected — retrying PROCEED and waiting longer.")
                click_proceed_overlays(page)
                page.wait_for_timeout(3000)

            # Full-page screenshot → OCR
            png = page.screenshot(full_page=True)
            ocr_txt = ocr_full_page(png)
            save_debug_blob("daily_full_ocr", png_bytes=png, text=ocr_txt)

            # Parse metrics (OCR)
            metrics = parse_metrics_ocr(ocr_txt)

            # Try a small DOM fill for some structured text if OCR missed
            metrics = dom_fill_some(page, metrics)

        finally:
            try:
                if context: context.close()
            except Exception: pass
            try:
                if browser: browser.close()
            except Exception: pass

    ok = send_daily_card(metrics)
    logger.info("Daily card send → %s", "OK" if ok else "FAIL")
    append_csv(metrics)

# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_daily_scrape()
