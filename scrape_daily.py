#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Daily Summary → Google Chat  (STRICT OCR ONLY)

- NO DOM/body innerText scraping for metrics.
- Clicks 'Week' -> 'This Week' and 'PROCEED' overlays (best-effort).
- Full-page screenshot -> Tesseract OCR -> parse blocks.
- Saves PNG + OCR text on every run for debugging.
"""

import os, re, csv, time, logging, configparser, hashlib
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional

import requests
from PIL import Image
import pytesseract
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ── Paths/constants ──────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).resolve().parent
AUTH_STATE_PATH = BASE_DIR / "auth_state.json"
LOG_FILE_PATH   = BASE_DIR / "scrape_daily.log"
DAILY_LOG_CSV   = BASE_DIR / "daily_report_log.csv"
SCREENS_DIR     = BASE_DIR / "screens"

DASHBOARD_URL = (
    "https://lookerstudio.google.com/embed/u/0/reporting/"
    "d93a03c7-25dc-439d-abaa-dd2f3780daa5/page/BLfDE"
    "?params=%7B%22f20f0n9kld%22:%22include%25EE%2580%25803%25EE%2580%2580T%22%7D"
)
COMMUNITY_VIZ_PLACEHOLDER = "You are about to interact with a community visualisation"

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE_PATH)],
)
logger = logging.getLogger("daily_ocr_only")
logger.addHandler(logging.StreamHandler())

# ── Config (file → env) ──────────────────────────────────────────────────────
cfg = configparser.ConfigParser()
cfg.read(BASE_DIR / "config.ini")

MAIN_WEBHOOK = (
    cfg["DEFAULT"].get("DAILY_WEBHOOK")
    or cfg["DEFAULT"].get("MAIN_WEBHOOK")
    or os.getenv("DAILY_WEBHOOK")
    or os.getenv("MAIN_WEBHOOK", "")
)
ALERT_WEBHOOK = cfg["DEFAULT"].get("ALERT_WEBHOOK", os.getenv("ALERT_WEBHOOK", ""))

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
                time.sleep(delay); backoff = min(backoff * 1.7, max_backoff); continue
            logger.error(f"Webhook error {r.status_code}: {r.text[:300]}")
            return False
        except Exception as e:
            logger.error(f"Webhook exception: {e}")
            time.sleep(backoff); backoff = min(backoff * 1.7, max_backoff)

def alert(msg: str):
    if ALERT_WEBHOOK and "chat.googleapis.com" in ALERT_WEBHOOK:
        _post_with_backoff(ALERT_WEBHOOK, {"text": msg})

# ── Page actions (no text extraction) ────────────────────────────────────────
def _click_by_text(page_or_frame, label: str, exact=True, timeout=1800) -> bool:
    try:
        loc = page_or_frame.get_by_text(label, exact=exact)
        if loc.count() > 0:
            loc.first.click(timeout=timeout)
            return True
    except Exception:
        pass
    return False

def set_this_week(page):
    # best-effort; safe if already selected
    if _click_by_text(page, "Week", exact=True) or _click_by_text(page, "Week", exact=False):
        page.wait_for_timeout(500)
    if _click_by_text(page, "This Week", exact=True) or _click_by_text(page, "This Week", exact=False):
        page.wait_for_timeout(900)
    logger.info("Period set (attempted): Week → This Week")

def click_proceed_overlays(page, retries=2):
    total = 0
    for _ in range(retries):
        clicked = 0
        for fr in page.frames:
            try:
                btns = fr.get_by_text(re.compile(r"\bPROCEED\b", re.I))
                n = btns.count()
                for i in range(n):
                    try:
                        btns.nth(i).click(timeout=1500)
                        clicked += 1
                        fr.wait_for_timeout(200)
                    except Exception:
                        continue
            except Exception:
                continue
        total += clicked
        if clicked == 0:
            break
        page.wait_for_timeout(1200)
    if total:
        logger.info(f"Clicked {total} PROCEED overlay(s).")

# ── OCR (single full-page) ───────────────────────────────────────────────────
def screenshot_and_ocr(page) -> str:
    SCREENS_DIR.mkdir(parents=True, exist_ok=True)
    png_bytes = page.screenshot(full_page=True)
    ts = int(time.time())
    png_path = SCREENS_DIR / f"{ts}_fullpage.png"
    png_path.write_bytes(png_bytes)
    logger.info(f"Saved screenshot → {png_path.name}")

    img = Image.open(BytesIO(png_bytes))
    try:
        img = img.convert("L")
        w, h = img.size
        if max(w, h) < 1600:
            img = img.resize((int(w*1.5), int(h*1.5)))
    except Exception:
        pass

    text = pytesseract.image_to_string(img, config="--psm 6")
    txt_path = SCREENS_DIR / f"{ts}_ocr.txt"
    txt_path.write_text(text, encoding="utf-8")
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
    logger.info(f"OCR dump saved → {txt_path.name} (sha {sha})")
    logger.info("SOURCE=OCR_ONLY (no DOM text read)")
    return text

def get_ocr_text() -> Optional[str]:
    if not AUTH_STATE_PATH.exists():
        alert("⚠️ Daily OCR scrape needs login. Run the NPS scraper once to create auth_state.json.")
        logger.error("auth_state.json missing.")
        return None

    with sync_playwright() as p:
        browser = context = page = None
        try:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                storage_state=str(AUTH_STATE_PATH),
                viewport={"width": 1600, "height": 1200},
                device_scale_factor=2,
            )
            page = context.new_page()
            logger.info("Opening dashboard…")
            try:
                page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
            except PlaywrightTimeoutError:
                logger.error("Timeout loading dashboard.")
                # Still try to OCR whatever rendered
                return screenshot_and_ocr(page)

            if "accounts.google.com" in page.url:
                logger.error("Redirected to login (auth expired).")
                return None

            page.wait_for_timeout(6000)
            set_this_week(page)
            click_proceed_overlays(page)
            page.wait_for_timeout(1000)
            # We only read page.content() for placeholder detection (not for metrics)
            try:
                if COMMUNITY_VIZ_PLACEHOLDER in (page.content() or ""):
                    logger.warning("Community viz placeholder detected — retrying PROCEED & waiting longer.")
                    click_proceed_overlays(page, retries=1)
                    page.wait_for_timeout(2500)
            except Exception:
                pass

            return screenshot_and_ocr(page)

        finally:
            try:
                if context: context.close()
            except Exception:
                pass
            try:
                if browser: browser.close()
            except Exception:
                pass

# ── Parsing helpers (from OCR text only) ─────────────────────────────────────
def _first(pats: List[str], text: str, flags=0, group=1, default="—") -> str:
    for pat in pats:
        m = re.search(pat, text, flags)
        if m:
            try:
                return m.group(group).strip()
            except Exception:
                return m.group(0).strip()
    return default

def _block(text: str, start: str, ends: List[str]) -> str:
    s = re.search(re.escape(start), text, flags=re.I)
    if not s: return ""
    epos = len(text)
    for e in ends:
        m = re.search(re.escape(e), text[s.end():], flags=re.I)
        if m: epos = min(epos, s.end()+m.start())
    return text[s.start():epos]

def _near_after(label: str, block: str, pat: str, window: int = 140) -> str:
    m = re.search(re.escape(label), block, flags=re.I)
    if not m: return "—"
    snippet = block[m.end(): m.end()+window]
    m2 = re.search(pat, snippet, flags=re.I)
    return (m2.group(1).strip() if m2 else "—")

def _clean_num(s: str) -> str:
    return s.replace(" ", "")

# ── Parse metrics from OCR ───────────────────────────────────────────────────
def parse_metrics(ocr: str) -> Dict[str, str]:
    out: Dict[str, str] = {}

    # Header/context
    out["period_range"] = _first([r"The data on this report is from:\s*([^\n]+)"], ocr, flags=re.I)
    out["page_timestamp"] = _first([r"\b(\d{1,2}\s+[A-Za-z]{3}\s+\d{4},\s*\d{2}:\d{2}:\d{2})\b"], ocr)
    out["store_line"] = _first(
        [r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}).*?\|\s*.*?\|\s*\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}"],
        ocr, flags=re.S, group=0
    )

    # Sales (tolerant to OCR spacing)
    m = re.search(
        r"Sales.*?Total\s+([£-]?\s?[0-9.,]+[KMB]?)\s+([+-]?\d+(?:\.\d+)?%)\s+([£+-]?\s?[0-9.,]+[KMB]?)",
        ocr, flags=re.I | re.S
    )
    if m:
        out["sales_total"]     = _clean_num(m.group(1))
        out["sales_lfl"]       = m.group(2)
        out["sales_vs_target"] = _clean_num(m.group(3))
    else:
        out["sales_total"] = out["sales_lfl"] = out["sales_vs_target"] = "—"

    # NPS near labels
    def near_int(label): return _near_after(label, ocr, r"(-?\d{1,3})", window=110)
    out["supermarket_nps"]     = near_int("Supermarket NPS")
    out["colleague_happiness"] = near_int("Colleague Happiness")
    out["home_delivery_nps"]   = near_int("Home Delivery NPS")
    out["cafe_nps"]            = near_int("Cafe NPS")
    out["click_collect_nps"]   = near_int("Click & Collect NPS")
    out["customer_toilet_nps"] = near_int("Customer Toilet NPS")

    # Front End Service
    fes = _block(ocr, "Front End Service", ["Production Planning", "More Card Engagement", "Online", "Privacy"])
    out["sco_utilisation"] = _near_after("Sco Utilisation", fes, r"(-?\d{1,3}(?:\.\d+)?%)")
    out["efficiency"]      = _near_after("Efficiency", fes, r"(-?\d{1,3}(?:\.\d+)?%)")
    sr = re.search(r"Scan Rate\s+(\d{1,4}).{0,50}?vs Target\s+(-?\d+(?:\.\d+)?)", fes, flags=re.I | re.S)
    out["scan_rate"], out["scan_vs_target"] = (sr.group(1), sr.group(2)) if sr else ("—","—")
    it = re.search(r"Interventions\s+(\d{1,4}).{0,50}?vs Target\s+(-?\d+(?:\.\d+)?)", fes, flags=re.I | re.S)
    out["interventions"], out["interventions_vs_target"] = (it.group(1), it.group(2)) if it else ("—","—")
    mb = re.search(r"Mainbank Closed\s+(\d{1,4}).{0,60}?vs Target\s+(-?\d+(?:\.\d+)?)", fes, flags=re.I | re.S)
    out["mainbank_closed"], out["mainbank_vs_target"] = (mb.group(1), mb.group(2)) if mb else ("—","—")

    # Online
    online = _block(ocr, "Online", ["Front End Service", "Production Planning", "Privacy"])
    out["availability_pct"]   = _near_after("Availability", online, r"(-?\d{1,3}(?:\.\d+)?%)")
    out["despatched_on_time"] = _near_after("Despatched on Time", online, r"(-?\d{1,3}(?:\.\d+)?%)")
    out["delivered_on_time"]  = _near_after("Delivered on Time", online, r"(-?\d{1,3}(?:\.\d+)?%)")
    out["cc_avg_wait"]        = _near_after("Click & Collect", online, r"(\b\d{2}:\d{2}\b)", window=80)

    # Waste & Markdowns
    wm = _block(ocr, "Waste & Markdowns", ["My Reports", "Clean & Rotate", "Payroll", "Online", "Privacy"])
    rx = re.search(
        r"Total\s+([£-]?\s?[0-9.,]+[KMB]?)\s+([£-]?\s?[0-9.,]+[KMB]?)\s+([£-]?\s?[0-9.,]+[KMB]?)\s+([£+-]?\s?[0-9.,]+[KMB]?)\s+(-?\d+(?:\.\d+)?%)",
        wm, flags=re.I | re.S
    )
    if rx:
        out["waste_total"]   = _clean_num(rx.group(1))
        out["markdowns_total"] = _clean_num(rx.group(2))
        out["wm_total"]      = _clean_num(rx.group(3))
        out["wm_delta"]      = _clean_num(rx.group(4))
        out["wm_delta_pct"]  = rx.group(5)
    else:
        out.update({k:"—" for k in ["waste_total","markdowns_total","wm_total","wm_delta","wm_delta_pct"]})

    # Payroll
    pay = _block(ocr, "Payroll", ["More Card Engagement", "Stock Record NPS", "Online", "Privacy"])
    def mny(lbl): return _clean_num(_near_after(lbl, pay, r"([£-]?\s?[0-9.,]+[KMB]?)"))
    out["payroll_outturn"]    = mny("Payroll Outturn")
    out["absence_outturn"]    = mny("Absence Outturn")
    out["productive_outturn"] = mny("Productive Outturn")
    out["holiday_outturn"]    = mny("Holiday Outturn")
    out["current_base_cost"]  = mny("Current Base Cost")

    # Shrink
    shr = _block(ocr, "Shrink", ["Online", "Production Planning", "Privacy"])
    out["moa"]                  = _clean_num(_near_after("Morrisons Order Adjustments", shr, r"([£-]?\s?[0-9.,]+[KMB]?)"))
    out["waste_validation"]     = _near_after("Waste Validation", shr, r"(-?\d{1,3}(?:\.\d+)?%)")
    out["unrecorded_waste_pct"] = _near_after("Unrecorded Waste %", shr, r"(-?\d{1,3}(?:\.\d+)?%)")
    out["shrink_vs_budget_pct"] = _near_after("Shrink vs Budget %", shr, r"(-?\d{1,3}(?:\.\d+)?%)")

    # Card Engagement
    ce = _block(ocr, "More Card Engagement", ["Stock Record NPS", "Production Planning", "Privacy"])
    out["swipe_rate"]     = _near_after("Swipe Rate", ce, r"(-?\d{1,3}(?:\.\d+)?%)")
    out["swipes_wow_pct"] = _near_after("Swipes WOW %", ce, r"(-?\d{1,3}(?:\.\d+)?%)")
    out["new_customers"]  = _near_after("New Customers", ce, r"([0-9,]{1,8})")
    out["swipes_yoy_pct"] = _near_after("Swipes YOY %", ce, r"(-?\d{1,3}(?:\.\d+)?%)")

    # Production Planning
    pp = _block(ocr, "Production Planning", ["Shrink", "Online", "Privacy"])
    out["data_provided"] = _near_after("Data Provided", pp, r"(-?\d{1,3}(?:\.\d+)?%)")
    out["trusted_data"]  = _near_after("Trusted Data", pp, r"(-?\d{1,3}(?:\.\d+)?%)")

    # Misc
    out["complaints_key"] = _near_after("Key Customer Complaints", ocr, r"(\d{1,5})")
    out["my_reports"]     = _near_after("My Reports", ocr, r"([0-9,]{1,6})")
    out["weekly_activity"]= _near_after("Weekly Activity %", ocr, r"(-?\d{1,3}(?:\.\d+)?%)")

    return out

# ── Chat card (unchanged layout) ─────────────────────────────────────────────
def build_chat_card(m: Dict[str,str]) -> dict:
    def t(x): return {"textParagraph": {"text": f"<b>{x}</b>"}}
    def kv(k, v): return {"decoratedText": {"topLabel": k, "text": v or "—"}}
    header = {"title": "📊 Retail Daily Summary (OCR)", "subtitle": (m.get("store_line") or "").replace("\n","  ")}
    sections = [
        {"widgets":[kv("Report Time", m.get("page_timestamp","—")), kv("Period", m.get("period_range","—"))]},
        {"widgets":[t("Sales & NPS"),
            kv("Sales Total", m.get("sales_total","—")), kv("LFL", m.get("sales_lfl","—")), kv("vs Target", m.get("sales_vs_target","—")),
            kv("Supermarket NPS", m.get("supermarket_nps","—")), kv("Colleague Happiness", m.get("colleague_happiness","—")),
            kv("Home Delivery NPS", m.get("home_delivery_nps","—")), kv("Cafe NPS", m.get("cafe_nps","—")),
            kv("Click & Collect NPS", m.get("click_collect_nps","—")), kv("Customer Toilet NPS", m.get("customer_toilet_nps","—"))]},
        {"widgets":[t("Front End Service"),
            kv("SCO Utilisation", m.get("sco_utilisation","—")), kv("Efficiency", m.get("efficiency","—")),
            kv("Scan Rate", f"{m.get('scan_rate','—')} (vs {m.get('scan_vs_target','—')})"),
            kv("Interventions", f"{m.get('interventions','—')} (vs {m.get('interventions_vs_target','—')})"),
            kv("Mainbank Closed", f"{m.get('mainbank_closed','—')} (vs {m.get('mainbank_vs_target','—')})")]},
        {"widgets":[t("Online"),
            kv("Availability", m.get("availability_pct","—")), kv("Despatched on Time", m.get("despatched_on_time","—")),
            kv("Delivered on Time", m.get("delivered_on_time","—")), kv("Click & Collect Avg Wait", m.get("cc_avg_wait","—"))]},
        {"widgets":[t("Waste & Markdowns (Total)"),
            kv("Waste", m.get("waste_total","—")), kv("Markdowns", m.get("markdowns_total","—")),
            kv("Total", m.get("wm_total","—")), kv("+/−", m.get("wm_delta","—")), kv("+/− %", m.get("wm_delta_pct","—"))]},
        {"widgets":[t("Payroll"),
            kv("Payroll Outturn", m.get("payroll_outturn","—")), kv("Absence Outturn", m.get("absence_outturn","—")),
            kv("Productive Outturn", m.get("productive_outturn","—")), kv("Holiday Outturn", m.get("holiday_outturn","—")),
            kv("Current Base Cost", m.get("current_base_cost","—"))]},
        {"widgets":[t("Shrink"),
            kv("Morrisons Order Adjustments", m.get("moa","—")), kv("Waste Validation", m.get("waste_validation","—")),
            kv("Unrecorded Waste %", m.get("unrecorded_waste_pct","—")), kv("Shrink vs Budget %", m.get("shrink_vs_budget_pct","—"))]},
        {"widgets":[t("Card Engagement & Misc"),
            kv("Swipe Rate", m.get("swipe_rate","—")), kv("Swipes WOW %", m.get("swipes_wow_pct","—")),
            kv("New Customers", m.get("new_customers","—")), kv("Swipes YOY %", m.get("swipes_yoy_pct","—")),
            kv("Key Complaints", m.get("complaints_key","—")), kv("Data Provided", m.get("data_provided","—")),
            kv("Trusted Data", m.get("trusted_data","—")), kv("My Reports", m.get("my_reports","—")),
            kv("Weekly Activity %", m.get("weekly_activity","—"))]},
    ]
    return {"cardsV2":[{"cardId": f"daily_{int(time.time())}", "card":{"header": header, "sections": sections}}]}

def send_card(metrics: Dict[str,str]) -> bool:
    if not MAIN_WEBHOOK or "chat.googleapis.com" not in MAIN_WEBHOOK:
        logger.error("MAIN_WEBHOOK/DAILY_WEBHOOK missing/invalid.")
        return False
    return _post_with_backoff(MAIN_WEBHOOK, build_chat_card(metrics))

# ── Main ─────────────────────────────────────────────────────────────────────
def run():
    ocr = get_ocr_text()
    if ocr is None:
        return
    if not ocr.strip():
        logger.error("OCR returned empty text.")
        return
    metrics = parse_metrics(ocr)
    ok = send_card(metrics)
    logger.info("Daily card send → %s", "OK" if ok else "FAIL")

    # CSV logging (unchanged schema)
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
        if write_header: w.writerow(headers)
        w.writerow(row)
    logger.info("Appended daily metrics row to %s", DAILY_LOG_CSV.name)

if __name__ == "__main__":
    run()
