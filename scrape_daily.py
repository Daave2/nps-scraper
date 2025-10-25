#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary (layout-by-lines + ROI OCR) → Google Chat

Strategy
- Prefer fast, stable parsing from the BODY TEXT (assumed near-fixed line order).
- Use anchors to carve subsections and regex within each sub-block.
- Fill missing metrics (esp. community viz tiles) with ROI OCR from a normalised map.
- Emit debug artefacts: full screenshot, numbered lines dump, and ROI overlay when used.

Requires:
  pip install playwright requests pillow pytesseract
  python -m playwright install --with-deps chromium
  (Ubuntu runners typically have tesseract-ocr; locally install it if needed.)
"""

import os
import re
import csv
import json
import time
import logging
import configparser
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Optional OCR deps
try:
    from PIL import Image, ImageDraw
    import pytesseract
    OCR_AVAILABLE = True
except Exception:
    OCR_AVAILABLE = False

# ──────────────────────────────────────────────────────────────────────────────
# Paths / constants
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).resolve().parent
AUTH_STATE     = BASE_DIR / "auth_state.json"
LOG_FILE       = BASE_DIR / "scrape_daily.log"
DAILY_LOG_CSV  = BASE_DIR / "daily_report_log.csv"
SCREENS_DIR    = BASE_DIR / "screens"

# Optional external map path (env takes precedence)
ENV_ROI_MAP    = os.getenv("ROI_MAP_FILE", "").strip()
ROI_MAP_FILE   = Path(ENV_ROI_MAP) if ENV_ROI_MAP else (BASE_DIR / "roi_map.json")

DASHBOARD_URL = (
    "https://lookerstudio.google.com/embed/u/0/reporting/"
    "d93a03c7-25dc-439d-abaa-dd2f3780daa5/page/BLfDE"
    "?params=%7B%22f20f0n9kld%22:%22include%25EE%2580%25803%25EE%2580%2580T%22%7D"
)

# Fixed viewport helps screenshots be consistent (normalised ROIs still work if different)
VIEWPORT = {"width": 1366, "height": 768}

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE)],
)
log = logging.getLogger("daily")
log.addHandler(logging.StreamHandler())

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
config = configparser.ConfigParser()
config.read(BASE_DIR / "config.ini")

MAIN_WEBHOOK  = config["DEFAULT"].get("DAILY_WEBHOOK") or config["DEFAULT"].get("MAIN_WEBHOOK", os.getenv("MAIN_WEBHOOK", ""))
ALERT_WEBHOOK = config["DEFAULT"].get("ALERT_WEBHOOK",  os.getenv("ALERT_WEBHOOK", ""))
CI_RUN_URL    = os.getenv("CI_RUN_URL", "")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers: Chat + file saves
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
                log.error(f"429 from webhook — sleeping {delay:.1f}s")
                time.sleep(delay)
                backoff = min(backoff * 1.7, max_backoff)
                continue
            log.error(f"Webhook error {r.status_code}: {r.text[:300]}")
            return False
        except Exception as e:
            log.error(f"Webhook exception: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 1.7, max_backoff)

def alert(lines: List[str]):
    if not ALERT_WEBHOOK or "chat.googleapis.com" not in ALERT_WEBHOOK:
        log.warning("No valid ALERT_WEBHOOK configured.")
        return
    if CI_RUN_URL:
        lines.append(f"• CI run: {CI_RUN_URL}")
    _post_with_backoff(ALERT_WEBHOOK, {"text": "\n".join(lines)})

def save_bytes(path: Path, data: bytes):
    try:
        SCREENS_DIR.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        log.info(f"Saved {path.name}")
    except Exception:
        pass

def save_text(path: Path, text: str):
    try:
        SCREENS_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        log.info(f"Saved {path.name}")
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────────────────────
# Browser automation
# ──────────────────────────────────────────────────────────────────────────────
def click_this_week(page):
    # Try a few heuristics to hit "This Week"
    try:
        el = page.get_by_role("button", name=re.compile(r"^This Week$", re.I))
        if el.count():
            el.first.click(timeout=2000)
            page.wait_for_timeout(600)
            return True
    except Exception:
        pass
    try:
        el = page.get_by_text(re.compile(r"^\s*This Week\s*$", re.I))
        if el.count():
            el.first.click(timeout=2000)
            page.wait_for_timeout(600)
            return True
    except Exception:
        pass
    return False

def click_proceed_overlays(page) -> int:
    clicked = 0
    for fr in page.frames:
        try:
            btn = fr.get_by_text("PROCEED", exact=True)
            for i in range(btn.count()):
                try:
                    btn.nth(i).click(timeout=1200)
                    clicked += 1
                    fr.wait_for_timeout(300)
                except Exception:
                    continue
        except Exception:
            continue
    if clicked:
        log.info(f"Clicked {clicked} 'PROCEED' overlay(s). Waiting for render…")
        page.wait_for_timeout(1200)
    return clicked

def open_and_prepare(page) -> bool:
    log.info("Opening Retail Performance Dashboard…")
    try:
        page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
    except PlaywrightTimeoutError:
        log.error("Timeout loading dashboard.")
        return False

    if "accounts.google.com" in page.url:
        log.warning("Redirected to login — auth state missing/invalid.")
        return False

    log.info("Waiting 12s for dynamic content…")
    page.wait_for_timeout(12_000)

    click_this_week(page)
    click_proceed_overlays(page)

    # Retry if community viz disclaimer text still present
    try:
        body = page.inner_text("body")
    except Exception:
        body = ""
    if "You are about to interact with a community visualisation" in body:
        log.info("Community visualisation placeholders detected — retrying PROCEED and waiting longer.")
        click_proceed_overlays(page)
        page.wait_for_timeout(1500)

    return True

# ──────────────────────────────────────────────────────────────────────────────
# Screenshot + OCR helpers
# ──────────────────────────────────────────────────────────────────────────────
def screenshot_full(page) -> Optional[Image.Image]:
    try:
        img_bytes = page.screenshot(full_page=True, type="png")
        ts = int(time.time())
        save_bytes(SCREENS_DIR / f"{ts}_fullpage.png", img_bytes)
        return Image.open(BytesIO(img_bytes))
    except Exception as e:
        log.error(f"Full-page screenshot failed: {e}")
        return None

def ocr_image(img: Image.Image, *, psm: int = 6) -> str:
    if not OCR_AVAILABLE or img is None:
        return ""
    try:
        # Upscale a little to help with small UI text
        w, h = img.size
        if max(w, h) < 1400:
            scale = 1400 / max(w, h)
            img = img.resize((int(w * scale), int(h * scale)))
        txt = pytesseract.image_to_string(img, config=f"--psm {psm}")
        return txt or ""
    except Exception:
        return ""

# ──────────────────────────────────────────────────────────────────────────────
# Text parsing (layout-by-lines)
# ──────────────────────────────────────────────────────────────────────────────
def get_body_text(page) -> str:
    """Get the longest body text (page or frames)."""
    best, best_len = "", 0
    try:
        t = page.inner_text("body")
        if t and len(t) > best_len:
            best, best_len = t, len(t)
    except Exception:
        pass
    for fr in page.frames:
        try:
            fr.wait_for_selector("body", timeout=3000)
            t = fr.locator("body").inner_text(timeout=5000)
            if t and len(t) > best_len:
                best, best_len = t, len(t)
        except Exception:
            continue
    return best

def dump_numbered_lines(txt: str) -> List[str]:
    lines = [ln.rstrip() for ln in txt.splitlines()]
    ts = int(time.time())
    numbered = "\n".join(f"{i:04d} | {ln}" for i, ln in enumerate(lines))
    save_text(SCREENS_DIR / f"{ts}_lines.txt", numbered)
    return lines

# regex helpers
NUM = r"[£]?-?\d+(?:\.\d+)?(?:[KMB]|%)?"
TIME_RE = re.compile(r"\b\d{2}:\d{2}\b")

def find_between(lines: List[str], start: str, end: str) -> List[str]:
    """Slice lines between first 'start' and next 'end' anchors (inclusive start, exclusive end)."""
    try:
        s = next(i for i, ln in enumerate(lines) if start.lower() in ln.lower())
    except StopIteration:
        return []
    try:
        e = next(i for i, ln in enumerate(lines[s+1:], start=s+1) if end.lower() in ln.lower())
    except StopIteration:
        e = len(lines)
    return lines[s:e]

def value_after_label(lines: List[str], label: str, lookahead: int = 5, accept_time=False) -> str:
    """Look for label, then pick first numeric token within the next few lines."""
    for i, ln in enumerate(lines):
        if label.lower() in ln.lower():
            window = " ".join(lines[i+1:i+1+lookahead])
            if accept_time:
                m = TIME_RE.search(window)
                if m:
                    return m.group(0)
            m = re.search(NUM, window, flags=re.I)
            if m:
                return m.group(0)
            return "—"
    return "—"

def parse_from_lines(lines: List[str]) -> Dict[str, str]:
    m: Dict[str, str] = {}

    # Context
    joined = "\n".join(lines)
    z = re.search(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}).*?\|\s*([^\|]+?)\s*\|\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", joined, flags=re.S)
    m["store_line"] = z.group(0).strip() if z else ""
    y = re.search(r"The data on this report is from:\s*([^\n]+)", joined)
    m["period_range"] = y.group(1).strip() if y else "—"
    x = re.search(r"\b(\d{1,2}\s+[A-Za-z]{3}\s+\d{4},\s*\d{2}:\d{2}:\d{2})\b", joined)
    m["page_timestamp"] = x.group(1) if x else "—"

    # Sales block
    sales_block = find_between(lines, "Sales", "Waste & Markdowns") or find_between(lines, "Sales", "Shrink")
    if sales_block:
        # Find the "Total" line of the sales table: Total, <value>, <LFL>, <vs Target>
        sb = "\n".join(sales_block[:120])
        r = re.search(r"Total\s*\n\s*(" + NUM + r")\s*\n\s*(" + NUM + r")\s*\n\s*(" + NUM + r")", sb, flags=re.I)
        if r:
            m["sales_total"], m["sales_lfl"], m["sales_vs_target"] = r.group(1), r.group(2), r.group(3)
        else:
            m["sales_total"] = m["sales_lfl"] = m["sales_vs_target"] = "—"
    else:
        m["sales_total"] = m["sales_lfl"] = m["sales_vs_target"] = "—"

    # Waste & Markdowns totals
    wm_block = find_between(lines, "Waste & Markdowns", "My Reports") or find_between(lines, "Waste & Markdowns", "Payroll")
    if wm_block:
        wb = "\n".join(wm_block[:100])
        r = re.search(
            r"Total\s*\n\s*(" + NUM + r")\s*\n\s*(" + NUM + r")\s*\n\s*(" + NUM + r")\s*\n\s*(" + NUM + r")\s*\n\s*(" + NUM + r")",
            wb, flags=re.I
        )
        if r:
            m["waste_total"], m["markdowns_total"], m["wm_total"], m["wm_delta"], m["wm_delta_pct"] = \
                r.group(1), r.group(2), r.group(3), r.group(4), r.group(5)
        else:
            m.update({k: "—" for k in ["waste_total","markdowns_total","wm_total","wm_delta","wm_delta_pct"]})
    else:
        m.update({k: "—" for k in ["waste_total","markdowns_total","wm_total","wm_delta","wm_delta_pct"]})

    # Payroll
    pay_block = find_between(lines, "Payroll", "More Card Engagement") or find_between(lines, "Payroll", "Online")
    if pay_block:
        m["payroll_outturn"]    = value_after_label(pay_block, "Payroll Outturn")
        m["absence_outturn"]    = value_after_label(pay_block, "Absence Outturn")
        m["productive_outturn"] = value_after_label(pay_block, "Productive Outturn")
        m["holiday_outturn"]    = value_after_label(pay_block, "Holiday Outturn")
        m["current_base_cost"]  = value_after_label(pay_block, "Current Base Cost")
    else:
        for k in ["payroll_outturn","absence_outturn","productive_outturn","holiday_outturn","current_base_cost"]:
            m[k] = "—"

    # Online
    online_block = find_between(lines, "Online", "Front End Service") or find_between(lines, "Online", "More Card Engagement")
    if online_block:
        m["availability_pct"]   = value_after_label(online_block, "Availability")
        m["despatched_on_time"] = value_after_label(online_block, "Despatched on Time")
        m["delivered_on_time"]  = value_after_label(online_block, "Delivered on Time")
        m["cc_avg_wait"]        = value_after_label(online_block, "average wait", accept_time=True)
    else:
        for k in ["availability_pct","despatched_on_time","delivered_on_time","cc_avg_wait"]:
            m[k] = "—"

    # Front End Service
    fes_block = find_between(lines, "Front End Service", "Privacy") or find_between(lines, "Front End Service", "More Card Engagement")
    if fes_block:
        m["sco_utilisation"]     = value_after_label(fes_block, "Sco Utilisation")
        m["efficiency"]          = value_after_label(fes_block, "Efficiency")
        m["scan_rate"]           = value_after_label(fes_block, "Scan Rate")
        m["scan_vs_target"]      = value_after_label(fes_block, "Scan Rate", lookahead=6)  # may remain "—" if absent
        m["interventions"]       = value_after_label(fes_block, "Interventions")
        m["interventions_vs_target"] = value_after_label(fes_block, "Interventions", lookahead=6)
        m["mainbank_closed"]     = value_after_label(fes_block, "Mainbank Closed")
        m["mainbank_vs_target"]  = value_after_label(fes_block, "Mainbank Closed", lookahead=6)
    else:
        for k in ["sco_utilisation","efficiency","scan_rate","scan_vs_target","interventions","interventions_vs_target","mainbank_closed","mainbank_vs_target"]:
            m[k] = "—"

    # Card Engagement
    ce_block = find_between(lines, "More Card Engagement", "Stock Record NPS") or find_between(lines, "More Card Engagement", "Production Planning")
    if ce_block:
        m["swipe_rate"]     = value_after_label(ce_block, "Swipe Rate")
        m["swipes_wow_pct"] = value_after_label(ce_block, "Swipes WOW")
        m["new_customers"]  = value_after_label(ce_block, "New Customers")
        m["swipes_yoy_pct"] = value_after_label(ce_block, "Swipes YOY")
    else:
        for k in ["swipe_rate","swipes_wow_pct","new_customers","swipes_yoy_pct"]:
            m[k] = "—"

    # Production Planning & misc tiles
    pp_block = find_between(lines, "Production Planning", "Shrink") or find_between(lines, "Production Planning", "Online")
    if pp_block:
        m["data_provided"] = value_after_label(pp_block, "Data Provided")
        m["trusted_data"]  = value_after_label(pp_block, "Trusted Data")
    else:
        m["data_provided"] = m["trusted_data"] = "—"

    # Shrink (row of circles)
    sh_block = find_between(lines, "Shrink", "Online") or find_between(lines, "Shrink", "Front End Service") or find_between(lines, "Shrink", "Privacy")
    if sh_block:
        m["moa"]                  = value_after_label(sh_block, "Order Adjustments")
        m["waste_validation"]     = value_after_label(sh_block, "Waste Validation")
        m["unrecorded_waste_pct"] = value_after_label(sh_block, "Unrecorded Waste")
        m["shrink_vs_budget_pct"] = value_after_label(sh_block, "Shrink vs Budget")
    else:
        for k in ["moa","waste_validation","unrecorded_waste_pct","shrink_vs_budget_pct"]:
            m[k] = "—"

    # Complaints + My Reports + Weekly Activity
    m["complaints_key"]  = value_after_label(lines, "Key Customer Complaints")
    m["my_reports"]      = value_after_label(lines, "My Reports")
    m["weekly_activity"] = value_after_label(lines, "Weekly Activity", lookahead=3)

    # NPS gauges (usually missing in text if community viz): initialise to "—"
    for k in ["supermarket_nps","colleague_happiness","home_delivery_nps","cafe_nps","click_collect_nps","customer_toilet_nps"]:
        m.setdefault(k, "—")

    return m

# ──────────────────────────────────────────────────────────────────────────────
# ROI OCR fallback
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_ROI_MAP = {
    # Gauges row (rough defaults; override in roi_map.json)
    "colleague_happiness": (0.235, 0.205, 0.095, 0.135),
    "supermarket_nps":     (0.385, 0.205, 0.095, 0.135),
    "cafe_nps":            (0.535, 0.205, 0.095, 0.135),
    "click_collect_nps":   (0.685, 0.205, 0.095, 0.135),
    "home_delivery_nps":   (0.835, 0.205, 0.095, 0.135),
    "customer_toilet_nps": (0.955, 0.205, 0.095, 0.135),

    # Waste & Markdowns TOTAL row cells
    "waste_total":     (0.105, 0.415, 0.065, 0.035),
    "markdowns_total": (0.170, 0.415, 0.065, 0.035),
    "wm_total":        (0.235, 0.415, 0.065, 0.035),
    "wm_delta":        (0.300, 0.415, 0.065, 0.035),
    "wm_delta_pct":    (0.365, 0.415, 0.065, 0.035),

    # Online
    "availability_pct":   (0.455, 0.605, 0.065, 0.085),
    "despatched_on_time": (0.515, 0.585, 0.085, 0.055),
    "delivered_on_time":  (0.585, 0.585, 0.085, 0.055),
    "cc_avg_wait":        (0.615, 0.650, 0.065, 0.085),

    # Front End Service
    "sco_utilisation": (0.680, 0.590, 0.065, 0.060),
    "efficiency":      (0.940, 0.585, 0.090, 0.120),
    "scan_rate":       (0.680, 0.655, 0.065, 0.050),
    "interventions":   (0.810, 0.590, 0.065, 0.060),
    "mainbank_closed": (0.810, 0.655, 0.065, 0.050),
}

def load_roi_map() -> Dict[str, Tuple[float,float,float,float]]:
    roi = DEFAULT_ROI_MAP.copy()
    # allow external map override
    try:
        if ROI_MAP_FILE and Path(ROI_MAP_FILE).exists():
            overrides = json.loads(Path(ROI_MAP_FILE).read_text(encoding="utf-8"))
            roi.update(overrides)
            log.info(f"Loaded ROI overrides from roi_map.json: {len(overrides)} entrie(s).")
    except Exception as e:
        log.warning(f"Could not read roi_map.json: {e}")
    return roi

def crop_norm(img: Image.Image, roi: Tuple[float,float,float,float]) -> Image.Image:
    W, H = img.size
    x, y, w, h = roi
    box = (int(x*W), int(y*H), int((x+w)*W), int((y+h)*H))
    return img.crop(box)

def ocr_cell(img: Image.Image, want_time=False, allow_percent=True) -> str:
    if not OCR_AVAILABLE:
        return "—"
    try:
        w, h = img.size
        if max(w, h) < 240:
            img = img.resize((int(w*2), int(h*2)))
        txt = pytesseract.image_to_string(img, config="--psm 7")
        if want_time:
            m = TIME_RE.search(txt)
            if m: return m.group(0)
        m = re.search(r"[£]?-?\d+(?:\.\d+)?[KMB]?", txt, flags=re.I)
        if m and m.group(0): return m.group(0)
        if allow_percent:
            m = re.search(r"-?\d+(?:\.\d+)?%", txt)
            if m and m.group(0): return m.group(0)
        m = re.search(r"\b-?\d{1,3}\b", txt)
        if m and m.group(0): return m.group(0)
    except Exception:
        pass
    return "—"

def draw_overlay(img: Image.Image, roi_map: Dict[str, Tuple[float,float,float,float]]):
    try:
        dbg = img.copy()
        draw = ImageDraw.Draw(dbg)
        W, H = dbg.size
        for key, (x,y,w,h) in roi_map.items():
            box = (int(x*W), int(y*H), int((x+w)*W), int((y+h)*H))
            draw.rectangle(box, outline=(0,255,0), width=2)
            draw.text((box[0]+3, box[1]+3), key, fill=(0,255,0))
        ts = int(time.time())
        outfile = SCREENS_DIR / f"{ts}_roi_overlay.png"
        dbg.save(outfile)
        log.info(f"ROI overlay saved → {outfile.name}")
    except Exception:
        pass

def fill_missing_with_roi(metrics: Dict[str, str], img: Optional[Image.Image]):
    if img is None:
        return
    roi_map = load_roi_map()
    used = False
    for key, roi in roi_map.items():
        if metrics.get(key) and metrics[key] != "—":
            continue
        want_time = (key == "cc_avg_wait")
        allow_percent = not key.endswith("_nps")
        val = ocr_cell(crop_norm(img, roi), want_time=want_time, allow_percent=allow_percent)
        if val and val != "—":
            metrics[key] = val
            used = True
    if used:
        draw_overlay(img, roi_map)
        log.info("Filled some missing values from ROI OCR.")

# ──────────────────────────────────────────────────────────────────────────────
# Card + CSV
# ──────────────────────────────────────────────────────────────────────────────
def kv(label: str, val: str) -> dict:
    return {"decoratedText": {"topLabel": label, "text": (val or "—")}}

def title_widget(text: str) -> dict:
    return {"textParagraph": {"text": f"<b>{text}</b>"}}

def build_chat_card(metrics: Dict[str, str]) -> dict:
    header = {
        "title": "📊 Retail Daily Summary (Layout+ROI OCR)",
        "subtitle": (metrics.get("store_line") or "").replace("\n", "  "),
    }
    sections = [
        {"widgets": [kv("Report Time", metrics.get("page_timestamp","—")),
                     kv("Period",      metrics.get("period_range","—"))]},
        {"widgets": [title_widget("Sales & NPS"),
                     kv("Sales Total", metrics.get("sales_total","—")),
                     kv("LFL",         metrics.get("sales_lfl","—")),
                     kv("vs Target",   metrics.get("sales_vs_target","—")),
                     kv("Supermarket NPS",     metrics.get("supermarket_nps","—")),
                     kv("Colleague Happiness", metrics.get("colleague_happiness","—")),
                     kv("Home Delivery NPS",   metrics.get("home_delivery_nps","—")),
                     kv("Cafe NPS",            metrics.get("cafe_nps","—")),
                     kv("Click & Collect NPS", metrics.get("click_collect_nps","—")),
                     kv("Customer Toilet NPS", metrics.get("customer_toilet_nps","—"))]},
        {"widgets": [title_widget("Front End Service"),
                     kv("SCO Utilisation", metrics.get("sco_utilisation","—")),
                     kv("Efficiency",      metrics.get("efficiency","—")),
                     kv("Scan Rate",       f"{metrics.get('scan_rate','—')} (vs {metrics.get('scan_vs_target','—')})"),
                     kv("Interventions",   f"{metrics.get('interventions','—')} (vs {metrics.get('interventions_vs_target','—')})"),
                     kv("Mainbank Closed", f"{metrics.get('mainbank_closed','—')} (vs {metrics.get('mainbank_vs_target','—')})")]},
        {"widgets": [title_widget("Online"),
                     kv("Availability",              metrics.get("availability_pct","—")),
                     kv("Despatched on Time",        metrics.get("despatched_on_time","—")),
                     kv("Delivered on Time",         metrics.get("delivered_on_time","—")),
                     kv("Click & Collect Avg Wait",  metrics.get("cc_avg_wait","—"))]},
        {"widgets": [title_widget("Waste & Markdowns (Total)"),
                     kv("Waste",     metrics.get("waste_total","—")),
                     kv("Markdowns", metrics.get("markdowns_total","—")),
                     kv("Total",     metrics.get("wm_total","—")),
                     kv("+/−",       metrics.get("wm_delta","—")),
                     kv("+/− %",     metrics.get("wm_delta_pct","—"))]},
        {"widgets": [title_widget("Payroll"),
                     kv("Payroll Outturn",    metrics.get("payroll_outturn","—")),
                     kv("Absence Outturn",    metrics.get("absence_outturn","—")),
                     kv("Productive Outturn", metrics.get("productive_outturn","—")),
                     kv("Holiday Outturn",    metrics.get("holiday_outturn","—")),
                     kv("Current Base Cost",  metrics.get("current_base_cost","—"))]},
        {"widgets": [title_widget("Shrink"),
                     kv("Morrisons Order Adjustments", metrics.get("moa","—")),
                     kv("Waste Validation",            metrics.get("waste_validation","—")),
                     kv("Unrecorded Waste %",          metrics.get("unrecorded_waste_pct","—")),
                     kv("Shrink vs Budget %",          metrics.get("shrink_vs_budget_pct","—"))]},
        {"widgets": [title_widget("Card Engagement & Misc"),
                     kv("Swipe Rate",      metrics.get("swipe_rate","—")),
                     kv("Swipes WOW %",    metrics.get("swipes_wow_pct","—")),
                     kv("New Customers",   metrics.get("new_customers","—")),
                     kv("Swipes YOY %",    metrics.get("swipes_yoy_pct","—")),
                     kv("Key Complaints",  metrics.get("complaints_key","—")),
                     kv("Data Provided",   metrics.get("data_provided","—")),
                     kv("Trusted Data",    metrics.get("trusted_data","—")),
                     kv("My Reports",      metrics.get("my_reports","—")),
                     kv("Weekly Activity %",metrics.get("weekly_activity","—"))]},
    ]
    return {"cardsV2": [{"cardId": f"daily_{int(time.time())}", "card": {"header": header, "sections": sections}}]}

CSV_HEADERS = [
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

def write_csv(metrics: Dict[str,str]):
    write_header = not DAILY_LOG_CSV.exists() or DAILY_LOG_CSV.stat().st_size == 0
    with open(DAILY_LOG_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(CSV_HEADERS)
        w.writerow([metrics.get(h, "—") for h in CSV_HEADERS])
    log.info(f"Appended daily metrics row to {DAILY_LOG_CSV.name}")

def send_card(metrics: Dict[str, str]) -> bool:
    if not MAIN_WEBHOOK or "chat.googleapis.com" not in MAIN_WEBHOOK:
        log.error("MAIN_WEBHOOK/DAILY_WEBHOOK missing or invalid — cannot send daily report.")
        return False
    return _post_with_backoff(MAIN_WEBHOOK, build_chat_card(metrics))

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def run_daily_scrape():
    if not AUTH_STATE.exists():
        alert(["⚠️ Daily dashboard scrape needs login. Run `python scrape.py now` once to save auth_state.json."])
        log.error("auth_state.json not found.")
        return

    with sync_playwright() as p:
        browser = context = page = None
        metrics: Dict[str,str] = {}
        screenshot: Optional[Image.Image] = None
        try:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                storage_state=str(AUTH_STATE),
                viewport=VIEWPORT,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            )
            page = context.new_page()
            if not open_and_prepare(page):
                alert(["⚠️ Daily scrape blocked by login or load failure — please re-login."])
                return

            # Full screenshot (for ROI + debugging)
            screenshot = screenshot_full(page)

            # BODY TEXT → numbered lines → layout parser
            body_text = get_body_text(page)
            lines = dump_numbered_lines(body_text)
            metrics = parse_from_lines(lines)

            # Fill stubborn tiles with ROI OCR (esp. the NPS dials / pills)
            fill_missing_with_roi(metrics, screenshot)

        finally:
            try:
                if context: context.close()
            except Exception:
                pass
            try:
                if browser: browser.close()
            except Exception:
                pass

    ok = send_card(metrics)
    log.info("Daily card send → %s", "OK" if ok else "FAIL")
    write_csv(metrics)

if __name__ == "__main__":
    run_daily_scrape()
