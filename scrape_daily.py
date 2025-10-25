#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary (layout-by-lines + ROI OCR) → Google Chat

Updates:
- Sales 'Total' row: FIRST 'Total' AFTER the 'Sales' header → capture next 3 numeric tokens.
- Front End Service: scoped values + correctly paired "vs Target" per KPI.
- Online/Complaints/Payroll/Shrink/Card Engagement: nearest, bidirectional, typed extraction.
- Availability: prefer a % within 3 lines ABOVE the label, else search nearby (prevents CE bleed).
- C&C average wait: nearest HH:MM to the label (wide window).
- Waste & Markdowns: robust block regex (Total row with (+/-) and (+/-)%).
- Keeps ROI OCR fallback and debug artifacts (full screenshot, numbered lines, ROI overlay).
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

ENV_ROI_MAP    = os.getenv("ROI_MAP_FILE", "").strip()
ROI_MAP_FILE   = Path(ENV_ROI_MAP) if ENV_ROI_MAP else (BASE_DIR / "roi_map.json")

DASHBOARD_URL = (
    "https://lookerstudio.google.com/embed/u/0/reporting/"
    "d93a03c7-25dc-439d-abaa-dd2f3780daa5/page/BLfDE"
    "?params=%7B%22f20f0n9kld%22:%22include%25EE%2580%25803%25EE%2580%2580T%22%7D"
)

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
# Screenshot helper
# ──────────────────────────────────────────────────────────────────────────────
def screenshot_full(page) -> Optional["Image.Image"]:
    try:
        img_bytes = page.screenshot(full_page=True, type="png")
        ts = int(time.time())
        save_bytes(SCREENS_DIR / f"{ts}_fullpage.png", img_bytes)
        from PIL import Image  # lazy import
        return Image.open(BytesIO(img_bytes))
    except Exception as e:
        log.error(f"Full-page screenshot failed: {e}")
        return None

# ──────────────────────────────────────────────────────────────────────────────
# Text parsing (layout-by-lines) — Deterministic rules
# ──────────────────────────────────────────────────────────────────────────────
NUM_ANY_RE   = re.compile(r"[£]?-?\d+(?:\.\d+)?(?:[KMB]|%)?", re.I)
NUM_INT_RE   = re.compile(r"\b-?\d+\b")
NUM_PCT_RE   = re.compile(r"-?\d+(?:\.\d+)?%")
NUM_MONEY_RE = re.compile(r"[£]-?\d+(?:\.\d+)?[KMB]?", re.I)
TIME_RE      = re.compile(r"\b\d{2}:\d{2}\b")

EMAILLOC = re.compile(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}).*?\|\s*([^\|]+?)\s*\|\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", re.S)
PERIOD_RE= re.compile(r"The data on this report is from:\s*([^\n]+)")
STAMP_RE = re.compile(r"\b(\d{1,2}\s+[A-Za-z]{3}\s+\d{4},\s*\d{2}:\d{2}:\d{2})\b")

def get_body_text(page) -> str:
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

def index_of_label(lines: List[str], label: str, start: int = 0, end: Optional[int] = None) -> int:
    end = len(lines) if end is None else end
    for i in range(start, end):
        if label.lower() in lines[i].lower():
            return i
    return -1

def _contains_num_of_type(s: str, kind: str) -> Optional[str]:
    if kind == "time":
        m = TIME_RE.search(s); return m.group(0) if m else None
    if kind == "percent":
        m = NUM_PCT_RE.search(s); return m.group(0) if m else None
    if kind == "integer":
        m = NUM_INT_RE.search(s); return m.group(0) if m else None
    if kind == "money":
        m = NUM_MONEY_RE.search(s)
        if m: return m.group(0)
        m2 = re.search(r"-?\d+(?:\.\d+)?[KMB]", s, re.I); return m2.group(0) if m2 else None
    m = NUM_ANY_RE.search(s); return m.group(0) if m else None

def nearest_num_of_type(lines: List[str], idx: int, window: int, *, prefer_before_first: int = 0, kind: str = "any") -> str:
    """
    Scan symmetrically outward from idx within ±window, optionally first try up to
    `prefer_before_first` lines above the label (useful for Availability 84% sitting above).
    """
    # First, a small bias search above the label
    if prefer_before_first > 0:
        for i in range(max(0, idx - prefer_before_first), idx):
            v = _contains_num_of_type(lines[i], kind)
            if v:
                return v
    # Then symmetric outward search
    for d in range(1, window + 1):
        j = idx + d
        if j < len(lines):
            v = _contains_num_of_type(lines[j], kind)
            if v: return v
        k = idx - d
        if k >= 0:
            v = _contains_num_of_type(lines[k], kind)
            if v: return v
    return "—"

def sales_three_after_total(lines: List[str]) -> Optional[Tuple[str,str,str]]:
    """‘Sales’ → first ‘Total’ after → next 3 numeric tokens across following lines."""
    i_sales = index_of_label(lines, "Sales", start=0)
    if i_sales < 0:
        return None
    for i in range(i_sales + 1, min(len(lines), i_sales + 200)):
        if lines[i].strip().lower() == "total":
            collected: List[str] = []
            for j in range(i + 1, min(len(lines), i + 40)):
                toks = NUM_ANY_RE.findall(lines[j])
                for t in toks:
                    collected.append(t)
                    if len(collected) == 3:
                        return collected[0], collected[1], collected[2]
            break
    return None

def parse_from_lines(lines: List[str]) -> Dict[str, str]:
    m: Dict[str, str] = {}

    # Context
    joined = "\n".join(lines)
    z = EMAILLOC.search(joined); m["store_line"]   = z.group(0).strip() if z else ""
    y = PERIOD_RE.search(joined); m["period_range"] = y.group(1).strip() if y else "—"
    x = STAMP_RE.search(joined);  m["page_timestamp"]= x.group(1) if x else "—"

    # Sales
    res = sales_three_after_total(lines)
    if res:
        m["sales_total"], m["sales_lfl"], m["sales_vs_target"] = res
    else:
        m["sales_total"] = m["sales_lfl"] = m["sales_vs_target"] = "—"

    # Waste & Markdowns (Total row around (+/-)% or section header)
    pivot = index_of_label(lines, "(+/-)%")
    if pivot < 0:
        pivot = index_of_label(lines, "Waste & Markdowns")
    if pivot >= 0:
        s = max(0, pivot - 60); e = min(len(lines), pivot + 80)
        window = "\n".join(lines[s:e])
        r = re.search(
            r"Total\s*\n\s*(" + NUM_ANY_RE.pattern + r")\s*\n\s*(" + NUM_ANY_RE.pattern + r")\s*\n\s*(" + NUM_ANY_RE.pattern + r")\s*\n\s*(" + NUM_ANY_RE.pattern + r")\s*\n\s*(" + NUM_ANY_RE.pattern + r")",
            window, flags=re.I
        )
        if r:
            m["waste_total"], m["markdowns_total"], m["wm_total"], m["wm_delta"], m["wm_delta_pct"] = \
                r.group(1), r.group(2), r.group(3), r.group(4), r.group(5)
        else:
            m.update({k: "—" for k in ["waste_total","markdowns_total","wm_total","wm_delta","wm_delta_pct"]})
    else:
        m.update({k: "—" for k in ["waste_total","markdowns_total","wm_total","wm_delta","wm_delta_pct"]})

    # ── Front End Service (scoped to its block; correct KPI ↔ vs Target)
    fes_start = index_of_label(lines, "Front End Service")
    anchors = []
    if fes_start >= 0:
        for a in ["More Card Engagement", "Privacy", "Stock Record NPS", "Production Planning", "Online"]:
            anchors.append(index_of_label(lines, a, start=fes_start+1))
    fes_end = min([i for i in anchors if i >= 0], default=len(lines)) if fes_start >= 0 else len(lines)

    def _fes_value(label: str, num_type: str) -> str:
        if fes_start < 0: return "—"
        li = index_of_label(lines, label, start=fes_start, end=fes_end)
        if li < 0: return "—"
        # search up to the local "vs Target" or next KPI label (whichever comes first)
        kpi_labels = ["Sco Utilisation","Efficiency","Scan Rate","Interventions","Mainbank Closed"]
        next_labels = [index_of_label(lines, l, start=li+1, end=fes_end) for l in kpi_labels]
        bound = min([i for i in next_labels if i >= 0], default=fes_end)
        vsi = index_of_label(lines, "vs Target", start=li+1, end=fes_end)
        limit = min([x for x in [bound, vsi if vsi >= 0 else fes_end] if x > li], default=fes_end)
        # try just after label; if blank (some UIs render above), try one line before
        v = nearest_num_of_type(lines, li, window=max(1, limit - li), kind=num_type)
        if v == "—":
            v = _contains_num_of_type(lines[li-1], num_type) if li-1 >= fes_start else None
            v = v or "—"
        return v

    def _fes_vs(label: str) -> str:
        if fes_start < 0: return "—"
        li = index_of_label(lines, label, start=fes_start, end=fes_end)
        if li < 0: return "—"
        vsi = index_of_label(lines, "vs Target", start=li+1, end=fes_end)
        if vsi < 0: return "—"
        kpi_labels = ["Sco Utilisation","Efficiency","Scan Rate","Interventions","Mainbank Closed"]
        next_labels = [index_of_label(lines, l, start=li+1, end=fes_end) for l in kpi_labels]
        bound = min([i for i in next_labels if i >= 0], default=fes_end)
        if vsi >= bound:
            return "—"
        return nearest_num_of_type(lines, vsi, window=min(3, bound - vsi), kind="any")

    m["sco_utilisation"]        = _fes_value("Sco Utilisation", "percent")
    m["efficiency"]             = _fes_value("Efficiency",      "percent")
    m["scan_rate"]              = _fes_value("Scan Rate",       "integer")
    m["interventions"]          = _fes_value("Interventions",   "integer")
    m["mainbank_closed"]        = _fes_value("Mainbank Closed", "integer")
    m["scan_vs_target"]         = _fes_vs("Scan Rate")
    m["interventions_vs_target"]= _fes_vs("Interventions")
    m["mainbank_vs_target"]     = _fes_vs("Mainbank Closed")

    # ── Online (global nearest-with-type; Availability prefers above-the-label hit)
    i_av  = index_of_label(lines, "Availability")
    i_dsp = index_of_label(lines, "Despatched on Time")
    i_del = index_of_label(lines, "Delivered on Time")
    i_wait= index_of_label(lines, "average wait")

    m["availability_pct"]   = nearest_num_of_type(lines, i_av,  window=30, prefer_before_first=3, kind="percent")  if i_av  >= 0 else "—"
    m["despatched_on_time"] = nearest_num_of_type(lines, i_dsp, window=30, prefer_before_first=0, kind="percent")   if i_dsp >= 0 else "—"
    m["delivered_on_time"]  = nearest_num_of_type(lines, i_del, window=30, prefer_before_first=0, kind="percent")   if i_del >= 0 else "—"
    m["cc_avg_wait"]        = nearest_num_of_type(lines, i_wait,window=80, prefer_before_first=0, kind="time")      if i_wait>= 0 else "—"

    # ── Payroll (nearest + types)
    for lbl, key in [
        ("Payroll Outturn", "payroll_outturn"),
        ("Absence Outturn", "absence_outturn"),
        ("Productive Outturn", "productive_outturn"),
        ("Holiday Outturn", "holiday_outturn"),
        ("Current Base Cost", "current_base_cost"),
    ]:
        i = index_of_label(lines, lbl)
        m[key] = nearest_num_of_type(lines, i, window=12, kind="any") if i >= 0 else "—"

    # ── Card Engagement (nearest + types)
    for lbl, key, kind in [
        ("Swipe Rate", "swipe_rate", "percent"),
        ("Swipes WOW", "swipes_wow_pct", "percent"),
        ("New Customers", "new_customers", "integer"),
        ("Swipes YOY", "swipes_yoy_pct", "percent"),
    ]:
        i = index_of_label(lines, lbl)
        m[key] = nearest_num_of_type(lines, i, window=12, kind=kind) if i >= 0 else "—"

    # ── Production Planning
    for lbl, key in [
        ("Data Provided", "data_provided"),
        ("Trusted Data", "trusted_data"),
    ]:
        i = index_of_label(lines, lbl)
        m[key] = nearest_num_of_type(lines, i, window=8, kind="percent") if i >= 0 else "—"

    # ── Shrink (typed; MOA can be money/KMB)
    i_moa  = index_of_label(lines, "Morrisons Order Adjustments")
    i_wv   = index_of_label(lines, "Waste Validation")
    i_urw  = index_of_label(lines, "Unrecorded Waste")
    i_svb  = index_of_label(lines, "Shrink vs Budget")
    m["moa"]                  = nearest_num_of_type(lines, i_moa, window=12, kind="money")   if i_moa >= 0 else "—"
    m["waste_validation"]     = nearest_num_of_type(lines, i_wv,  window=12, kind="percent") if i_wv  >= 0 else "—"
    m["unrecorded_waste_pct"] = nearest_num_of_type(lines, i_urw, window=12, kind="percent") if i_urw >= 0 else "—"
    m["shrink_vs_budget_pct"] = nearest_num_of_type(lines, i_svb, window=12, kind="percent") if i_svb >= 0 else "—"

    # ── Complaints / My Reports / Weekly Activity
    i_kcc = index_of_label(lines, "Key Customer Complaints")
    i_cc  = index_of_label(lines, "Customer Complaints")
    val = nearest_num_of_type(lines, i_kcc, window=10, kind="integer") if i_kcc >= 0 else "—"
    if val == "—" and i_cc >= 0:
        val = nearest_num_of_type(lines, i_cc, window=10, kind="integer")
    m["complaints_key"]  = val
    i_mr = index_of_label(lines, "My Reports");      m["my_reports"]      = nearest_num_of_type(lines, i_mr, window=10, kind="integer") if i_mr >= 0 else "—"
    i_wa = index_of_label(lines, "Weekly Activity"); m["weekly_activity"] = nearest_num_of_type(lines, i_wa, window=10, kind="any")      if i_wa >= 0 else "—"

    # Gauges via ROI OCR later
    for k in ["supermarket_nps","colleague_happiness","home_delivery_nps","cafe_nps","click_collect_nps","customer_toilet_nps"]:
        m.setdefault(k, "—")

    return m

# ──────────────────────────────────────────────────────────────────────────────
# ROI OCR fallback
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_ROI_MAP = {
    # Gauges row (override in roi_map.json if needed)
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
    try:
        if ROI_MAP_FILE and Path(ROI_MAP_FILE).exists():
            overrides = json.loads(Path(ROI_MAP_FILE).read_text(encoding="utf-8"))
            roi.update(overrides)
            log.info(f"Loaded ROI overrides from roi_map.json: {len(overrides)} entrie(s).")
    except Exception as e:
        log.warning(f"Could not read roi_map.json: {e}")
    return roi

def crop_norm(img: "Image.Image", roi: Tuple[float,float,float,float]) -> "Image.Image":
    from PIL import Image  # type: ignore
    W, H = img.size
    x, y, w, h = roi
    box = (int(x*W), int(y*H), int((x+w)*W), int((y+h)*H))
    return img.crop(box)

def ocr_cell(img: "Image.Image", want_time=False, allow_percent=True) -> str:
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

def draw_overlay(img: "Image.Image", roi_map: Dict[str, Tuple[float,float,float,float]]):
    try:
        from PIL import ImageDraw  # type: ignore
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

def fill_missing_with_roi(metrics: Dict[str, str], img: Optional["Image.Image"]):
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

    from PIL import Image  # ensure PIL available for type refs
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

            # Screenshot for ROI + debugging
            screenshot = screenshot_full(page)

            # BODY TEXT → numbered lines → layout parser
            body_text = get_body_text(page)
            lines = dump_numbered_lines(body_text)
            metrics = parse_from_lines(lines)

            # Fill stubborn tiles with ROI OCR (esp. gauges / online wait)
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
