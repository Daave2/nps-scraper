#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary (OCR-first) → Google Chat

Key fixes:
- OCR uses pytesseract Output.DICT (no pandas dependency).
- Viewport fixed to 1366x768; screenshot is viewport-only to match ROI map.
- ROI map scaled from a base size (default 1366x768; overridable in roi_map.json).
- Saves per-ROI crops and an overlay to validate alignment.

Requires:
  pip install playwright requests pillow pytesseract
  python -m playwright install --with-deps chromium
  (apt-get install -y tesseract-ocr on runners or locally)
"""

import os
import re
import csv
import json
import time
import logging
import configparser
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# OCR deps
from PIL import Image, ImageDraw
from io import BytesIO
import pytesseract
from pytesseract import Output

# ──────────────────────────────────────────────────────────────────────────────
# Paths / constants
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
AUTH_STATE_PATH = BASE_DIR / "auth_state.json"
LOG_FILE_PATH   = BASE_DIR / "scrape_daily.log"
DAILY_LOG_CSV   = BASE_DIR / "daily_report_log.csv"
SCREENS_DIR     = BASE_DIR / "screens"
ROI_JSON_PATH   = BASE_DIR / "roi_map.json"

DASHBOARD_URL = (
    "https://lookerstudio.google.com/embed/u/0/reporting/"
    "d93a03c7-25dc-439d-abaa-dd2f3780daa5/page/BLfDE"
    "?params=%7B%22f20f0n9kld%22:%22include%25EE%2580%25803%25EE%2580%2580T%22%7D"
)

# Viewport used for ROI map
VIEWPORT = {"width": 1366, "height": 768, "device_scale_factor": 1}

# ROI base size (used to scale normalized ROIs to screenshot pixels)
ROI_BASE_W = 1366
ROI_BASE_H = 768

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE_PATH)],
)
log = logging.getLogger("daily")
log.addHandler(logging.StreamHandler())

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
config = configparser.ConfigParser()
config.read(BASE_DIR / "config.ini")

MAIN_WEBHOOK   = config["DEFAULT"].get("DAILY_WEBHOOK") or config["DEFAULT"].get("MAIN_WEBHOOK", os.getenv("MAIN_WEBHOOK", ""))
ALERT_WEBHOOK  = config["DEFAULT"].get("ALERT_WEBHOOK",  os.getenv("ALERT_WEBHOOK", ""))
CI_RUN_URL     = os.getenv("CI_RUN_URL", "")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers (Chat + file IO)
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
                time.sleep(delay); backoff = min(backoff * 1.7, max_backoff)
                continue
            log.error(f"Webhook error {r.status_code}: {r.text[:300]}")
            return False
        except Exception as e:
            log.error(f"Webhook exception: {e}")
            time.sleep(backoff); backoff = min(backoff * 1.7, max_backoff)

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
# ROI map (normalized coords in 0..1), with optional base size override
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_ROI_MAP: Dict[str, Tuple[float,float,float,float]] = {
    # Gauges row
    "colleague_happiness": (0.235, 0.215, 0.095, 0.140),
    "supermarket_nps":     (0.392, 0.215, 0.095, 0.140),
    "cafe_nps":            (0.546, 0.215, 0.095, 0.140),
    "click_collect_nps":   (0.700, 0.215, 0.095, 0.140),
    "home_delivery_nps":   (0.854, 0.215, 0.095, 0.140),
    "customer_toilet_nps": (0.916, 0.240, 0.060, 0.090),  # complaints gauge nearby

    # Waste & Markdowns (TOTAL row)
    "waste_total":     (0.095, 0.436, 0.065, 0.038),
    "markdowns_total": (0.167, 0.436, 0.065, 0.038),
    "wm_total":        (0.238, 0.436, 0.065, 0.038),
    "wm_delta":        (0.309, 0.436, 0.065, 0.038),
    "wm_delta_pct":    (0.380, 0.436, 0.065, 0.038),

    # Payroll
    "payroll_outturn":     (0.528, 0.468, 0.100, 0.130),
    "absence_outturn":     (0.636, 0.455, 0.055, 0.045),
    "productive_outturn":  (0.636, 0.505, 0.055, 0.045),
    "holiday_outturn":     (0.725, 0.455, 0.055, 0.045),
    "current_base_cost":   (0.725, 0.505, 0.055, 0.045),

    # Online
    "availability_pct":    (0.456, 0.616, 0.080, 0.100),
    "despatched_on_time":  (0.523, 0.583, 0.085, 0.050),
    "delivered_on_time":   (0.592, 0.583, 0.085, 0.050),
    "cc_avg_wait":         (0.622, 0.650, 0.080, 0.090),

    # Front End Service (bottom-right panel)
    "sco_utilisation":     (0.690, 0.606, 0.065, 0.060),
    "scan_rate":           (0.690, 0.666, 0.065, 0.060),
    "interventions":       (0.802, 0.606, 0.065, 0.060),
    "mainbank_closed":     (0.802, 0.666, 0.065, 0.060),
    "efficiency":          (0.935, 0.605, 0.095, 0.130),

    # Card Engagement
    "swipe_rate":          (0.830, 0.456, 0.060, 0.045),
    "swipes_wow_pct":      (0.902, 0.456, 0.060, 0.045),
    "new_customers":       (0.830, 0.505, 0.060, 0.045),
    "swipes_yoy_pct":      (0.902, 0.505, 0.060, 0.045),

    # Production planning
    "data_provided":       (0.065, 0.612, 0.070, 0.075),
    "trusted_data":        (0.065, 0.680, 0.070, 0.075),

    # Misc
    "my_reports":          (0.358, 0.470, 0.055, 0.080),
    "weekly_activity":     (0.444, 0.482, 0.070, 0.090),
    "complaints_key":      (0.915, 0.306, 0.050, 0.090),
}

def load_roi_map() -> Tuple[Dict[str, Tuple[float,float,float,float]], int, int]:
    rois = DEFAULT_ROI_MAP.copy()
    base_w, base_h = ROI_BASE_W, ROI_BASE_H
    if ROI_JSON_PATH.exists():
        try:
            payload = json.loads(ROI_JSON_PATH.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                if "base_width" in payload and "base_height" in payload:
                    base_w = int(payload["base_width"])
                    base_h = int(payload["base_height"])
                # merge any key->[x,y,w,h]
                for k, v in payload.items():
                    if k in ("base_width", "base_height"):
                        continue
                    if isinstance(v, (list, tuple)) and len(v) == 4:
                        rois[k] = tuple(float(x) for x in v)  # type: ignore
            log.info(f"Loaded ROI overrides from {ROI_JSON_PATH.name}: {len(payload.keys())} key(s).")
        except Exception as e:
            log.warning(f"Failed to parse {ROI_JSON_PATH.name}: {e}")
    return rois, base_w, base_h

# ──────────────────────────────────────────────────────────────────────────────
# Browser automation
# ──────────────────────────────────────────────────────────────────────────────
def click_this_week(page):
    tries = [
        page.get_by_role("button", name="This Week"),
        page.get_by_text("This Week"),
        page.get_by_text("This week"),
    ]
    for q in tries:
        try:
            if q.count():
                q.first.click(timeout=1500)
                page.wait_for_timeout(700)
                return True
        except Exception:
            pass
    return False

def click_proceed_overlays(page) -> int:
    clicked = 0
    for fr in page.frames:
        try:
            btns = fr.get_by_text("PROCEED", exact=True)
            n = btns.count()
            for i in range(n):
                try:
                    btns.nth(i).click(timeout=1200)
                    fr.wait_for_timeout(250)
                    clicked += 1
                except Exception:
                    continue
        except Exception:
            continue
    if clicked:
        log.info(f"Clicked {clicked} 'PROCEED' overlay(s). Waiting…")
        page.wait_for_timeout(1000)
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

    log.info("Waiting 10s for dynamic content…")
    page.wait_for_timeout(10_000)

    click_this_week(page)
    click_proceed_overlays(page)

    # Detect community viz placeholders and retry once
    try:
        body_text = page.inner_text("body")
    except Exception:
        body_text = ""
    if "You are about to interact with a community visualisation" in body_text:
        log.info("Community visualisation placeholders detected — retrying PROCEED and waiting longer.")
        click_proceed_overlays(page)
        page.wait_for_timeout(1500)

    return True

# ──────────────────────────────────────────────────────────────────────────────
# Screenshot + OCR
# ──────────────────────────────────────────────────────────────────────────────
def screenshot_viewport(page) -> Optional[Image.Image]:
    try:
        img_bytes = page.screenshot(full_page=False, type="png")  # viewport only
        ts = int(time.time())
        save_bytes(SCREENS_DIR / f"{ts}_viewport.png", img_bytes)
        return Image.open(BytesIO(img_bytes))
    except Exception as e:
        log.error(f"Viewport screenshot failed: {e}")
        return None

NumPat = re.compile(r"[£]?-?\d+(?:\.\d+)?(?:[KMB]|%)?|\b\d{2}:\d{2}\b", re.I)

def ocr_words(img: Image.Image) -> List[dict]:
    """
    Returns a list of dicts: {text,left,top,width,height,conf}
    Using pytesseract Output.DICT to avoid pandas dependency.
    """
    words: List[dict] = []
    try:
        W,H = img.size
        if max(W,H) < 1300:
            scale = 1300 / max(W,H)
            img = img.resize((int(W*scale), int(H*scale)))
        data = pytesseract.image_to_data(img, output_type=Output.DICT)
        n = len(data.get("text", []))
        for i in range(n):
            text = (data["text"][i] or "").strip()
            if not text:
                continue
            words.append({
                "text": text,
                "left": int(data.get("left", [0])[i]),
                "top": int(data.get("top", [0])[i]),
                "width": int(data.get("width", [0])[i]),
                "height": int(data.get("height", [0])[i]),
                "conf": float(data.get("conf", [-1])[i]),
            })
        txt = " ".join(w["text"] for w in words)
        save_text(SCREENS_DIR / f"{int(time.time())}_daily_text.txt", txt)
    except Exception as e:
        log.error(f"OCR words failed: {e}")
    return words

def nearest_number(words: List[dict], label: str, window: int = 180) -> str:
    labs = [w for w in words if w["text"].lower() == label.lower()]
    if not labs:
        labs = [w for w in words if label.lower() in w["text"].lower()]
    if not labs:
        return "—"
    lx, ly = labs[0]["left"], labs[0]["top"]
    best, bestd = None, 1e9
    for w in words:
        if w is labs[0]: 
            continue
        t = w["text"]
        if not NumPat.fullmatch(t):
            continue
        dx, dy = abs(w["left"] - lx), abs(w["top"] - ly)
        d = (dx*dx + dy*dy) ** 0.5
        if dx < window and dy < window and d < bestd:
            best, bestd = t, d
    return best if best else "—"

def build_metrics_from_ocr_labels(words: List[dict]) -> Dict[str, str]:
    m: Dict[str,str] = {}
    # Context via OCR is brittle; we will fill from DOM text later
    m["page_timestamp"] = "—"
    m["period_range"]   = "—"

    # Gauges
    m["supermarket_nps"]     = nearest_number(words, "Supermarket")
    m["colleague_happiness"] = nearest_number(words, "Colleague")
    m["home_delivery_nps"]   = nearest_number(words, "Home")
    m["cafe_nps"]            = nearest_number(words, "Cafe")
    m["click_collect_nps"]   = nearest_number(words, "Collect")
    m["customer_toilet_nps"] = nearest_number(words, "Toilet")

    # Front End Service
    m["sco_utilisation"]         = nearest_number(words, "Utilisation")
    m["efficiency"]              = nearest_number(words, "Efficiency")
    m["scan_rate"]               = nearest_number(words, "Scan")
    m["scan_vs_target"]          = "—"
    m["interventions"]           = nearest_number(words, "Interventions")
    m["interventions_vs_target"] = "—"
    m["mainbank_closed"]         = nearest_number(words, "Mainbank")
    m["mainbank_vs_target"]      = "—"

    # Online
    m["availability_pct"]    = nearest_number(words, "Availability")
    m["despatched_on_time"]  = nearest_number(words, "Despatched")
    m["delivered_on_time"]   = nearest_number(words, "Delivered")
    m["cc_avg_wait"]         = nearest_number(words, "wait")

    # Waste & Markdowns (via ROI later)
    for k in ["waste_total","markdowns_total","wm_total","wm_delta","wm_delta_pct"]:
        m[k] = "—"

    # Payroll
    m["payroll_outturn"]    = nearest_number(words, "Payroll")
    m["absence_outturn"]    = nearest_number(words, "Absence")
    m["productive_outturn"] = nearest_number(words, "Productive")
    m["holiday_outturn"]    = nearest_number(words, "Holiday")
    m["current_base_cost"]  = nearest_number(words, "Cost")

    # Shrink
    m["moa"]                  = nearest_number(words, "Adjustments")
    m["waste_validation"]     = nearest_number(words, "Validation")
    m["unrecorded_waste_pct"] = nearest_number(words, "Unrecorded")
    m["shrink_vs_budget_pct"] = nearest_number(words, "Budget")

    # Card Engagement & misc
    m["swipe_rate"]     = nearest_number(words, "Swipe")
    m["swipes_wow_pct"] = nearest_number(words, "WOW")
    m["new_customers"]  = nearest_number(words, "Customers")
    m["swipes_yoy_pct"] = nearest_number(words, "YOY")
    m["data_provided"]  = nearest_number(words, "Provided")
    m["trusted_data"]   = nearest_number(words, "Trusted")
    m["complaints_key"] = nearest_number(words, "Complaints")
    m["my_reports"]     = nearest_number(words, "Reports")
    m["weekly_activity"]= nearest_number(words, "Activity")
    # Sales totals left as ROI/DOM fallback
    m["sales_total"]     = "—"
    m["sales_lfl"]       = "—"
    m["sales_vs_target"] = "—"
    return m

# ──────────────────────────────────────────────────────────────────────────────
# ROI fallback
# ──────────────────────────────────────────────────────────────────────────────
def crop_roi(img: Image.Image, norm_roi: Tuple[float,float,float,float], base_w: int, base_h: int) -> Image.Image:
    W, H = img.size
    # scale normalized coordinates by base, then remap to current image size
    x, y, w, h = norm_roi
    # target pixels if base==current; normalize to current by W/(base_w), H/(base_h)
    px = int((x * base_w) * (W / base_w))
    py = int((y * base_h) * (H / base_h))
    pw = int((w * base_w) * (W / base_w))
    ph = int((h * base_h) * (H / base_h))
    box = (px, py, px + pw, py + ph)
    return img.crop(box)

def ocr_number_from_image(img: Image.Image, want_time=False, allow_percent=True) -> str:
    try:
        w,h = img.size
        if max(w,h) < 240:
            scale = 240 / max(w,h)
            img = img.resize((int(w*scale), int(h*scale)))
        img = img.convert("L")
        cfg = "--psm 7"
        txt = pytesseract.image_to_string(img, config=cfg)
        if want_time:
            m = re.search(r"\b\d{2}:\d{2}\b", txt)
            if m: return m.group(0)
        m = re.search(r"[£]?-?\d+(?:\.\d+)?[KMB]?", txt, re.I)
        if m and m.group(0): return m.group(0)
        if allow_percent:
            m = re.search(r"-?\d+(?:\.\d+)?%", txt)
            if m: return m.group(0)
        m = re.search(r"\b-?\d{1,3}\b", txt)
        if m: return m.group(0)
    except Exception:
        pass
    return "—"

def draw_roi_overlay(img: Image.Image, rois: Dict[str, Tuple[float,float,float,float]], base_w: int, base_h: int, ts: int):
    try:
        dbg = img.copy()
        draw = ImageDraw.Draw(dbg)
        W,H = dbg.size
        for key, norm in rois.items():
            x,y,w,h = norm
            px = int((x * base_w) * (W / base_w))
            py = int((y * base_h) * (H / base_h))
            pw = int((w * base_w) * (W / base_w))
            ph = int((h * base_h) * (H / base_h))
            box = (px, py, px+pw, py+ph)
            draw.rectangle(box, outline=(0,255,0), width=2)
            draw.text((px+3, py+3), key, fill=(0,255,0))
        out = SCREENS_DIR / f"{ts}_roi_overlay.png"
        dbg.save(out)
        log.info(f"ROI overlay saved → {out.name}")
    except Exception:
        pass

def fill_from_roi(metrics: Dict[str,str], img: Optional[Image.Image], rois: Dict[str, Tuple[float,float,float,float]], base_w: int, base_h: int):
    if img is None:
        return
    ts = int(time.time())
    changed = False
    for key, norm in rois.items():
        if metrics.get(key, "—") != "—":
            continue
        want_time = (key == "cc_avg_wait")
        allow_percent = not key.endswith("_nps")
        crop = crop_roi(img, norm, base_w, base_h)
        # save small crops for debug
        try:
            crop_path = SCREENS_DIR / f"{ts}_{key}_crop.png"
            crop.save(crop_path)
        except Exception:
            pass
        val = ocr_number_from_image(crop, want_time=want_time, allow_percent=allow_percent)
        if val and val != "—":
            metrics[key] = val
            changed = True
    draw_roi_overlay(img, rois, base_w, base_h, ts)
    if changed:
        log.info("Filled some missing values from ROI OCR.")

# ──────────────────────────────────────────────────────────────────────────────
# Build + send Chat card
# ──────────────────────────────────────────────────────────────────────────────
def kv(label: str, val: str) -> dict:
    return {"decoratedText": {"topLabel": label, "text": (val or "—")}}

def title_widget(text: str) -> dict:
    return {"textParagraph": {"text": f"<b>{text}</b>"}}

def build_chat_card(metrics: Dict[str, str]) -> dict:
    header = {
        "title": "📊 Retail Daily Summary (OCR)",
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

def send_daily_card(metrics: Dict[str, str]) -> bool:
    if not MAIN_WEBHOOK or "chat.googleapis.com" not in MAIN_WEBHOOK:
        log.error("MAIN_WEBHOOK/DAILY_WEBHOOK missing or invalid — cannot send daily report.")
        return False
    payload = build_chat_card(metrics)
    return _post_with_backoff(MAIN_WEBHOOK, payload)

# ──────────────────────────────────────────────────────────────────────────────
# CSV logging
# ──────────────────────────────────────────────────────────────────────────────
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

def write_csv_row(metrics: Dict[str,str]):
    write_header = not DAILY_LOG_CSV.exists() or DAILY_LOG_CSV.stat().st_size == 0
    with open(DAILY_LOG_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(CSV_HEADERS)
        w.writerow([metrics.get(h, "—") for h in CSV_HEADERS])
    log.info(f"Appended daily metrics row to {DAILY_LOG_CSV.name}")

# ──────────────────────────────────────────────────────────────────────────────
# Run
# ──────────────────────────────────────────────────────────────────────────────
def run_daily_scrape():
    if not AUTH_STATE_PATH.exists():
        alert(["⚠️ Daily dashboard scrape needs login. Run `python scrape.py now` once to save auth_state.json."])
        log.error("auth_state.json not found.")
        return

    rois, base_w, base_h = load_roi_map()

    with sync_playwright() as p:
        browser = context = page = None
        img = None
        words: List[dict] = []
        try:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                storage_state=str(AUTH_STATE_PATH),
                viewport={"width": VIEWPORT["width"], "height": VIEWPORT["height"]},
                device_scale_factor=VIEWPORT["device_scale_factor"],
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            )
            page = context.new_page()
            if not open_and_prepare(page):
                alert(["⚠️ Daily scrape blocked by login or load failure — please re-login."])
                return

            # Screenshot viewport (matches ROI base)
            img = screenshot_viewport(page)

            # OCR words + label proximity
            if img:
                words = ocr_words(img)
            metrics = build_metrics_from_ocr_labels(words)

            # Context from DOM text (for long strings)
            try:
                body = page.inner_text("body")
            except Exception:
                body = ""
            m = re.search(r"\b(\d{1,2}\s+[A-Za-z]{3}\s+\d{4},\s*\d{2}:\d{2}:\d{2})\b", body)
            if m: metrics["page_timestamp"] = m.group(1)
            m = re.search(r"The data on this report is from:\s*([^\n]+)", body)
            metrics["period_range"] = m.group(1).strip() if m else "—"
            m = re.search(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}).*?\|\s*([^\|]+?)\s*\|\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})",
                          body, flags=re.S)
            metrics["store_line"] = m.group(0).strip() if m else ""

            # ROI fallback for anything still missing
            fill_from_roi(metrics, img, rois, base_w, base_h)

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
    log.info("Daily card send → %s", "OK" if ok else "FAIL")
    write_csv_row(metrics)

if __name__ == "__main__":
    run_daily_scrape()
