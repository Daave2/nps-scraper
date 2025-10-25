#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary (layout-by-lines + GEMINI VISION) → Google Chat

Key points in this build:
- Gemini Vision Integration: Uses Gemini Pro Vision for all difficult, visual-based metrics (NPS, Payroll, Shrink circles).
- Robustness: Eliminates brittle ROI coordinates and traditional OCR misreads.
- Efficiency: Retains fast, reliable line parsing for easy text-based metrics.
- SCOPE CORRECTED: All functions are now correctly defined in the proper scope for the entry point.
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

# --- GEMINI INTEGRATION IMPORTS ---
try:
    from google import genai
    from google.genai import types
    from PIL import Image
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    class Image: pass # Placeholder for type hints

# --- Placeholder for compatibility/simplicity of the final script structure ---
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

# --- METRICS REQUIRING GEMINI (The previously failing list) ---
GEMINI_METRICS = [
    "supermarket_nps", "colleague_happiness", "home_delivery_nps", "cafe_nps", 
    "click_collect_nps", "customer_toilet_nps", "payroll_outturn", "absence_outturn", 
    "productive_outturn", "holiday_outturn", "current_base_cost", "moa", 
    "waste_validation", "unrecorded_waste_pct", "shrink_vs_budget_pct", "availability_pct",
    "cc_avg_wait"
]

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

# Safely retrieve GEMINI_API_KEY
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", config["DEFAULT"].get("GEMINI_API_KEY"))

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
# Card + CSV
# ──────────────────────────────────────────────────────────────────────────────
def kv(label: str, val: str) -> dict:
    return {"decoratedText": {"topLabel": label, "text": (val or "—")}}

def title_widget(text: str) -> dict:
    return {"textParagraph": {"text": f"<b>{text}</b>"}}

def build_chat_card(metrics: Dict[str, str]) -> dict:
    header = {
        "title": "📊 Retail Daily Summary (Layout+GEMINI VISION)",
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
# Text parsing (layout-by-lines) — Deterministic rules (SCOPED)
# ──────────────────────────────────────────────────────────────────────────────
NUM_ANY_RE   = re.compile(r"[£]?-?\d+(?:\.\d+)?(?:[KMB]|%)?", re.I)
NUM_INT_RE   = re.compile(r"\b-?\d+\b")
NUM_PCT_RE   = re.compile(r"-?\d+(?:\.\d+)?%")
# allow both "£-8K" and "-£8K"
NUM_MONEY_RE = re.compile(r"(?:-?\s*£|£\s*-?)\s*\d+(?:\.\d+)?[KMB]?", re.I)
TIME_RE      = re.compile(r"\b\d{1,2}:\d{2}\b")

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

def _contains_num_of_type(s: str, kind: str) -> Optional[str]:
    # --- TARGETED FIXES START: Enforce % if number found ---
    if kind == "percent_format":
        m = NUM_PCT_RE.search(s)
        if m: return m.group(0)
        # If it's a number that should be a percentage but lacks the %, add it if it's the only numeric content
        m_num = NUM_ANY_RE.search(s)
        if m_num and (m_num.group(0) == s.strip() or s.strip().endswith(m_num.group(0))):
            # Exclude currency/K/M/B symbols from getting a % added
            if not re.search(r"[£KMB]", m_num.group(0), re.I):
                return m_num.group(0) + "%"
        return None
    # --- TARGETED FIXES END ---
    
    if kind == "time":
        m = TIME_RE.search(s); return m.group(0) if m else None
    if kind == "percent":
        m = NUM_PCT_RE.search(s); return m.group(0) if m else None
    if kind == "integer":
        m = NUM_INT_RE.search(s); return m.group(0) if m else None
    if kind == "money":
        m = NUM_MONEY_RE.search(s)
        if m: return re.sub(r"\s+", "", m.group(0))  # tidy spaces
        m2 = re.search(r"-?\d+(?:\.\d+)?[KMB]?", s, re.I); return m2.group(0) if m2 else None
    m = NUM_ANY_RE.search(s); return m.group(0) if m else None

def _idx(lines: List[str], needle: str, start=0, end=None) -> int:
    end = len(lines) if end is None else end
    nl = needle.lower()
    for i in range(start, end):
        if nl in lines[i].lower():
            return i
    return -1

def _scope_end(lines: List[str], starts: List[int], fallback_end: int) -> int:
    nxt = [i for i in starts if i >= 0]
    return min(nxt) if nxt else fallback_end

def section_bounds(lines: List[str], start_anchor: str, candidate_next: List[str]) -> Tuple[int,int]:
    s = _idx(lines, start_anchor)
    if s < 0: return -1, -1
    next_idxs = [ _idx(lines, a, s+1) for a in candidate_next ]
    e = _scope_end(lines, next_idxs, len(lines))
    return s, e

def value_near_scoped(lines: List[str], label: str, kind: str, scope: Tuple[int,int], *, near_before=6, near_after=6, prefer_before_first=0) -> str:
    s, e = scope
    if s < 0: return "—"
    li = _idx(lines, label, s, e)
    if li < 0: return "—"
    
    # Check for percentage format first if requested
    target_kind = "percent" if "percent" in kind else kind

    # bias: prefer hits just above the label (Availability 84%)
    if prefer_before_first > 0:
        for i in range(max(s, li - prefer_before_first), li):
            v = _contains_num_of_type(lines[i], target_kind if target_kind != "percent" else "percent_format")
            if v: return v
    # after the label
    for i in range(li+1, min(e, li+1+near_after)):
        v = _contains_num_of_type(lines[i], target_kind if target_kind != "percent" else "percent_format")
        if v: return v
    # before the label
    for i in range(max(s, li - near_before), li):
        v = _contains_num_of_type(lines[i], target_kind if target_kind != "percent" else "percent_format")
        if v: return v
    return "—"

def sales_three_after_total(lines: List[str]) -> Optional[Tuple[str,str,str]]:
    """‘Sales’ → first ‘Total’ after → next 3 numeric tokens across following lines."""
    i_sales = _idx(lines, "Sales", start=0)
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

# NEW: safe "coalesce" to handle "—" being truthy
def coalesce(*vals: str) -> str:
    """Return the first value that isn't empty and isn't '—'."""
    for v in vals:
        if v and v != "—":
            return v
    return "—"

# FES helpers (scoped KPI value + correctly paired vs Target)
def _fes_value(lines: List[str], label: str, num_type: str, scope: Tuple[int,int]) -> str:
    s, e = scope
    if s < 0: return "—"
    li = _idx(lines, label, s, e)
    if li < 0: return "—"
    kpi_labels = ["Sco Utilisation","SCO Utilisation","Efficiency","Scan Rate","Interventions","Mainbank Closed"]
    next_labels = [ _idx(lines, l, li+1, e) for l in kpi_labels ]
    bound = _scope_end(lines, next_labels, e)
    vsi = _idx(lines, "vs Target", li+1, e)
    limit = min([x for x in [bound, vsi if vsi >= 0 else e] if x > li], default=e)
    # typed search right after label; if nothing, peek one line above (some tiles render above)
    # after
    for i in range(li+1, min(limit, li+1+8)):
        v = _contains_num_of_type(lines[i], num_type)
        if v: return v
    # one line above fallback
    if li-1 >= s:
        v = _contains_num_of_type(lines[li-1], num_type)
        if v: return v
    # outward bounded
    for i in range(li+1, limit):
        v = _contains_num_of_type(lines[i], num_type)
        if v: return v
    for i in range(max(s, li-3), li):
        v = _contains_num_of_type(lines[i], num_type)
        if v: return v
    return "—"

def _fes_vs(lines: List[str], label: str, scope: Tuple[int,int]) -> str:
    s, e = scope
    if s < 0: return "—"
    li = _idx(lines, label, s, e)
    if li < 0: return "—"
    vsi = _idx(lines, "vs Target", li+1, e)
    if vsi < 0: return "—"
    kpi_labels = ["Sco Utilisation","SCO Utilisation","Efficiency","Scan Rate","Interventions","Mainbank Closed"]
    next_labels = [ _idx(lines, l, li+1, min(e, li+40)) for l in kpi_labels ]
    bound = _scope_end(lines, next_labels, min(e, li+40))
    if vsi >= bound:
        return "—"
    # limited window after vs Target
    for i in range(vsi+1, min(vsi+3, bound)):
        v = _contains_num_of_type(lines[i], "any")
        if v: return v
    return "—"

# ──────────────────────────────────────────────────────────────────────────────
# Gemini Vision Extraction (For hard-to-read metrics)
# ──────────────────────────────────────────────────────────────────────────────
def extract_gemini_metrics(metrics: Dict[str, str], image_path: Path) -> Dict[str, str]:
    if not GEMINI_AVAILABLE or not GEMINI_API_KEY:
        log.warning("Gemini API not available or key missing. Skipping AI extraction.")
        return metrics

    # 1. Determine which fields are missing or require AI validation
    fields_to_query = [k for k in GEMINI_METRICS if metrics.get(k) in [None, "—"]]

    # Also add key complaints to the AI query for robustness
    if metrics.get("complaints_key") in [None, "—", "0"]:
        fields_to_query.append("key_customer_complaints")
    
    # Add other key metrics to query Gemini for validation (can remove later if confident in line parsing)
    fields_to_query.extend(["swipe_rate", "swipes_wow_pct"])
    fields_to_query = list(set(fields_to_query)) # Remove duplicates

    if not fields_to_query:
        log.info("All high-value metrics were successfully line-parsed. Skipping Gemini call.")
        return metrics

    log.info(f"Querying Gemini for {len(fields_to_query)} fields: {', '.join(fields_to_query)}")
    
    # Clean up keys for the prompt (e.g., 'payroll_outturn' -> 'Payroll Outturn')
    # and map back to Python keys later.
    prompt_map = {k.replace('_', ' ').title(): k for k in fields_to_query}
    
    system_instruction = (
        "You are a hyper-accurate retail dashboard data extraction engine. Your task is to extract "
        "the exact numeric or short text values for the requested metrics from the provided image. "
        "Return the output as a single, valid JSON object, using the requested keys exactly as provided. "
        "For percentages, include the '%' symbol."
    )
    
    user_prompt = (
        f"Analyze the image and return the exact values for the following metrics as a single JSON object. "
        f"For any NPS value, return the number. For Payroll/Finance, include K or M/B if present, and the negative sign if present. "
        f"Metrics to extract: {list(prompt_map.keys())}"
    )

    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        
        # Load the image
        img = Image.open(image_path)
        
        # Configure model and send prompt
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[img, user_prompt],
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                response_mime_type="application/json",
                response_schema=types.Schema(
                    type=types.Type.OBJECT,
                    properties={v: types.Schema(type=types.Type.STRING) for v in prompt_map.keys()}
                )
            )
        )
        
        # Parse the AI's JSON response
        ai_data = json.loads(response.text)
        
        updated_metrics = metrics.copy()
        for ai_key, ai_val in ai_data.items():
            python_key = prompt_map.get(ai_key)
            if python_key and ai_val is not None:
                # The AI's result is the definitive value
                updated_metrics[python_key] = str(ai_val).strip()
                log.info(f"Gemini Success: {python_key} -> {updated_metrics[python_key]}")

        return updated_metrics

    except Exception as e:
        log.error(f"Gemini API Error: Failed to extract metrics: {e}")
        return metrics

# ──────────────────────────────────────────────────────────────────────────────
# Main Parser
# ──────────────────────────────────────────────────────────────────────────────

def parse_from_lines(lines: List[str]) -> Dict[str, str]:
    m: Dict[str, str] = {}

    # Context
    joined = "\n".join(lines)
    z = EMAILLOC.search(joined); m["store_line"]    = z.group(0).strip() if z else ""
    y = PERIOD_RE.search(joined); m["period_range"] = y.group(1).strip() if y else "—"
    x = STAMP_RE.search(joined);  m["page_timestamp"]= x.group(1) if x else "—"

    # ── Section scopes ────────────────────────────────────────────────────────
    FES_SCOPE     = section_bounds(lines, "Front End Service",
                                   ["More Card Engagement","Card Engagement","Production Planning","Online","Waste & Markdowns","Shrink","Payroll","Privacy"])
    ONLINE_SCOPE  = section_bounds(lines, "Online",
                                   ["Front End Service","More Card Engagement","Card Engagement","Waste & Markdowns","Shrink","Payroll","Privacy"])
    PAYROLL_SCOPE = section_bounds(lines, "Payroll",
                                   ["Online","Front End Service","More Card Engagement","Card Engagement","Waste & Markdowns","Shrink","Privacy"])
    SHRINK_SCOPE  = section_bounds(lines, "Shrink",
                                   ["Waste & Markdowns","My Reports","Payroll","Online","Front End Service","More Card Engagement","Card Engagement","Privacy"])
    CARD_SCOPE    = section_bounds(lines, "More Card Engagement",
                                   ["Payroll","Online","Front End Service","Waste & Markdowns","Shrink","Privacy"])
    PP_SCOPE      = section_bounds(lines, "Production Planning",
                                   ["More Card Engagement","Card Engagement","Payroll","Shrink","Privacy"])
    COMPLAINTS_SCOPE = section_bounds(lines, "Customer Complaints",
                                   ["Production Planning","More Card Engagement","Card Engagement","Payroll","Shrink","Privacy"])
    CLEAN_ROTATE_SCOPE = section_bounds(lines, "Clean & Rotate",
                                   ["My Reports","More Card Engagement","Card Engagement","Payroll","Shrink", "Privacy"])

    # ── Sales (triple after 'Total') ──────────────────────────────────────────
    res = sales_three_after_total(lines)
    if res:
        m["sales_total"], m["sales_lfl"], m["sales_vs_target"] = res
    else:
        m["sales_total"] = m["sales_lfl"] = m["sales_vs_target"] = "—"

    # ── Waste & Markdowns (robust Total row regex) ───────────────────────────
    pivot = _idx(lines, "(+/-)%")
    if pivot < 0:
        pivot = _idx(lines, "Waste & Markdowns")
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

    # ── Front End Service (Line Parse) ───────────────────────────────────────────
    m["sco_utilisation"]         = coalesce(_fes_value(lines, "Sco Utilisation", "percent", FES_SCOPE), _fes_value(lines, "SCO Utilisation", "percent", FES_SCOPE))
    m["efficiency"]              = _fes_value(lines, "Efficiency",      "percent", FES_SCOPE)
    m["scan_rate"]               = _fes_value(lines, "Scan Rate",       "integer", FES_SCOPE)
    m["interventions"]           = _fes_value(lines, "Interventions",   "integer", FES_SCOPE)
    m["mainbank_closed"]         = _fes_value(lines, "Mainbank Closed", "integer", FES_SCOPE)
    m["scan_vs_target"]          = _fes_vs(lines, "Scan Rate",       FES_SCOPE)
    m["interventions_vs_target"] = _fes_vs(lines, "Interventions",   FES_SCOPE)
    m["mainbank_vs_target"]      = _fes_vs(lines, "Mainbank Closed", FES_SCOPE)
    
    # ── Other easy/contextual metrics (Line Parse) ─────────────────────────────────────────
    m["cc_avg_wait"]        = value_near_scoped(lines, "average wait",       "time",    ONLINE_SCOPE, near_before=15, near_after=20)
    m["new_customers"]      = value_near_scoped(lines, "New Customers",      "integer", CARD_SCOPE, near_before=6, near_after=10)
    m["swipe_rate"]         = value_near_scoped(lines, "Swipe Rate",         "percent_format", CARD_SCOPE, near_before=4, near_after=8)
    m["swipes_wow_pct"]     = value_near_scoped(lines, "Swipes WOW",         "percent_format", CARD_SCOPE, near_before=4, near_after=8)
    m["data_provided"] = value_near_scoped(lines, "Data Provided", "percent_format", PP_SCOPE, near_before=6, near_after=8)
    m["trusted_data"]  = value_near_scoped(lines, "Trusted Data",  "percent_format", PP_SCOPE, near_before=6, near_after=8)
    m["my_reports"]    = value_near_scoped(lines, "My Reports", "integer", section_bounds(lines, "My Reports", ["Cafe NPS","Privacy","Payroll","Shrink","Waste & Markdowns"]), near_before=6, near_after=10)
    m["complaints_key"] = value_near_scoped(lines, "Key Customer Complaints", "integer", COMPLAINTS_SCOPE, near_before=10, near_after=12)
    m["weekly_activity"] = "No data" if "No data" in "\n".join(lines[CLEAN_ROTATE_SCOPE[0]:CLEAN_ROTATE_SCOPE[1]]) else "—"


    # ── Placeholder for Gemini Metrics (will be overwritten by AI call) ────────────────
    for k in GEMINI_METRICS:
        # Check if the line parser already found the value. If so, apply formatting and keep it.
        if k in m and m[k] not in [None, "—"]:
            if "pct" in k or k in ["availability_pct", "waste_validation"]:
                if "%" not in m[k] and re.match(r"^-?\d+(\.\d+)?$", m[k]):
                    m[k] += "%"
            continue 
        m[k] = "—" # Mark for Gemini if not found
        
    return m

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def run_daily_scrape():
    if not AUTH_STATE.exists():
        alert(["⚠️ Daily dashboard scrape needs login. Run `python scrape.py now` once to save auth_state.json."])
        log.error("auth_state.json not found.")
        return
    
    if not GEMINI_AVAILABLE:
        alert(["⚠️ Gemini library (google-genai) is not installed. Please install it to use the AI features."])
    
    if not GEMINI_API_KEY:
        alert(["⚠️ Gemini API Key is missing. Check your GitHub Secrets/Environment variables."])


    with sync_playwright() as p:
        browser = context = page = None
        metrics: Dict[str,str] = {}
        screenshot_path: Optional[Path] = None
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

            # Screenshot for GEMINI and debugging
            img_bytes = page.screenshot(full_page=True, type="png")
            ts = int(time.time())
            SCREENS_DIR.mkdir(parents=True, exist_ok=True)
            screenshot_path = SCREENS_DIR / f"{ts}_fullpage.png"
            save_bytes(screenshot_path, img_bytes)
            
            # BODY TEXT → numbered lines → layout parser (Primary extraction)
            body_text = get_body_text(page)
            lines = dump_numbered_lines(body_text)
            metrics = parse_from_lines(lines)

            # 💥 FALLBACK: Use Gemini for all metrics marked as '—' or requiring visual confirmation
            if GEMINI_AVAILABLE and GEMINI_API_KEY and screenshot_path.exists():
                 metrics = extract_gemini_metrics(metrics, screenshot_path)
            else:
                 log.warning("Skipping Gemini Extraction. Results may be incomplete or incorrect due to missing dependencies/key.")


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
