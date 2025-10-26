#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary (layout-by-lines + GEMINI VISION) → Google Chat

Key points in this build:
- Gemini Vision Integration: Uses Gemini Pro Vision for all difficult, visual-based metrics.
- NEW: Target-based Chat Card formatting using **<font color='...'>** for visual distinction.
- UPDATE: Increased waiting time for dashboard stability before capture.
- NEW: Blank/missing metrics are now excluded from the Chat Card.
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

GEMINI_METRICS = [
    "supermarket_nps", "colleague_happiness", "home_delivery_nps", "cafe_nps", 
    "click_collect_nps", "customer_toilet_nps", "payroll_outturn", "absence_outturn", 
    "productive_outturn", "holiday_outturn", "current_base_cost", "moa", 
    "waste_validation", "unrecorded_waste_pct", "shrink_vs_budget_pct", "availability_pct",
    "cc_avg_wait","key Complaints"
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

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", config["DEFAULT"].get("GEMINI_API_KEY"))

MAIN_WEBHOOK  = config["DEFAULT"].get("DAILY_WEBHOOK") or config["DEFAULT"].get("MAIN_WEBHOOK", os.getenv("MAIN_WEBHOOK", ""))
ALERT_WEBHOOK = config["DEFAULT"].get("ALERT_WEBHOOK",  os.getenv("ALERT_WEBHOOK", ""))
CI_RUN_URL    = os.getenv("CI_RUN_URL", "")

# ──────────────────────────────────────────────────────────────────────────────
# Targets / Formatting (FIXED: Status mapping and lookup logic)
# ──────────────────────────────────────────────────────────────────────────────
# Target values and comparison rules for Google Chat Card formatting.
# Rules: 'A>X G, A<Y R', 'A>X R', 'A<X R', 'A<X G, A>Y R', 
# 'A>X O, A>Y R, A>Z BR' (O=Orange, R=Red, G=Green, BR=Bold Red, M=Minutes)
METRIC_TARGETS = {
    # Sales
    "sales_lfl":           ("0",     "A>2 G, A<-2 R"),
    "sales_vs_target":     ("0",     "A>2K G, A<-2K R"),
    # NPS
    "supermarket_nps":     ("65",    "A>65 G, A<50 R"),
    "colleague_happiness": ("40",    "A>40 G, A<0 R"),
    "home_delivery_nps":   ("75",    "A>75 G, A<65 R"),
    "cafe_nps":            ("65",    "A>65 G, A<50 R"), 
    "click_collect_nps":   ("40",    "A>40 G, A<30 R"),
    "customer_toilet_nps": ("40",    "A>40 G, A<30 R"), 
    # FES
    "sco_utilisation":     ("67%",   "A>67% G, A<65% R"),
    "efficiency":          ("100%",  "A>99% G, A<90% R"),
    "scan_rate":           ("21.3",  "A>21.3 G, A<20.1 R"),
    "interventions":       ("20",    "A<20.1 G, A>25 R"),
    "mainbank_closed":     ("0",     "A<1 G, A>2 R"),
    # Online
    "availability_pct":    ("96%",   "A>96% G, A<92% R"),
    "cc_avg_wait":         ("4:30",  "A<4.5M G, A>5M R"), # M = Minutes
    # Waste/Shrink 
    "shrink_vs_budget_pct":("0%",    "A>6% R, A<-0% G"),
    # Payroll (Outturn metrics)
    "payroll_outturn":     ("0",     "A<0 R"),
    "absence_outturn":     ("0",     "A<0 R"),
    "productive_outturn":  ("0",     "A<0 R"),
    "holiday_outturn":     ("0",     "A<0 R"),
    # Card
    "swipe_rate":          ("75%",   "A<65% R, A>80% G"),
    # Misc
    "complaints_key":      ("0",     "A>0 O, A>1 R, A>2 BR"),
    "trusted_data":        ("49%",   "A>49% G"),
}

# --- Color Definitions for Unofficial <font> Tag ---
COLOR_RED = "#FF0000"   # Critical/Bad 
COLOR_AMBER = "#FFA500" # Warning/Orange

# Mapping status to HTML tags (using font color for poor performance, plain for good)
STATUS_FORMAT = {
    "GREEN":  ("", ""),                                 # Good/Green is plain text
    "RED":    (f"<font color='{COLOR_RED}'>", "</font>"),      # Deviation is red
    "ORANGE": (f"<font color='{COLOR_AMBER}'>", "</font>"),    # Warning is amber
    "BOLD_RED": (f"<font color='{COLOR_RED}'><b>", "</b></font>"), # Critical is bold and red
    "NONE":   ("", ""),                                 # No rule/data is plain text
}

# Mapping single-letter rule codes to full status keys for lookup
STATUS_CODE_MAP = {
    "G": "GREEN", 
    "R": "RED", 
    "O": "ORANGE", 
    "BR": "BOLD_RED"
}

def _clean_numeric_value(val: str, is_time_min: bool = False) -> Optional[float]:
    """Converts a metric string (e.g., '3.43%', '£-8K', '4:30') to a comparable float."""
    if not val or val == "—":
        return None
    
    val = str(val).strip().replace(',', '')
    
    # Time (M:SS to total minutes for CC Avg Wait)
    if is_time_min:
        parts = val.split(':')
        if len(parts) == 2:
            try:
                minutes = float(parts[0])
                seconds = float(parts[1])
                return minutes + (seconds / 60.0)
            except ValueError:
                return None
        try:
            return float(val)
        except ValueError:
            return None

    # General numeric cleanup
    val = re.sub(r'[£$€]', '', val).strip()
    
    multiplier = 1.0
    val_clean = val.rstrip('%')
    
    if val.endswith('K'):
        multiplier = 1000.0
        val_clean = val_clean.rstrip('K')
    elif val.endswith('M'):
        multiplier = 1_000_000.0
        val_clean = val_clean.rstrip('M')
    elif val.endswith('B'):
        multiplier = 1_000_000_000.0
        val_clean = val_clean.rstrip('B')

    try:
        return float(val_clean) * multiplier
    except ValueError:
        return None

def get_status_formatting(key: str, value: str) -> Tuple[str, str]:
    """
    Determines the status (color/bolding) for a metric based on its value and rules.
    Returns: (prefix_html, suffix_html)
    """
    if key not in METRIC_TARGETS or value in [None, "—"]:
        return STATUS_FORMAT["NONE"] # Return tuple (prefix, suffix) directly

    _, rule_str = METRIC_TARGETS[key]
    is_time = "M" in rule_str # Flag for minute conversion

    # Clean the actual value
    comp_value = _clean_numeric_value(value, is_time_min=is_time)
    if comp_value is None:
        return STATUS_FORMAT["NONE"]

    # Parse the rule string (e.g., 'A>65 G, A<50 R')
    rules = [r.strip() for r in rule_str.split(',')]
    
    # Function to check a single rule segment
    def check_rule(rule_segment, value, is_time):
        # Matches patterns like 'A>2 BR', 'A<-2K R', 'A<4.5M G'
        m = re.match(r"A([<>])(-?[\d.]+)([KMB%]?|[M])?\s*(R|G|O|BR)", rule_segment, re.I)
        if m:
            op, str_val, unit, status = m.groups()
            is_min_target = (unit == 'M')
            comp_target = _clean_numeric_value(str_val + (unit if unit != 'M' else ''), is_time_min=is_min_target)
            
            if comp_target is not None:
                is_match = False
                if op == '>' and value > comp_target:
                    is_match = True
                elif op == '<' and value < comp_target:
                    is_match = True
                
                if is_match:
                    return status.upper() # Returns single letter code (G, R, O, BR)
        return None

    # Process rules in a priority order: BOLD_RED > RED > ORANGE > GREEN
    priority_statuses = ["BR", "R", "O", "G"]
    
    for status_code_letter in priority_statuses:
        for rule in rules:
            status_letter = check_rule(rule, comp_value, is_time)
            if status_letter == status_code_letter:
                # Look up the full status name from the code
                full_status = STATUS_CODE_MAP.get(status_letter)
                if full_status:
                    # Return the formatting for the highest priority status matched
                    return STATUS_FORMAT[full_status]

    # No rule match
    return STATUS_FORMAT["NONE"]

def format_metric_value(key: str, value: str) -> str:
    """Applies status formatting (color/bold) to the metric value string."""
    prefix, suffix = get_status_formatting(key, value)
    return f"{prefix}{value}{suffix}"


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
# Card + CSV (UPDATED: Added conditional metric function and logic in build_chat_card)
# ──────────────────────────────────────────────────────────────────────────────
def kv(label: str, val: str, key: Optional[str] = None) -> dict:
    """Creates a decoratedText widget, optionally applying target-based formatting."""
    # Apply color/bolding if the metric key is provided and a rule is matched
    formatted_val = format_metric_value(key, val) if key else (val or "—")
    return {"decoratedText": {"topLabel": label, "text": formatted_val}}

def title_widget(text: str) -> dict:
    return {"textParagraph": {"text": f"<b>{text}</b>"}}

def _create_metric_widget(metrics: Dict[str, str], label: str, key: str, custom_val: Optional[str] = None) -> Optional[dict]:
    """
    Creates a decoratedText widget if the metric's value is not blank.
    
    :param metrics: The dictionary of all metrics.
    :param label: The label for the widget.
    :param key: The key in the metrics dict (and in METRIC_TARGETS).
    :param custom_val: Optional pre-formatted value (used for Scan Rate/Interventions with vs Target).
    :return: The widget dictionary or None if the value is blank/missing.
    """
    val = metrics.get(key)
    
    # Check if the value is effectively blank
    is_blank = (val is None or val.strip() == "" or val.strip() == "—")
    
    # Special handling for FES metrics when they are compounded with "vs Target"
    if custom_val:
        # Check the base metric's value for blankness, but use the custom_val if not blank.
        # This is a good way to ensure a complex metric isn't shown if the core number is missing.
        if is_blank:
            return None
        return {"decoratedText": {"topLabel": label, "text": custom_val}}
        
    if is_blank:
        return None
    
    # Check for metrics that often return 'NPS' as the value (e.g., when the number itself is missing)
    if val.upper() == "NPS":
        return None

    # Create the standard widget using the kv helper
    return kv(label, val, key=key)

def build_chat_card(metrics: Dict[str, str]) -> dict:
    header = {
        "title": "📊 Retail Daily Summary",
        "subtitle": (metrics.get("store_line") or "").replace("\n", "  "),
    }
    
    # Define the structure and metric keys for each section
    section_data = [
        {"title": None, "metrics": [
            ("Report Time", "page_timestamp"),
            ("Period", "period_range")
        ]},
        {"title": "Sales & NPS", "metrics": [
            ("Sales Total", "sales_total"),
            ("LFL", "sales_lfl"),
            ("vs Target", "sales_vs_target"),
            ("Key Complaints", "complaints_key"),
            ("Supermarket NPS", "supermarket_nps"),
            ("Colleague Happiness", "colleague_happiness"),
            # Removed Home Delivery NPS based on recent card layout tweak
            ("Cafe NPS", "cafe_nps"),
            ("Click & Collect NPS", "click_collect_nps"),
            ("Customer Toilet NPS", "customer_toilet_nps"),
        ]},
        {"title": "Front End", "metrics": [
            ("SCO Utilisation", "sco_utilisation"),
            ("Efficiency", "efficiency"),
            # These FES metrics require custom assembly using the vs_target field
            ("Scan Rate", "scan_rate", f"{format_metric_value('scan_rate', metrics.get('scan_rate','—'))} (vs {metrics.get('scan_vs_target','—')})"),
            ("Interventions", "interventions", f"{format_metric_value('interventions', metrics.get('interventions','—'))} (vs {metrics.get('interventions_vs_target','—')})"),
            ("Mainbank Closed", "mainbank_closed", f"{format_metric_value('mainbank_closed', metrics.get('mainbank_closed','—'))} (vs {metrics.get('mainbank_vs_target','—')})"),
            ("More card Swipe Rate", "swipe_rate"),
            ("More card Swipes WOW %", "swipes_wow_pct"),
        ]},
        {"title": "Online", "metrics": [
            ("C&C Availability", "availability_pct"),
            ("Click & Collect Wait", "cc_avg_wait"),
        ]},
        {"title": "Waste & Markdowns (Total)", "metrics": [
            ("Waste", "waste_total"),
            ("Markdowns", "markdowns_total"),
            ("Total", "wm_total"),
            ("+/−", "wm_delta"),
            # Removed wm_delta_pct and replaced with Clean and rotate (weekly_activity is used for this)
            ("Clean and rotate", "weekly_activity"),
        ]},
        {"title": "Payroll", "metrics": [
            ("Payroll Outturn", "payroll_outturn"),
            ("Absence Outturn", "absence_outturn"),
            ("Productive Outturn", "productive_outturn"),
            ("Holiday Outturn", "holiday_outturn"),
            # Removed Current Base Cost
        ]},
        {"title": "Shrink", "metrics": [
            ("Morrisons Order Adjustments", "moa"),
            ("Waste Validation", "waste_validation"),
            ("Unrecorded Waste %", "unrecorded_waste_pct"),
            ("Shrink vs Budget %", "shrink_vs_budget_pct"),
        ]},
        {"title": "Production Plans", "metrics": [
            ("Data Provided", "data_provided"),
            ("Trusted Data", "trusted_data"),
            # Retaining weekly_activity here as a temporary fix from previous logic
            ("Weekly Activity %", "weekly_activity"),
        ]},
    ]

    final_sections = []
    
    # --- Process Sections ---
    for section in section_data:
        widgets = []
        
        # Build metric widgets, only adding if they are not blank
        for metric_data in section["metrics"]:
            label, key = metric_data[0], metric_data[1]
            custom_val = metric_data[2] if len(metric_data) > 2 else None
            
            widget = _create_metric_widget(metrics, label, key, custom_val)
            if widget:
                widgets.append(widget)
        
        # Only create the section if there are metrics/widgets to display
        if widgets:
            section_dict = {"widgets": []}
            
            # Add title widget if specified
            if section["title"]:
                section_dict["widgets"].append(title_widget(section["title"]))
            
            # Add all non-title widgets
            section_dict["widgets"].extend(widgets)
            final_sections.append(section_dict)


    return {"cardsV2": [{"cardId": f"daily_{int(time.time())}", "card": {"header": header, "sections": final_sections}}]}

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

    # INCREASED WAIT: Gave 12s, now giving 20s for general content load
    log.info("Waiting 20s for dynamic content…")
    page.wait_for_timeout(20_000)

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
# Text parsing (Unmodified)
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
# Gemini Vision Extraction (Unmodified)
# ──────────────────────────────────────────────────────────────────────────────
def extract_gemini_metrics(metrics: Dict[str, str], image_path: Path) -> Dict[str, str]:
    if not GEMINI_AVAILABLE or not GEMINI_API_KEY:
        log.warning("Gemini API not available or key missing. Skipping AI extraction.")
        return metrics

    # 1. Determine which fields are missing or require AI validation
    # Mark for AI only if the line parser failed (or if it's the unreliable Key Complaints)
    fields_to_query = [k for k in GEMINI_METRICS if metrics.get(k) in [None, "—"]]

    # Key Complaints is manually added to the query regardless of line parse outcome, 
    # as the line parser is known to be unreliable for this field ("0" vs "2")
    fields_to_query.append("key_customer_complaints")
    
    # Add other key metrics for validation (Line parsing can be slow/error prone on long strings)
    fields_to_query.extend(["swipe_rate", "swipes_wow_pct"])
    fields_to_query = list(set(fields_to_query)) # Remove duplicates

    if not fields_to_query:
        log.info("All high-value metrics were successfully line-parsed. Skipping Gemini call.")
        return metrics

    log.info(f"Querying Gemini for {len(fields_to_query)} fields: {', '.join(fields_to_query)}")
    
    # Clean up keys for the prompt (e.g., 'payroll_outturn' -> 'Payroll Outturn')
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
                # The AI's result is the definitive value, overwrite the metrics dict
                updated_metrics[python_key] = str(ai_val).strip()
                log.info(f"Gemini Success: {python_key} -> {updated_metrics[python_key]}")

        return updated_metrics

    except Exception as e:
        log.error(f"Gemini API Error: Failed to extract metrics: {e}")
        return metrics

# ──────────────────────────────────────────────────────────────────────────────
# Main Parser (Unmodified)
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
    
    # ── Key Complaints (Line Parse - UNRELIABLE, will be overwritten by Gemini) ────────────────
    m["complaints_key"] = value_near_scoped(lines, "Key Customer Complaints", "integer", COMPLAINTS_SCOPE, near_before=1, near_after=0)
    # ──────────────────────────────────────────────────────────────────────────────────────────

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

            # --- NEW: Final buffer wait to ensure all elements have stabilized ---
            log.info("Adding 5s final buffer wait before screenshot and capture…")
            page.wait_for_timeout(5_000)
            
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
