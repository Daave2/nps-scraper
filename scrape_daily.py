#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Retail Performance Dashboard → Daily Summary (layout-by-lines + GEMINI VISION) → Google Chat

Key points in this build:
- CRITICAL UPDATE: Transitioned to 100% Gemini Vision extraction due to new
  "Retail Steering Wheel" layout requiring navigation/clicks to access data.
- Strategy: Capture initial wheel, navigate to NPS tab, capture NPS detail,
  then combine results.
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
# Paths / constants (Unmodified from previous step)
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).resolve().parent
AUTH_STATE     = BASE_DIR / "auth_state.json"
LOG_FILE       = BASE_DIR / "scrape_daily.log"
DAILY_LOG_CSV  = BASE_DIR / "daily_report_log.csv"
SCREENS_DIR    = BASE_DIR / "screens"

ENV_ROI_MAP    = os.getenv("ROI_MAP_FILE", "").strip()
ROI_MAP_FILE   = Path(ENV_ROI_MAP) if ENV_ROI_MAP else (BASE_DIR / "roi_map.json")

# !!! IMPORTANT !!!
# YOU MUST UPDATE THIS URL TO THE NEW LOOKER STUDIO EMBED URL.
# The GAS link provided (https://script.google.com/...) is unlikely to work
# directly with the existing Playwright authentication state.
DASHBOARD_URL = (
    "https://script.google.com/a/macros/morrisonsplc.co.uk/s/AKfycbwO5CmuEkGFtPLXaZ_B2gMLrWhkLgONDlnsHt3HhOWzHen4yCbVOHA7O8op79zq2NYfCQ/exec"
# !!! IMPORTANT !!!

VIEWPORT = {"width": 1366, "height": 768}

# --- METRICS TO EXTRACT VIA GEMINI (Updated for New Dashboard Structure) ---
GEMINI_METRICS_WHEEL = [
    "shrink", "retail_expenses", "payroll_wheel", "isp", "ambient_wmd", 
    "fresh_wmd", "complaints_wheel", "safe_legal", "taking_to_plan", "sales_lfl_wheel", 
    "nps_wheel", "stock_record"
]
GEMINI_METRICS_NPS = [
    "supermarket_nps", "colleague_happiness", "home_delivery_nps", "cafe_nps", 
    "click_collect_nps", "customer_toilet_nps", "internal_factors_nps", 
    "external_factors_nps", "cc_avg_wait"
]
GEMINI_METRICS_ALL = list(set(GEMINI_METRICS_WHEEL + GEMINI_METRICS_NPS + 
                              ["sales_total", "sales_vs_target", "complaints_key", 
                               "moa", "waste_validation", "unrecorded_waste_pct", 
                               "shrink_vs_budget_pct", "sco_utilisation", "efficiency", 
                               "scan_rate", "interventions", "mainbank_closed", 
                               "swipe_rate", "swipes_wow_pct", "data_provided", 
                               "trusted_data", "my_reports", "weekly_activity", 
                               "payroll_outturn", "absence_outturn", "productive_outturn", 
                               "holiday_outturn", "current_base_cost"]))

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
# Targets / Formatting 
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
# Card + CSV (Logic for metric filtering implemented here)
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
    
    # Check if the value is effectively blank - FIXED: checks for "-", "—", "", None
    is_blank = (val is None or val.strip() == "" or val.strip() == "—" or val.strip() == "-")
    
    # Special handling for FES metrics when they are compounded with "vs Target"
    if custom_val:
        # We need the base metric's value AND the vs_target's value to not be blank to show the complex widget
        vs_target_key = f"{key}_vs_target"
        val_vs = metrics.get(vs_target_key)
        
        # Check both the main value and the vs_target value
        is_vs_blank = (val_vs is None or val_vs.strip() == "" or val_vs.strip() == "—" or val_vs.strip() == "-")
        is_complex_blank = is_blank or is_vs_blank

        if is_complex_blank:
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
            ("Report Time", "page_timestamp"), # Keeping Report Time here as a static context metric
            ("Period", "period_range")
        ]},
        {"title": "Sales", "metrics": [
            ("Sales Total", "sales_total"),
            ("LFL", "sales_lfl"),
            ("vs Target", "sales_vs_target"),
        ]},    
        {"title": "Complaints & NPS", "metrics": [
            ("Key Complaints", "complaints_key"),
            ("Supermarket NPS", "supermarket_nps"),
            ("Colleague Happiness", "colleague_happiness"),
            ("Cafe NPS", "cafe_nps"),
            ("Click & Collect NPS", "click_collect_nps"),
            ("Customer Toilet NPS", "customer_toilet_nps"),
            ("Home Delivery NPS", "home_delivery_nps"), 
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
            # 'weekly_activity' is used for Clean and rotate
            ("Clean and rotate", "weekly_activity"),
        ]},
        {"title": "Payroll", "metrics": [
            ("Payroll Outturn", "payroll_outturn"),
            ("Absence Outturn", "absence_outturn"),
            ("Productive Outturn", "productive_outturn"),
            ("Holiday Outturn", "holiday_outturn"),
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
            ("My Reports", "my_reports") 
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

# ... (omitted helper functions for old text parsing which is no longer used for metrics) ...

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

# ──────────────────────────────────────────────────────────────────────────────
# Gemini Vision Extraction (Combined Logic)
# ──────────────────────────────────────────────────────────────────────────────
def _extract_gemini_vision(image_path: Path, prompt_map: Dict[str, str], system_instruction: str) -> Dict[str, str]:
    """Generic function to call Gemini Vision for a set of fields."""
    if not GEMINI_AVAILABLE or not GEMINI_API_KEY:
        log.warning("Gemini API not available or key missing. Skipping AI extraction.")
        return {}

    if not image_path.exists():
        log.error(f"Image not found at {image_path}. Cannot perform vision extraction.")
        return {}

    client = genai.Client(api_key=GEMINI_API_KEY)
    img = Image.open(image_path)
    
    user_prompt = (
        f"{system_instruction.strip()} Analyze the image and return the exact values for "
        f"the following metrics as a single JSON object. For percentages, include '%'. "
        f"Metrics to extract: {list(prompt_map.keys())}"
    )

    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[img, user_prompt],
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=types.Schema(
                    type=types.Type.OBJECT,
                    properties={v: types.Schema(type=types.Type.STRING) for v in prompt_map.keys()}
                )
            )
        )
        
        ai_data = json.loads(response.text)
        
        extracted = {}
        for ai_key, ai_val in ai_data.items():
            python_key = prompt_map.get(ai_key)
            if python_key and ai_val is not None:
                # Store cleaned value
                extracted[python_key] = str(ai_val).strip()
                log.info(f"Gemini Success: {python_key} -> {extracted[python_key]}")
        
        return extracted

    except Exception as e:
        log.error(f"Gemini Vision API Error for {list(prompt_map.keys())}: {e}")
        return {}

def parse_context_from_lines(lines: List[str]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    joined = "\n".join(lines)
    
    # Store Line (Niki Cooke | 218 Thornton Cleveleys)
    z = re.search(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}).*?\|\s*([^\|]+?)\s*\|\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", joined, re.S)
    m["store_line"] = z.group(0).strip() if z else "—"

    # Report Time/Page Timestamp (From the footer)
    ts_match = re.search(r"\b(\d{1,2}\s+[A-Za-z]{3}\s+\d{4},\s*\d{2}:\d{2}:\d{2})\b", joined)
    m["page_timestamp"] = ts_match.group(1) if ts_match else "—"
    
    # Period Range (Likely only visible on the NPS page after filter application)
    period_match = re.search(r"Dates included:\s*([^\n]+)", joined, re.I)
    m["period_range"] = period_match.group(1).strip() if period_match else "—"

    # NOTE: These manual extraction methods are highly brittle but are the
    # most reliable for grabbing context strings like 'store_line' or 'period_range'.
    return m

# ──────────────────────────────────────────────────────────────────────────────
# Main (UPDATED NAVIGATION)
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

    all_metrics: Dict[str,str] = {}
    
    with sync_playwright() as p:
        browser = context = page = None
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

            # --- STEP 1: Capture and Extract Initial Wheel Data ---
            log.info("Adding 5s final buffer wait before screenshot (Wheel)…")
            page.wait_for_timeout(5_000)
            
            # 1a. Screenshot Wheel
            ts = int(time.time())
            SCREENS_DIR.mkdir(parents=True, exist_ok=True)
            screenshot_path_wheel = SCREENS_DIR / f"{ts}_wheel_page.png"
            save_bytes(screenshot_path_wheel, page.screenshot(full_page=True, type="png"))
            
            # 1b. Extract Context (Time/Store)
            body_text = page.inner_text("body")
            lines = [ln.rstrip() for ln in body_text.splitlines()]
            all_metrics.update(parse_context_from_lines(lines))
            
            # 1c. Extract Wheel Metrics 
            prompt_map_wheel = {
                "Shrink": "shrink_wheel", "Retail Expenses": "retail_expenses", "Payroll": "payroll_outturn", 
                "ISP": "isp", "Ambient WMD": "ambient_wmd", "Fresh WMD": "fresh_wmd", 
                "Complaints": "complaints_key", "Safe & Legal": "safe_legal", 
                "Taking to Plan": "taking_to_plan", "Take-up LFL": "sales_lfl", 
                "NPS": "supermarket_nps", "Stock Record NPS": "stock_record"
            }

            system_inst_wheel = "You are a hyper-accurate retail dashboard data extractor. Extract the main metric (number + unit/K/%) next to each label on the 'Retail Steering Wheel' and other key components (e.g., '42.8K', '(2.3K)', '11', '0.0%'). For items in parentheses like (2.3K) return the value as -2.3K. For items like 'Retail Wheel Guide', return the content text."

            wheel_metrics = _extract_gemini_vision(screenshot_path_wheel, prompt_map_wheel, system_inst_wheel)
            all_metrics.update(wheel_metrics)

            # --- STEP 2: Navigate and Extract NPS Detail Page ---
            log.info("Navigating to NPS Detail page…")
            
            # 2a. Click the NPS navigation button/tab
            try:
                # Find the button/tab with text 'NPS'
                page.get_by_role("button", name="NPS").last.click(timeout=5000) # Use .last to target the tab button
                page.wait_for_timeout(6000) # Wait for content transition and loading
            except Exception as e:
                log.warning(f"Failed to click NPS tab. Skipping NPS detail extraction: {e}")
                pass
            
            # 2b. Screenshot NPS Detail Page
            log.info("Adding 5s final buffer wait before screenshot (NPS Detail)…")
            page.wait_for_timeout(5_000)
            img_bytes_nps = page.screenshot(full_page=True, type="png")
            screenshot_path_nps = SCREENS_DIR / f"{ts}_nps_detail_page.png"
            save_bytes(screenshot_path_nps, img_bytes_nps)
            
            # 2c. Extract NPS Metrics (using a map for the NPS page)
            prompt_map_nps = {
                "Supermarket NPS": "supermarket_nps_detail", "Cafe NPS": "cafe_nps", 
                "Click & Collect NPS": "click_collect_nps", "Internal Factors NPS": "colleague_happiness",
                "External Factors NPS": "external_factors_nps", "Home Delivery NPS": "home_delivery_nps"
            }

            system_inst_nps = "You are a specialist retail data extractor. Extract the main numeric score (number only, ignore targets) for the titled NPS metrics. For NPS values, extract the main large number (e.g., '40', '73', '80')."

            nps_metrics = _extract_gemini_vision(screenshot_path_nps, prompt_map_nps, system_inst_nps)
            
            # Merge NPS metrics, prioritizing detail page data
            all_metrics.update(nps_metrics)
            
            # --- STEP 3: Combine with default values for unextracted metrics ---
            # NOTE: Scan Rate/Interventions/Mainbank Closed/Payroll detail will be MISSING until
            # more navigation steps are added, so they are initialized as '—' now.
            metrics_to_default = ["sales_total", "sales_vs_target", "scan_rate", "interventions", 
                                  "mainbank_closed", "payroll_outturn", "absence_outturn", 
                                  "productive_outturn", "holiday_outturn", "current_base_cost",
                                  "sco_utilisation", "efficiency", "moa", "waste_validation",
                                  "unrecorded_waste_pct", "shrink_vs_budget_pct", "weekly_activity",
                                  "scan_vs_target", "interventions_vs_target", "mainbank_vs_target"]
            
            for key in metrics_to_default:
                if key not in all_metrics:
                     all_metrics[key] = "—" # Initialize if not found by parsers

        finally:
            try:
                if context: context.close()
            except Exception:
                pass
            try:
                if browser: browser.close()
            except Exception:
                pass

    ok = send_card(all_metrics)
    log.info("Daily card send → %s", "OK" if ok else "FAIL")
    # ... (omitted write_csv function call) ...


if __name__ == "__main__":
    # Ensure helper functions are available if running directly
    
    # Dummy definitions for helper functions used in Main but not in this block
    def save_bytes(path: Path, data: bytes):
        try:
            SCREENS_DIR.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            log.info(f"Saved {path.name}")
        except Exception:
            pass
            
    def _post_with_backoff(url: str, payload: dict) -> bool:
        # Dummy implementation
        log.warning("Dummy _post_with_backoff called.")
        return True

    def alert(lines: List[str]):
        # Dummy implementation
        log.warning(f"ALERT: {lines}")
        
    def write_csv(metrics: Dict[str,str]):
        # Dummy implementation
        log.info(f"Dummy write_csv called with {len(metrics)} metrics.")
        
    run_daily_scrape()
