# 📖 Looker Studio Scraper Suite: Full Technical Manual

This repository contains a multi-script, scheduled scraping solution designed to extract business-critical data from Google Looker Studio reports and deliver formatted, actionable alerts to Google Chat. It leverages headless browsing (Playwright) and AI-powered image analysis (Gemini Vision) to ensure robust data capture.

## 1. Overview and Architecture

### File Manifest

| File | Role | Execution | Logic Focus |
| :--- | :--- | :--- | :--- |
| **`scrape_daily.py`** | **Daily Summary Report Generator** | Daily (8:30 AM UTC) | Gemini Vision for visual metrics, Conditional Card Filtering, Custom Color/Target Formatting. |
| **`scrape.py`** | **NPS Comment Scraper** | Multiple Times Daily | 2FA Handling, Comment Parsing, Batched Chat Sending, Deduplication. |
| **`scrape_complaints.py`** | **Customer Complaints Scraper** | Multiple Times Daily | State-Machine Parsing, Case Number Deduplication, Per-Complaint Card Alerting. |
| **`.github/workflows/...`** | **GitHub Actions Workflow** | Scheduled / Manual | Caching, Authentication Fallback, Conditional Runs. |
| **`auth_state.json`** | **Shared Resource** | Stores authenticated browser session cookie. | |
| **`comments_log.csv`** | **Shared Resource** | Logs unique NPS comments. | |
| **`complaints_log.csv`** | **Shared Resource** | Logs unique complaint case numbers. | |

---

## 2. Configuration: GitHub Secrets

The system is configured entirely via **GitHub Secrets**. These secrets are injected into the runtime environment and stored in a temporary `config.ini` for the scripts.

| Secret Name | Purpose & Script Usage |
| :--- | :--- |
| `GOOGLE_EMAIL` | The credential for accessing the Looker Studio reports. |
| `GOOGLE_PASSWORD` | The password for the Google account (used only for re-login). |
| `GEMINI_API_KEY` | **Critical for `scrape_daily.py`**. Provides access to the Gemini API for visual metric extraction. |
| `MAIN_WEBHOOK` | Target URL for general alerts and batched NPS comments. |
| `ALERT_WEBHOOK` | Target URL for high-priority alerts: login failures, 2FA codes, and critical script errors. |
| `DAILY_WEBHOOK` | Target URL for the comprehensive daily summary card. |
| `COMPLAINTS_WEBHOOK` | Target URL for new complaint notifications. |
| `AUTH_STATE_B64` | **(Optional)** Base64-encoded string of a successful `auth_state.json` file for recovery. |

---

## 3. Manual Operational Guide: Initial Login & 2FA

**The first successful run is a required manual process** to handle Google's 2FA and create the shared authenticated session file (`auth_state.json`).

### Initial Authentication Procedure

1.  **Set Secrets:** Verify all required credentials are set in GitHub Secrets.
2.  **Trigger Login:** Navigate to the **Actions** tab, select the primary workflow, and click **Run workflow** (`workflow_dispatch`).
3.  **Wait for Alert:** The `scrape.py` process will detect the need for 2FA and pause.
4.  **2FA Intervention (Critical Step):**
    *   The script uses screen-scraping logic to identify the verification number on the Google sign-in screen.
    *   It sends an alert to the **`ALERT_WEBHOOK`** (e.g., `🔐 Tap this number on your phone: **42**`).
    *   **Action Required:** You must immediately approve the sign-in request on your mobile device and tap the matching number.
5.  **Completion:** Once approved, the session state is saved to `auth_state.json`, cached by the workflow, and used for all subsequent runs.

---

## 4. Deep Dive: Python Script Logic

### A. `scrape_daily.py` (Retail Daily Summary)

The focus is on accuracy through AI and clean output via filtering.

| Feature | Logic Mechanism |
| :--- | :--- |
| **Gemini Vision Integration** | Metrics failing initial text parsing (`"—"`) and all items in `GEMINI_METRICS` (e.g., NPS dials, Payroll, Shrink) are sent with a full screenshot to **Gemini Pro Vision** for accurate visual data extraction. |
| **Data Stabilization** | A mandatory **20-second wait** is executed after initial navigation, followed by a **5-second buffer** before taking the screenshot, ensuring dynamic content is fully rendered. |
| **Conditional Widget Filtering** | The helper `_create_metric_widget` inspects the metric value. It returns `None` (and is thus excluded from the card) if the value is: `None`, empty (`""`), `"—"`, or the literal hyphen (`"-"`), or the placeholder `"NPS"`. |
| **Dynamic Section Filtering** | The `build_chat_card` function only includes a section in the final output if that section's widget list is not empty, removing entire irrelevant blocks. |
| **Custom Color Formatting** | Performance thresholds in `METRIC_TARGETS` are translated using an explicit `STATUS_FORMAT` mapping to the unofficial **`<font color='...'>`** HTML tag (Red: `#FF0000`, Amber: `#FFA500`) to visually flag deviations. |

### B. `scrape_complaints.py` (Customer Complaints)

| Feature | Logic Mechanism |
| :--- | :--- |
| **State-Machine Parsing** | `parse_complaints_from_lines` uses line-by-line sequential logic, progressing based on markers like `DATE_RE` and `CASE_NUM_RE`, to reconstruct unstructured data lines into complete, correctly grouped complaint records (Description, Response, Metadata). |
| **Case Deduplication** | `read_existing_complaints` loads all historical `case_number`s from `complaints_log.csv` into a Python `set`. Only cases not present in this set are sent to Google Chat and appended to the log. |
| **Lock Management** | Employs a physical `scrape.lock` file to prevent concurrent complaint scrapes. |

### C. `scrape.py` (NPS Comments)

| Feature | Logic Mechanism |
| :--- | :--- |
| **2FA Extraction** | The `wait_for_2fa_and_alert` function uses targeted regex and heuristic filtering (to ignore device model numbers like "14T") to reliably extract the 2- or 3-digit verification code from the screen text. |
| **Batched Sending** | New comments are sent in batches (`BATCH_SIZE=10`) within the `_post_with_backoff` loop to respect Google Chat API rate limits. |
| **Stale Lock Cleanup** | Includes checks to remove the `scrape.lock` file if it is older than `STALE_LOCK_MAX_AGE_S` (20 minutes), preventing a hard-fail from blocking the scheduler indefinitely. |

---

## 5. GitHub Actions Workflow (`.github/workflows/...`)

The workflow ensures an organized, robust, and efficient schedule.

| Component | Logic & Purpose |
| :--- | :--- |
| **Triggers** | **Scheduled (Cron):** Runs two types of jobs: Daily Full Run (`30 8 * * *`) and Hourly Partial Runs (e.g., `0 11,13...`). **Manual:** `workflow_dispatch` allows on-demand runs (always executes the Full Run). |
| **Setup & Env** | Installs Python, Playwright, and `google-genai`. Exports all GitHub Secrets as environment variables (`$GITHUB_ENV`) for script access. |
| **Caching** | Uses `actions/cache` on the shared state files (`auth_state.json`, `*log.csv`) across runs, minimizing re-login frequency and preserving data integrity. |
| **Conditional Execution** | Uses the `determine_run_type` step to set the `IS_DAILY_REPORT` flag based on the schedule. The `scrape_daily.py` step is guarded by `if: steps.determine_run_type.outputs.IS_DAILY_REPORT == 'true'`. |
| **Execution Command** | All Python scripts are run via `xvfb-run -a python...`. This is mandatory to provide the virtual display environment required by Playwright's Chromium browser when running on headless Linux runners. |
| **Artifacts** | Logs, screenshots, and state files are uploaded on failure or success to the job artifacts for essential debugging and operational review. |
