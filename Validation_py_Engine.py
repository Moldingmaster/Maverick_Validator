#!/usr/bin/env python3
"""
HMI (Human-Machine Interface) Data Processor - Validation Engine
================================================================

PURPOSE:
    This script is the core engine for an automated quality-control system in a
    manufacturing facility that operates hydraulic presses and curing ovens.
    It continuously watches for new data files exported by press/oven HMI systems,
    parses the raw sensor logs, splits them into individual production cycles,
    validates each cycle against engineering specifications, and generates
    PDF chart reports with Pass/Fail/In-Progress status.

HIGH-LEVEL WORKFLOW:
    1. The main loop polls input directories (one per press/oven) for new .txt/.csv files.
    2. Each press data file is loaded, parsed, and optionally stitched with adjacent
       day-files so that cycles spanning midnight are not split.
    3. The data is split into individual production cycles by detecting idle boundaries
       (tonnage at zero AND thermocouple temps below 399 F).
    4. For each cycle, the script looks up the part number in an Excel spec sheet,
       extracts the required temperature holds, tonnage targets, and soak durations,
       then evaluates the actual sensor data against those specs.
    5. A PDF chart (temperature + tonnage vs. time) is generated along with a
       FAILURE_REPORT.txt when specs are not met.
    6. Oven files follow a similar but simpler flow (temperature-only validation).

KEY CONCEPTS:
    - "Press": A hydraulic press that applies tonnage at temperature to cure parts.
    - "Cycle": One complete press run for a part number — heat up, apply tonnage,
      hold at temp/pressure for a specified time, then cool down and release.
    - "Thermocouple (TC)": Temperature sensor embedded in the press tooling.
      Different press models log TCs as TC1-TC4 or Z1TC1A2A-style zone columns.
    - "Tonnage": The hydraulic force applied by the press, in tons.
    - "Part number / Program": Identifies the product being pressed. The Excel
      spec sheet maps each program number to its required temps, tonnages, and hold times.
    - "Soak": A sustained hold at a target temperature range for a minimum duration.

FIXES APPLIED (v2):
    - UTF-16 encoding support for HMI files
    - clean_numeric_column helper to strip null bytes before numeric conversion
    - Column name normalization (strip BOM, null bytes)
    - Fixed lookback logic for leading zeros (iterate backwards)
    - Filter out cycles that never had pressing
    - Fixed mutable default argument
    - Explicit tonnage cleaning in validation

Date: 2026-01-22
"""

# ============================================================================
# IMPORTS
# ============================================================================

import os
import sys
import pandas as pd

# Force matplotlib to use the non-interactive 'Agg' backend so it can render
# charts to PDF files without needing a display/GUI (this runs on a headless server).
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import MaxNLocator  # Used to keep axis tick counts reasonable

import json           # For persisting processing state between restarts
import subprocess     # For spawning background reprocess worker processes
from datetime import datetime, date, timedelta
import time
import shutil         # For copying oven source files to output folders
import csv            # For writing results_index.csv audit logs
import re             # Regex for parsing part numbers, date codes, spec text
from io import StringIO  # Allows feeding string content into pandas read_csv
from concurrent.futures import ThreadPoolExecutor, as_completed  # Parallel file processing
import threading      # Thread lock for shared results_index CSV writes

import traceback      # For printing full stack traces on unexpected errors
# ============================================================================
# CONFIGURATION
# ============================================================================
# All settings can be overridden via environment variables (prefix HMI_).
# Defaults are tuned for the production floor at the Maverick facility.

# --- Equipment Identifiers ---
# Which press and oven numbers to monitor. Each gets its own input subfolder
# under BASE_WATCH_PATH (e.g., C:\HMI_Upload\Press_3, C:\HMI_Upload\Oven_1).
PRESS_NUMBERS = [3, 4, 5, 6, 7, 8]
OVEN_NUMBERS = [1, 2, 3]

# --- Directory Paths ---
# Where the HMI systems drop their raw data files (one subfolder per machine)
BASE_WATCH_PATH = os.environ.get("HMI_BASE_WATCH_PATH", r"C:\HMI_Upload")
# Network share where processed press results (charts, reports) are written
OUTPUT_BASE = os.environ.get("HMI_OUTPUT_BASE", r"M:\Quality\Press Charts")
# Network share for oven/furnace results
OVEN_OUTPUT_BASE = os.environ.get("HMI_OVEN_OUTPUT_BASE", r"M:\Quality\Furnace Chart")
# Location of the Excel spec files that define press programs and oven cycles
EXCEL_PATH = os.environ.get("HMI_EXCEL_PATH", r"C:\HMI_Upload\PythonScripts")

# --- Polling & Threading ---
# How often (seconds) the main loop checks for new/changed files
WATCH_INTERVAL_SECONDS = int(os.environ.get("HMI_WATCH_INTERVAL_SECONDS", "30"))
# Max parallel worker threads for processing files concurrently
MAX_WORKER_THREADS = int(os.environ.get("HMI_MAX_WORKER_THREADS", "10"))

# --- File Stability Guards ---
# Don't process a file until it's at least this many seconds old (avoids
# reading a file the HMI is still actively writing to)
MIN_FILE_AGE_SECONDS = int(os.environ.get("HMI_MIN_FILE_AGE_SECONDS", "10"))
FILE_STABLE_SECONDS = int(os.environ.get("HMI_FILE_STABLE_SECONDS", "10"))
# Don't re-process the same file more often than this (prevents thrashing
# when a file is updated frequently during an active press cycle)
FILE_REPROCESS_THROTTLE_SECONDS = int(os.environ.get("HMI_FILE_REPROCESS_THROTTLE_SECONDS", "120"))

# --- Lookback Window ---
# On startup, only process files created within this many hours of now.
# This prevents re-processing ancient history on every restart.
DEFAULT_LOOKBACK_HOURS = float(os.environ.get("HMI_DEFAULT_LOOKBACK_HOURS", "35"))
# Optional overrides: set either LOOKBACK_DAYS or LOOKBACK_HOURS to change the window
LOOKBACK_DAYS = os.environ.get("HMI_LOOKBACK_DAYS", "").strip()
LOOKBACK_HOURS = os.environ.get("HMI_LOOKBACK_HOURS", "").strip()

# --- Reprocessing Behavior ---
# If True, ignore the saved state file and reprocess all files in the lookback window
FORCE_REPROCESS = os.environ.get("HMI_FORCE_REPROCESS", "0").strip() in ["1", "true", "True", "yes", "YES"]
# If True, delete existing output folders before reprocessing (clean slate)
PURGE_OUTPUTS_ON_START = os.environ.get("HMI_PURGE_OUTPUTS_ON_START", "0").strip() in ["1", "true", "True", "yes", "YES"]
# JSON file that tracks which files have already been processed (by mtime+size signature)
PROCESS_STATE_PATH = os.environ.get("HMI_PROCESS_STATE_PATH", os.path.join(EXCEL_PATH, "hmi_processor_state.json"))
# If True (default), start with an empty state map so everything gets reprocessed
REPROCESS_ON_START = os.environ.get("HMI_REPROCESS_ON_START", "1").strip() not in ["0", "false", "False", "no", "NO"]
# If True, spawn a separate background Python process to do a one-shot reprocess
SPAWN_REPROCESS_WORKER = os.environ.get("HMI_SPAWN_REPROCESS_WORKER", "0").strip() in ["1", "true", "True", "yes", "YES"]

# --- Cycle Detection: Tonnage & Temperature Thresholds ---
# Extra minutes of data to include before/after the detected pressing window
# so charts show the ramp-up and cool-down context around the actual press hold
CYCLE_PAD_MINUTES = int(os.environ.get("HMI_CYCLE_PAD_MINUTES", "3"))
# Tonnage readings above this are considered sensor noise or errors
MAX_REALISTIC_TONNAGE = float(os.environ.get("HMI_MAX_REALISTIC_TONNAGE", "600"))

# Press 5 reports "TONNAGE DISPLAY" which has a non-zero idle baseline (~10 tons)
PRESS5_IDLE_TONNAGE_DISPLAY = float(os.environ.get("HMI_PRESS5_IDLE_TONNAGE_DISPLAY", "10"))
# Press 7 also idles above zero due to its hydraulic system
PRESS7_IDLE_TONNAGE = float(os.environ.get("HMI_PRESS7_IDLE_TONNAGE", "10"))
# Press 7 must exceed this to be considered "actually pressing" (not just idling)
PRESS7_MIN_PRESSING_TONNAGE = float(os.environ.get("HMI_PRESS7_MIN_PRESSING_TONNAGE", "11"))
# Press 7 must exceed this for a segment to count as a valid production cycle
PRESS7_MIN_CYCLE_TONNAGE = float(os.environ.get("HMI_PRESS7_MIN_CYCLE_TONNAGE", "20"))

# If consecutive data points are more than this many minutes apart, treat as a gap
MAX_TIME_GAP_MINUTES = float(os.environ.get("HMI_MAX_TIME_GAP_MINUTES", "5"))
# If an "In Progress" cycle hasn't received new data in this many minutes, it's stale
STALE_IN_PROGRESS_MINUTES = float(os.environ.get("HMI_STALE_IN_PROGRESS_MINUTES", "180"))

# --- Cycle Detection: Temperature-Based Boundary Detection ---
# When ALL active thermocouples drop below this temp AND tonnage is idle,
# the cycle is considered complete (press is cool enough to open).
TC_TEMP_THRESHOLD = 399  # degrees Fahrenheit
# A cycle must reach at least this peak temperature to be considered a real production
# run (filters out test runs, brief heat-ups that never reached curing temp)
CYCLE_VALID_MIN_MAX_TEMP = float(os.environ.get("HMI_CYCLE_VALID_MIN_MAX_TEMP", "600"))
# A cycle must last at least this many minutes to be valid (filters out aborted runs)
CYCLE_VALID_MIN_DURATION_MIN = float(os.environ.get("HMI_CYCLE_VALID_MIN_DURATION_MIN", "15"))
# Tonnage value that means "press is not applying force"
TONNAGE_ZERO = 0
# When building cycle boundaries, include up to N zero-tonnage samples before/after
# the actual pressing so the chart context is complete
MAX_LEADING_ZEROS = 3
MAX_TRAILING_ZEROS = 3

# --- Failure Report Cleanup ---
# Remove leftover FAILURE_REPORT.txt files from previous runs on startup
CLEAN_FAILURE_REPORTS_ON_START = os.environ.get("HMI_CLEAN_FAILURE_REPORTS_ON_START", "1").strip() not in ["0", "false", "False", "no", "NO"]

# --- File Stitching ---
# When processing a file, also load data from the previous day's file going back
# this many minutes, so cycles that started late in the prior day are captured fully.
PREPEND_PREV_FILE_MINUTES = float(os.environ.get("HMI_PREPEND_PREV_FILE_MINUTES", "120"))
# Similarly, look forward into the next day's file by this many minutes
APPEND_NEXT_FILE_MINUTES = float(os.environ.get("HMI_APPEND_NEXT_FILE_MINUTES", "720"))

# --- Sensor Data Sanity Limits ---
# Temp readings above this are treated as sensor errors and masked out
MAX_REALISTIC_TEMP_F = float(os.environ.get("HMI_MAX_REALISTIC_TEMP_F", "1500"))
# A thermocouple column must have at least this many non-zero readings to be
# considered "active" (prevents dead/disconnected TCs from affecting validation)
MIN_ACTIVE_TC_SAMPLES = int(os.environ.get("HMI_MIN_ACTIVE_TC_SAMPLES", "5"))
# A TC must reach at least this temp to be considered active
ACTIVE_TC_MIN_TEMP_F = float(os.environ.get("HMI_ACTIVE_TC_MIN_TEMP_F", str(TC_TEMP_THRESHOLD)))
# Minimum tonnage that counts as "pressing" for cycle detection purposes
MIN_CYCLE_PRESSING_TONNAGE = float(os.environ.get("HMI_MIN_CYCLE_PRESSING_TONNAGE", "5"))
# Some presses have unreliable tonnage sensors — skip tonnage validation for these.
# Set via comma-separated press numbers, e.g., HMI_TONNAGE_UNRELIABLE_PRESSES="3,8"
TONNAGE_UNRELIABLE_PRESSES = set(
    int(x.strip())
    for x in os.environ.get("HMI_TONNAGE_UNRELIABLE_PRESSES", "").split(",")
    if x.strip().isdigit()
)

# ============================================================================
# MODULE-LEVEL STATE (shared across the main polling loop)
# ============================================================================

# Tracks file (mtime, size) signatures to detect when a file has actually changed
_last_seen_file_signature = {}
# Tracks the wall-clock time each file was last processed (for throttling)
_last_processed_ts = {}
# Thread lock protecting concurrent writes to the shared results_index CSV files
_results_index_lock = threading.Lock()


# ============================================================================
# STATE PERSISTENCE
# ============================================================================
# Between restarts, we save which files have been processed (keyed by filepath,
# valued by (mtime, size) signature). This avoids redundant work when the
# service restarts but the input files haven't changed.

def load_process_state(path: str):
    """Load the JSON state file that records which files have already been processed.

    Returns two dicts:
        processed_press_map: {filepath_str: (mtime, size)} for press files
        processed_oven_map:  {filepath_str: (mtime, size)} for oven files
    Returns empty dicts if the file doesn't exist or is corrupted.
    """
    try:
        if not path or not os.path.isfile(path):
            return {}, {}
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        press_map = data.get("processed_press_map", {}) or {}
        oven_map = data.get("processed_oven_map", {}) or {}
        return press_map, oven_map
    except Exception:
        return {}, {}


def save_process_state(path: str, processed_press_map, processed_oven_map):
    """Persist the current processing state to disk so we can resume after restart."""
    try:
        if not path:
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        payload = {
            "processed_press_map": processed_press_map,
            "processed_oven_map": processed_oven_map,
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
    except Exception:
        pass


# ============================================================================
# BACKGROUND WORKER HELPERS
# ============================================================================

def _should_prompt_spawn_worker() -> bool:
    """Check if we're running interactively (TTY attached) so we can ask the
    operator whether to spawn a background reprocess worker."""
    try:
        return bool(sys.stdin and sys.stdin.isatty())
    except Exception:
        return False


def _spawn_reprocess_worker() -> bool:
    """Spawn a detached child process that does a one-shot reprocess of all
    eligible files. Useful for doing a bulk catch-up without blocking the
    main polling loop."""
    try:
        args = [sys.executable, os.path.abspath(__file__), "--__reprocess_worker"]
        subprocess.Popen(args, close_fds=True)
        return True
    except Exception:
        return False


# ============================================================================
# GENERAL UTILITY FUNCTIONS
# ============================================================================

def safe_print(text):
    """Print wrapper that catches encoding errors (e.g., when console can't
    render certain Unicode characters from HMI file content)."""
    try:
        print(str(text))
    except:
        print("Print error")


def cleanup_failure_reports(output_base: str) -> int:
    """Walk the output directory tree and delete all FAILURE_REPORT.txt files
    left over from previous processing runs. This is done on startup so that
    stale failure reports don't persist after a file has been reprocessed
    and now passes. Returns the count of files removed."""
    removed = 0
    try:
        if not output_base or not os.path.isdir(output_base):
            return 0

        for root, _dirs, files in os.walk(output_base):
            for fname in files:
                if fname == "FAILURE_REPORT.txt" or fname.startswith("FAILURE_REPORT_ARCHIVED_"):
                    try:
                        os.remove(os.path.join(root, fname))
                        removed += 1
                    except Exception:
                        continue
    except Exception:
        return removed

    return removed


# ============================================================================
# DATE CODE HELPERS
# ============================================================================
# HMI data files use a 6-digit YYMMDD date code in their filenames
# (e.g., "Press5_261022.txt" = 2026-10-22). These helpers parse and format
# those codes so we can find adjacent day files for stitching.

def _parse_yymmdd_datecode(datecode: str):
    """Convert a 6-char 'YYMMDD' string to a datetime.date, or None if invalid."""
    try:
        if not datecode or len(datecode) != 6:
            return None
        yy = int(datecode[0:2])
        mm = int(datecode[2:4])
        dd = int(datecode[4:6])
        return date(2000 + yy, mm, dd)
    except Exception:
        return None


def _format_yymmdd_datecode(d: date) -> str:
    """Format a date as a 6-char 'YYMMDD' string."""
    return f"{d.year % 100:02d}{d.month:02d}{d.day:02d}"


def get_adjacent_daily_file_path(filepath: str, delta_days: int):
    """Given a press data file path with a YYMMDD date code in the filename,
    compute the path to the file for +/- delta_days.

    Example: get_adjacent_daily_file_path("Press5_260115.txt", -1)
             returns "Press5_260114.txt"

    Returns None if the filename doesn't contain a parseable date code.
    """
    try:
        base = os.path.basename(filepath)
        # Find the last 6-digit group right before .txt
        m = list(re.finditer(r"\d{6}(?=\.txt$)", base))
        if not m:
            return None
        m = m[-1]
        cur_date = _parse_yymmdd_datecode(m.group(0))
        if cur_date is None:
            return None
        new_date = cur_date + timedelta(days=delta_days)
        new_code = _format_yymmdd_datecode(new_date)
        new_base = base[:m.start()] + new_code + base[m.end():]
        return os.path.join(os.path.dirname(filepath), new_base)
    except Exception:
        return None


# ============================================================================
# PRESS DATA FILE LOADING & PARSING
# ============================================================================

def load_press_dataframe(filepath: str):
    """Load a tab-delimited press data file into a pandas DataFrame.

    HMI systems export data in two formats depending on the press model:
      - 15-column format (Press 5): Has TONNAGE DISPLAY, TOOL TEMP, zone TCs (Z1TC1A2A, etc.)
      - 11-column format (Presses 3,4,6,7,8): Has TC1-TC4, TONNAGE, PLATTEN1/2

    Some files include a header row; others are headerless (we detect which
    by checking if the first line contains 'Date' or 'TONNAGE').

    The Date column is particularly tricky because HMI systems format it as
    "HH:MM:SS MM-DD-YYYY" (time first!) and may include UTF-16 null bytes
    or BOM characters. We try multiple parse strategies to handle this.

    Returns:
        DataFrame with DatetimeIndex (the 'Date' column), or None on failure.
        The 'Date' column is also kept as a regular column for convenience.
    """
    try:
        content = safe_file_read(filepath)
        if not content:
            return None

        # Detect whether the file has a header row
        first_line = content.split('\n')[0]
        has_header = 'Date' in first_line or 'TONNAGE' in first_line
        data_io = StringIO(content)
        if has_header:
            # File has its own column names — use them directly
            df = pd.read_csv(data_io, sep='\t')
            df = normalize_column_names(df)
            required_cols = ['Date', 'PART#']
            for col in required_cols:
                if col not in df.columns:
                    return None
        else:
            # No header — assign column names based on the number of tab-separated fields.
            # This handles the two known HMI export formats.
            raw_df = pd.read_csv(StringIO(content), sep='\t', header=None, engine='python', dtype=str, na_filter=False)
            num_cols = raw_df.shape[1]
            # Press 5 format: 15 columns with zone-based TC names and TONNAGE DISPLAY
            headers15 = ['Date', 'TONNAGE DISPLAY', 'TOOL TEMP', 'TOP PLATTEN AVG', 'BOTTOM PLATTEN',
                         'Z1TC1A2A', 'Z1TC3A4A', 'Z2TC1B2B', 'Z2TC3B4B',
                         'Z3TC1C2C', 'Z3TC3C4C', 'Z4TC1D2D', 'Z4TC3D4D', 'SCREENNO', 'PART#']
            # Standard press format: 11 columns with simple TC1-TC4 names
            headers11 = ['Date', 'TC1', 'TC2', 'TC3', 'TC4',
                         'TONNAGE', 'PLATTEN1', 'PLATTEN2', 'PRESS', 'PART#', 'Useless']
            if num_cols >= 15:
                names = headers15[:num_cols]
            elif num_cols == 11:
                names = headers11
            else:
                # Unknown format — use generic names, but always put Date first and PART# last
                names = ['Date'] + [f'Col{i}' for i in range(1, num_cols - 1)] + ['PART#']
            df = pd.read_csv(StringIO(content), sep='\t', names=names, engine='python')
            df = normalize_column_names(df)

        # --- Parse the Date column into proper datetime objects ---
        # HMI date strings may contain null bytes, BOM, or extra whitespace
        s = df['Date'].astype(str)
        s = s.str.replace(r'[\x00-\x1F\x7F\u200B\uFEFF]', '', regex=True)
        s = s.str.strip().str.replace(r'\s+', ' ', regex=True)
        # Extract time (HH:MM:SS) and date (MM-DD-YYYY) components separately
        # because the HMI often puts time BEFORE date (non-standard order)
        time_part = s.str.extract(r'(\d{2}:\d{2}:\d{2})')[0]
        date_part = s.str.extract(r'(\d{2}-\d{2}-\d{4})')[0]
        # Try time-first format: "HH:MM:SS MM-DD-YYYY" (most common HMI format)
        combined_tf = time_part.str.cat(date_part, sep=' ', na_rep='')
        dt = pd.to_datetime(combined_tf, format='%H:%M:%S %m-%d-%Y', errors='coerce')
        if dt.isna().all():
            # Fallback: try date-first format "MM-DD-YYYY HH:MM:SS"
            combined_df = date_part.str.cat(time_part, sep=' ', na_rep='')
            dt = pd.to_datetime(combined_df, format='%m-%d-%Y %H:%M:%S', errors='coerce')
        if dt.isna().all():
            # Last resort: let pandas auto-detect the format
            dt = pd.to_datetime(s, errors='coerce')
        df['Date'] = dt
        # Drop rows where the date couldn't be parsed (corrupt lines, partial writes)
        df = df.dropna(subset=['Date'])
        if len(df) < 1:
            return None
        # Add a human-readable time string for chart labels
        df['TimeStr'] = df['Date'].dt.strftime('%H:%M')
        # Use Date as the index (for time-based slicing) but also keep it as a column
        df.set_index('Date', inplace=True, drop=False)
        return df
    except Exception:
        return None


def is_file_stable_for_processing(filepath: str) -> bool:
    """Check that a file is old enough to be fully written before we process it.
    The HMI writes data continuously during a press cycle, so we wait until
    the file hasn't been modified for MIN_FILE_AGE_SECONDS before reading it."""
    try:
        now = time.time()
        age_seconds = now - os.path.getmtime(filepath)
        if age_seconds < MIN_FILE_AGE_SECONDS:
            return False

        return True
    except Exception:
        return False


def get_file_signature(filepath: str) -> tuple:
    """Return (mtime, size) for a file. Used to detect whether a file has changed
    since we last processed it — if the signature is the same, we skip it."""
    try:
        return (os.path.getmtime(filepath), os.path.getsize(filepath))
    except Exception:
        return None


# ============================================================================
# CYCLE WINDOW & COMPLETION DETECTION
# ============================================================================

def expand_cycle_window_around_pressing(df, start_idx, end_idx, tonnage_col):
    """Expand a detected cycle's time window to include context before/after pressing.

    The raw cycle boundaries from split_into_cycles() are tight around the data.
    This function adds CYCLE_PAD_MINUTES of padding before the first tonnage
    application and after the last, so the resulting chart shows the heat-up
    ramp and cool-down ramp for better visual context.

    Args:
        df: Full DataFrame for the entire file (used to pull extra rows from)
        start_idx, end_idx: Integer positions of the cycle within df
        tonnage_col: Name of the tonnage column ('TONNAGE' or 'TONNAGE DISPLAY')

    Returns:
        DataFrame with the expanded time window, or the original slice if
        expansion isn't possible.
    """
    cycle_df = df.iloc[start_idx:end_idx + 1].copy()
    if cycle_df.empty or tonnage_col not in cycle_df.columns:
        return cycle_df

    try:
        # Determine which press this is (affects idle threshold)
        press_number = None
        try:
            if 'PRESS' in cycle_df.columns and len(cycle_df) > 0:
                press_number = int(float(cycle_df['PRESS'].iloc[0]))
        except Exception:
            press_number = None

        idle_threshold = get_idle_tonnage_threshold(press_number)
        cycle_pressing_threshold = get_cycle_pressing_threshold(press_number)
        tonnage = clean_numeric_column(cycle_df[tonnage_col])
        # Find the time range where tonnage was actually applied
        pressing_mask = (tonnage > cycle_pressing_threshold) & (tonnage <= MAX_REALISTIC_TONNAGE)
        if not pressing_mask.any():
            return cycle_df

        # Extend the window by CYCLE_PAD_MINUTES before first press and after last press
        press_start = cycle_df.index[pressing_mask].min()
        press_end = cycle_df.index[pressing_mask].max()
        pad = timedelta(minutes=CYCLE_PAD_MINUTES)
        window_start = min(cycle_df.index.min(), press_start - pad)
        window_end = max(cycle_df.index.max(), press_end + pad)

        # Pull the expanded window from the full file DataFrame
        expanded = df.loc[(df.index >= window_start) & (df.index <= window_end)].copy()
        if expanded.empty:
            return cycle_df
        return expanded
    except Exception:
        return cycle_df


def is_cycle_complete(cycle_df, tonnage_col, press_number=None):
    """Determine whether a press cycle has finished based on the last 5 minutes of data.

    A cycle is considered complete when BOTH of these are true for the entire
    trailing 5-minute window:
      1. Tonnage is at or below the idle threshold (press is not applying force)
      2. All active thermocouples are below 399 F (press has cooled enough to open)

    Special case: if all sensor values (tonnage + temps) are exactly zero for
    the trailing window, that also counts as complete (some HMI loggers zero
    out all channels when the cycle program ends).

    Returns True if the cycle appears complete, False otherwise.
    """
    try:
        if cycle_df.empty or tonnage_col not in cycle_df.columns:
            return False

        # Look at only the last 5 minutes of data
        last_time = cycle_df.index.max()
        window_start = last_time - timedelta(minutes=5)
        window_df = cycle_df.loc[cycle_df.index >= window_start].copy()
        if window_df.empty:
            return False

        # Need a full 5-minute window to be confident
        window_minutes = (window_df.index.max() - window_df.index.min()).total_seconds() / 60
        if window_minutes < 5:
            return False

        # Check tonnage is idle across the entire window
        idle_threshold = get_idle_tonnage_threshold(press_number)
        tonnage = clean_numeric_column(window_df[tonnage_col]).fillna(0)
        tonnage_idle = (tonnage <= idle_threshold).all()

        temp_cols = _get_temperature_columns(window_df)
        if not temp_cols:
            return False

        temps_window, active_cols = _get_active_temperature_columns(window_df, temp_cols)
        if not active_cols:
            return False

        # Special case: everything is zero (logger stopped outputting real data)
        all_zero_window = tonnage_idle and (temps_window.fillna(0) == 0).all().all()
        if all_zero_window:
            return True

        # Normal case: tonnage idle AND all active TCs below 399 F
        temps_active_window = temps_window[active_cols]
        temps_active_ok = temps_active_window.notna().all(axis=1) & (temps_active_window < TC_TEMP_THRESHOLD).all(axis=1)
        return tonnage_idle and temps_active_ok.all()
    except Exception:
        return False


# ============================================================================
# FILE ENCODING & DATA CLEANING
# ============================================================================
# HMI systems (especially older Siemens/Allen-Bradley models) export data in
# UTF-16 encoding with BOM markers and null bytes embedded in the content.
# Standard Python text reading fails on these files, so we need a multi-strategy
# approach to decode them reliably.

def safe_file_read(filepath):
    """Read a text file, auto-detecting its encoding. Handles UTF-16 HMI files.

    Strategy:
      1. Try a list of common encodings (UTF-16 first, since that's most common
         for HMI exports). Validate that the decoded content looks like real text.
      2. If all text-mode reads fail, fall back to binary mode and detect encoding
         by looking for UTF-16 BOM markers or null-byte patterns.

    Returns the file content as a clean string, or None if unreadable.
    """
    # Try UTF-16 FIRST since HMI files commonly use it
    encodings = ['utf-16', 'utf-16-le', 'utf-16-be', 'utf-8', 'utf-8-sig', 'cp1252', 'latin-1', 'iso-8859-1']

    for encoding in encodings:
        try:
            with open(filepath, 'r', encoding=encoding) as f:
                content = f.read()
            if content and len(content.strip()) > 0:
                # UTF-16 decoding can leave stray null bytes in the output
                if 'utf-16' in encoding.lower():
                    content = content.replace('\x00', '')

                # Sanity check: first line should be normal ASCII-range text
                # (column headers or date strings), not high-Unicode garbage
                first_line = content.split('\n')[0] if '\n' in content else content[:100]
                if not any(ord(c) > 1000 for c in first_line[:50]):
                    return content
        except:
            continue

    # Fallback: read as raw bytes and manually detect encoding
    try:
        with open(filepath, 'rb') as f:
            raw_content = f.read()

        # Check for UTF-16 BOM (Byte Order Mark) at start of file
        if raw_content.startswith(b'\xff\xfe') or raw_content.startswith(b'\xfe\xff'):
            content = raw_content.decode('utf-16', errors='ignore')
            content = content.replace('\x00', '')
            return content

        # No BOM, but null bytes present = likely UTF-16-LE without BOM
        if b'\x00' in raw_content[:100]:
            content = raw_content.decode('utf-16-le', errors='ignore')
            content = content.replace('\x00', '')
            return content

        # Last resort: treat as UTF-8
        content = raw_content.decode('utf-8', errors='ignore')
        if content and len(content.strip()) > 0:
            return content
    except:
        pass

    return None


def normalize_column_names(df):
    """Strip encoding artifacts from DataFrame column names.

    HMI-exported files often have invisible characters in column headers:
    BOM (\\uFEFF), null bytes (\\x00), and extra whitespace. This function
    cleans all of those so we can reliably match column names like 'TONNAGE'.
    """
    df.columns = (df.columns
                  .astype(str)
                  .str.replace('\x00', '', regex=False)
                  .str.replace('\ufeff', '', regex=False)
                  .str.replace('\0', '', regex=False)
                  .str.strip())
    return df


def clean_numeric_column(series):
    """Convert a pandas Series to numeric values, stripping UTF-16 artifacts first.

    HMI data values like tonnage and temperature are stored as text in the file
    and may contain embedded null bytes from UTF-16 encoding. This function
    strips those artifacts before converting to float. Non-numeric values
    become NaN (not errors).
    """
    cleaned = series.astype(str).str.replace('\x00', '', regex=False)
    cleaned = cleaned.str.replace('\0', '', regex=False)
    cleaned = cleaned.str.strip()
    return pd.to_numeric(cleaned, errors='coerce')


# ============================================================================
# EXCEL SPEC SHEET LOADING
# ============================================================================
# The engineering department maintains two Excel workbooks that define the
# required processing conditions for each product:
#
# 1. Press Programs (Form# 0337): Maps each program/part number to its
#    required temperature setpoints, tonnage targets, hold times, and tool
#    cavity counts. The "Program Detail" sheet has one row per program.
#
# 2. Oven Cycles (OvenCyclesMaverick): Maps oven file prefixes to their
#    required ramp rates, soak temperatures, and hold durations.
#
# We try a "_FIXED" version first (manually cleaned by engineering) and
# fall back to the original if not found.

def load_excel_files():
    """Load the press program spec sheet and oven cycle spec sheet from Excel.

    Returns:
        program_df: DataFrame with one row per press program number, containing
                    columns for temperature, tonnage, hold time specs, and tool counts.
        oven_df:    DataFrame with oven cycle definitions keyed by file prefix.

    Both may be empty DataFrames if the Excel files can't be loaded.
    """
    program_df = pd.DataFrame()
    oven_df = pd.DataFrame()

    # Try the fixed version first, then the original
    press_files = [
        (os.path.join(EXCEL_PATH, "Copy of Form# 0337 - SuperImide Auto Press Programs Rev D_FIXED.xlsx"), "Program Detail"),
        (os.path.join(EXCEL_PATH, "Copy of Form# 0337 - SuperImide Auto Press Programs Rev D.xlsx"), "Program Detail"),
    ]
    oven_files = [
        (os.path.join(EXCEL_PATH, "OvenCyclesMaverick_FIXED.xlsx"), "OvenCycles"),
        (os.path.join(EXCEL_PATH, "OvenCyclesMaverick.xlsx"), "OvenCycles"),
    ]

    for path, sheet in press_files:
        if os.path.exists(path):
            try:
                df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
                if "Program" in df.columns:
                    df["Program"] = pd.to_numeric(df["Program"], errors="coerce")
                    program_df = df.dropna(subset=["Program"])
                    safe_print(f"SUCCESS: Loaded press programs: {os.path.basename(path)}")
                    break
            except Exception as e:
                safe_print(f"WARNING: Could not load {path}: {e}")

    for path, sheet in oven_files:
        if os.path.exists(path):
            try:
                df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
                if "Excel File Prefix" in df.columns:
                    oven_df = df
                    safe_print(f"SUCCESS: Loaded oven conditions: {os.path.basename(path)}")
                    break
            except Exception as e:
                safe_print(f"WARNING: Could not load {path}: {e}")

    return program_df, oven_df


def get_tool_quantity(part_number, program_df):
    """Look up how many tool cavities (nests/stacks) are used for a given part number.

    This matters because tonnage specs in the program sheet are "per tool" —
    e.g., "Apply 50 Tons/ea" with 4 tools means the press should show ~200 tons total.

    The function tries multiple strategies to find the tool count:
      1. Look for columns with names containing 'tool'+'qty' or 'cavity'+'count'
      2. Fall back to columns scored by keyword relevance
      3. Parse complex formats like "2x4" or "8 tools"
      4. Default to 1 if nothing is found

    Returns an integer tool count (1-200 range considered plausible).
    """
    if program_df is None or program_df.empty:
        return 1
    try:
        match = program_df.loc[program_df['Program'] == part_number]
        if match.empty:
            return 1

        def _parse_qty(raw_val):
            qty = pd.to_numeric(raw_val, errors='coerce')
            if pd.notna(qty):
                try:
                    q = int(float(qty))
                    return q
                except Exception:
                    return None

            s = str(raw_val)
            m_mul = re.search(r"(\d+)\s*[xX\*]\s*(\d+)", s)
            if m_mul:
                try:
                    a = int(m_mul.group(1))
                    b = int(m_mul.group(2))
                    if a > 0 and b > 0:
                        return int(a * b)
                except Exception:
                    return None
            m = re.search(r"(\d+)", s)
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    return None
            return None

        def _is_plausible_tool_qty(q):
            try:
                q = int(q)
            except Exception:
                return False
            return 1 <= q <= 200

        def _col_score(col_name):
            s = str(col_name).strip().lower()
            score = 0
            if any(k in s for k in ['tool', 'cav', 'cavity', 'cavities', 'nest', 'stack']):
                score += 10
            if any(k in s for k in ['qty', 'quantity', 'count', 'total', 'num', 'number', '#', 'no.']):
                score += 5
            if any(k in s for k in ['temp', 'ton', 'time', 'min', 'max', 'range', 'heat', 'hold', 'soak', 'deg', '°', 'f']):
                score -= 20
            return score

        tool_cols = []
        for c in match.columns:
            s = str(c).strip().lower()
            if any(k in s for k in ['tool', 'cav', 'cavity', 'cavities', 'nest', 'stack']):
                if any(k in s for k in ['qty', 'quantity', 'count', 'total', 'num', 'number', '#', 'no.']):
                    tool_cols.append(c)

        if not tool_cols:
            for c in match.columns:
                if _col_score(c) >= 10:
                    tool_cols.append(c)

        if not tool_cols:
            tool_cols = []

        best_qty = None
        best_score = None
        for tool_col in tool_cols:
            raw_val = match[tool_col].values[0]
            q = _parse_qty(raw_val)
            if q is None or not _is_plausible_tool_qty(q):
                continue
            sc = _col_score(tool_col)
            if best_qty is None or sc > best_score or (sc == best_score and q > best_qty):
                best_qty = q
                best_score = sc

        if best_qty is not None:
            return best_qty

        row = match.iloc[0]
        best_qty = None
        best_score = None
        for c in match.columns:
            q = _parse_qty(row[c])
            if q is None or not _is_plausible_tool_qty(q):
                continue
            sc = _col_score(c)
            if sc < 0:
                continue
            if best_qty is None or sc > best_score or (sc == best_score and q > best_qty):
                best_qty = q
                best_score = sc

        return best_qty if best_qty is not None else 1
    except Exception:
        pass
    safe_print(f"WARNING: Tool quantity not found for {part_number}, using default value 1")
    return 1


def extract_conditions(steps, num_tools):
    """Parse the free-text step descriptions from the Excel spec sheet into
    structured validation conditions.

    The Excel program sheet has columns like Step1, Step2, ... containing
    human-readable instructions such as:
        "Heat to 350°F"
        "Apply 50.0 Tons/ea @ 350°F"
        "Hold for 60 Min. @ 350°F"
        "Soak for 120 Minutes Min."

    This function uses regex to extract the numeric parameters from each step
    and builds a list of condition dicts that the validator can check against
    the actual sensor data. Tonnage is multiplied by num_tools to get the
    total expected press tonnage (specs are per-tool).

    Returns:
        List of condition dicts, each with 'type' (temperature/tonnage/soak),
        target ranges, and optional duration requirements.
    """
    temp_pattern = re.compile(r'Heat\s+to\s*(\d+)\s*°?\s*F', re.IGNORECASE)
    tonnage_pattern = re.compile(r'Apply\s*([\d\.]+)\s*Tons?/ea\s*@\s*(\d+)\s*°?\s*F', re.IGNORECASE)
    hold_pattern = re.compile(r'Hold\s+for\s*(\d+)\s*Min\.?\s*@\s*(\d+)\s*°?\s*F', re.IGNORECASE)
    soak_pattern = re.compile(r'Soak\s+for\s*(\d+)\s*Minutes?\s*Min\.?', re.IGNORECASE)

    conditions = []
    last_tonnage_condition_idx = None
    last_tonnage_temp = None
    last_heat_temp = None
    for step in steps:
        if isinstance(step, str):
            temp_match = temp_pattern.search(step)
            tonnage_match = tonnage_pattern.search(step)
            hold_match = hold_pattern.search(step)
            soak_match = soak_pattern.search(step)

            if temp_match:
                temp = int(temp_match.group(1))
                last_heat_temp = temp
                conditions.append({
                    'type': 'temperature',
                    'range': (temp - 20, temp + 40),
                    'duration': None,
                })

            if tonnage_match:
                tons_per_tool = float(tonnage_match.group(1))
                temp = int(tonnage_match.group(2))
                total_tons = tons_per_tool * num_tools
                tolerance = total_tons * 0.03
                conditions.append({
                    'type': 'tonnage',
                    'tons_per_tool': tons_per_tool,
                    'num_tools': num_tools,
                    'tons_range': (total_tons - tolerance, total_tons + tolerance),
                    'temp_range': (temp - 20, temp + 40),
                    'duration': None,
                })
                last_tonnage_condition_idx = len(conditions) - 1
                last_tonnage_temp = temp

            if hold_match:
                duration = int(hold_match.group(1))
                temp = int(hold_match.group(2))
                conditions.append({
                    'type': 'temperature',
                    'range': (temp - 20, temp + 40),
                    'duration': (duration - 5, duration + 5),
                })

                if last_tonnage_condition_idx is not None:
                    conditions[last_tonnage_condition_idx]['duration'] = (duration - 5, duration + 5)
                    conditions[last_tonnage_condition_idx]['temp_range'] = (temp - 20, temp + 40)

            if soak_match:
                duration = int(soak_match.group(1))
                if last_heat_temp is not None:
                    conditions.append({
                        'type': 'soak',
                        'range': (last_heat_temp - 20, last_heat_temp + 40),
                        'duration': duration,
                    })
    return conditions


def _infer_effective_tool_count_from_tonnage(cycle_df, tonnage_col, press_number, conditions, current_num_tools):
    """Auto-detect the actual number of tools loaded based on observed tonnage.

    Sometimes the operator loads fewer (or more) tools than the spec sheet says.
    For example, the spec says 4 tools at 50 tons/ea = 200 tons total, but the
    actual median pressing tonnage is ~100 tons, implying only 2 tools were loaded.

    This function compares the observed median pressing tonnage against the
    expected total (from the spec), and if the ratio is ~2x off, it infers the
    actual tool count and adjusts the tonnage validation ranges accordingly.

    Only triggers when the ratio is > 1.8x (clearly a different tool count,
    not just normal process variation).

    Returns:
        (inferred_tool_count, updated_conditions_list)
    """
    try:
        if not conditions:
            return current_num_tools, conditions
        ton_conds = [c for c in conditions if c.get('type') == 'tonnage']
        if not ton_conds:
            return current_num_tools, conditions

        base = ton_conds[0]
        tons_per_tool = base.get('tons_per_tool')
        if tons_per_tool is None:
            return current_num_tools, conditions

        cycle_pressing_threshold = get_cycle_pressing_threshold(press_number)
        t_series = clean_numeric_column(cycle_df[tonnage_col]).fillna(0)
        pressing_tonnage = t_series[t_series > cycle_pressing_threshold]
        if pressing_tonnage.empty:
            return current_num_tools, conditions

        pressing_med = float(pressing_tonnage.median())
        if pressing_med <= 0:
            return current_num_tools, conditions

        expected_center = None
        try:
            tr = base.get('tons_range')
            if tr is not None and len(tr) == 2:
                expected_center = (float(tr[0]) + float(tr[1])) / 2.0
        except Exception:
            expected_center = None

        if expected_center is None or expected_center <= 0:
            return current_num_tools, conditions

        ratio = pressing_med / expected_center
        if ratio < 1.8 and ratio > (1.0 / 1.8):
            return current_num_tools, conditions

        implied_tools = int(round(pressing_med / float(tons_per_tool)))
        if implied_tools < 1 or implied_tools > 200:
            return current_num_tools, conditions

        implied_total = float(tons_per_tool) * float(implied_tools)
        if implied_total <= 0:
            return current_num_tools, conditions

        tol = implied_total * 0.03
        low = implied_total - tol
        high = implied_total + tol
        if not (low <= pressing_med <= high):
            return current_num_tools, conditions

        updated = []
        for c in conditions:
            if c.get('type') == 'tonnage' and c.get('tons_per_tool') is not None:
                tp = float(c.get('tons_per_tool'))
                tot = tp * float(implied_tools)
                t_tol = tot * 0.03
                cc = dict(c)
                cc['num_tools'] = implied_tools
                cc['tons_range'] = (tot - t_tol, tot + t_tol)
                updated.append(cc)
            else:
                updated.append(c)

        return implied_tools, updated
    except Exception:
        return current_num_tools, conditions


def _get_temperature_columns(session_group):
    """Find all thermocouple (TC) columns in the DataFrame.

    Matches two naming patterns used by different press models:
      - Standard presses: TC1, TC2, TC3, TC4
      - Press 5 (zone-based): Z1TC1A2A, Z1TC3A4A, Z2TC1B2B, etc.

    Returns a list of matching column names.
    """
    cols = []
    for c in session_group.columns:
        s = str(c).strip()
        s_upper = s.upper()
        if re.match(r'^TC\d+$', s_upper):
            cols.append(c)
            continue
        if re.match(r'^Z\d+TC', s_upper):
            cols.append(c)
            continue
    return cols


def _get_active_temperature_columns(session_group, temp_cols):
    """Filter TC columns to only those that are actively reading during this cycle.

    A TC is considered "active" if it has:
      - At least MIN_ACTIVE_TC_SAMPLES non-zero readings (not a dead sensor)
      - A peak temperature >= ACTIVE_TC_MIN_TEMP_F (399 F) (actually got hot)

    Values of exactly 0, above MAX_REALISTIC_TEMP_F (1500 F), or below -50 F
    are masked as invalid (sensor errors or disconnected probes).

    Returns:
        temps_df: The cleaned temperature DataFrame (invalid values = NaN)
        active:   List of column names that passed the activity filter
    """
    temps_df = session_group[temp_cols].copy()
    # Mask out sensor errors: zero means disconnected, >1500 F is impossible, <-50 F is noise
    temps_df = temps_df.mask((temps_df == 0) | (temps_df > MAX_REALISTIC_TEMP_F) | (temps_df < -50))
    active = []
    for c in temp_cols:
        try:
            s = temps_df[c]
            # Need enough valid readings to be meaningful
            if int(s.notna().sum()) < MIN_ACTIVE_TC_SAMPLES:
                continue
            try:
                max_temp = float(s.max())
            except Exception:
                max_temp = float("nan")
            # Must have actually gotten hot (reached curing temperature range)
            if pd.isna(max_temp) or max_temp < ACTIVE_TC_MIN_TEMP_F:
                continue
            active.append(c)
        except Exception:
            continue
    return temps_df, active


def _max_continuous_minutes(mask, time_index):
    """Calculate the longest continuous run (in minutes) where a boolean mask is True.

    This is the core duration-checking function used to verify hold times.
    For example, if the spec says "Hold for 60 min at 350 F", we need to find
    the longest unbroken stretch where temperature was in-range.

    Time gaps > 1.5 minutes between consecutive data points break the streak
    (indicates a data logging interruption). The first sample in a streak
    is assigned the typical (median) time delta between samples.

    Returns the maximum continuous duration in minutes.
    """
    current_duration = 0.0
    max_duration = 0.0

    # Calculate the typical time step between consecutive data points.
    # HMI loggers typically write once per minute, but this can vary.
    typical_delta = 1.0
    try:
        deltas = []
        for i in range(1, len(mask)):
            delta = (time_index[i] - time_index[i - 1]).total_seconds() / 60
            if delta <= 1.5:
                deltas.append(float(delta))
        if deltas:
            typical_delta = float(pd.Series(deltas).median())
    except Exception:
        typical_delta = 1.0

    for i in range(len(mask)):
        if not bool(mask.iloc[i]):
            # Condition not met at this point — reset the streak
            current_duration = 0.0
            continue

        if i == 0:
            # First sample: assume one typical time step of duration
            current_duration = typical_delta
        else:
            delta = (time_index[i] - time_index[i - 1]).total_seconds() / 60
            if bool(mask.iloc[i - 1]) and delta <= 1.5:
                # Continuation of an unbroken streak
                current_duration += delta
            else:
                # Gap too large or previous sample wasn't in-range — start new streak
                current_duration = typical_delta

        max_duration = max(max_duration, current_duration)

    return max_duration


def _widen_tonnage_range_if_close(cycle_df, tonnage_col, press_number, conditions, max_extra_tol=0.06):
    """Slightly widen the acceptable tonnage range if the observed median is close
    but just outside the +-3% default tolerance.

    Press hydraulics have natural variation. If the spec says 200 tons +/-3% but
    the press is consistently hitting 191 tons (4.5% low), that's still likely
    acceptable. This function widens the tolerance up to max_extra_tol (6%) to
    avoid nuisance failures from minor calibration drift.

    Only widens if the observed median is within max_extra_tol of the center —
    truly wrong tonnages (e.g., wrong tool count) won't be rescued by this.
    """
    try:
        if not conditions:
            return conditions
        if tonnage_col is None or tonnage_col not in cycle_df.columns:
            return conditions

        cycle_pressing_threshold = get_cycle_pressing_threshold(press_number)
        t_series = clean_numeric_column(cycle_df[tonnage_col]).fillna(0)
        pressing = t_series[t_series > cycle_pressing_threshold]
        if pressing.empty:
            return conditions

        pressing_med = float(pressing.median())
        if pressing_med <= 0:
            return conditions

        updated = []
        for c in conditions:
            if c.get('type') != 'tonnage':
                updated.append(c)
                continue

            tr = c.get('tons_range')
            if tr is None or len(tr) != 2:
                updated.append(c)
                continue

            low = float(tr[0])
            high = float(tr[1])
            if low <= pressing_med <= high:
                updated.append(c)
                continue

            center = (low + high) / 2.0
            if center <= 0:
                updated.append(c)
                continue

            ratio = pressing_med / center
            diff = abs(ratio - 1.0)
            if diff > float(max_extra_tol):
                updated.append(c)
                continue

            # Widen just enough (bounded) to include observed median
            tol = max(0.03, min(float(max_extra_tol), diff + 0.01))
            new_low = center * (1.0 - tol)
            new_high = center * (1.0 + tol)

            cc = dict(c)
            cc['tons_range'] = (new_low, new_high)
            updated.append(cc)

        return updated
    except Exception:
        return conditions


def evaluate_conditions_progress(session_group, conditions, press_number=None):
    """Check each spec condition against the actual cycle data and report which
    conditions have been met and which haven't.

    This is the primary validation function called for every cycle. It checks:
      - Temperature conditions: Did all active TCs reach the target range?
        If a duration is specified, were they in-range for long enough?
      - Soak conditions: Was the average TC temperature held within the target
        band for the required continuous duration?
      - Tonnage conditions: Was the correct tonnage applied at the correct
        temperature, and if a hold duration is specified, was it sustained?

    Returns:
        all_met (bool): True if every condition passed
        unmet_reasons (list[str]): Human-readable explanations for each failure
        debug (dict): Per-condition details for troubleshooting
    """
    session_group = session_group.copy()
    unmet_reasons = []
    debug = {}

    if press_number is None:
        try:
            if 'PRESS' in session_group.columns and len(session_group) > 0:
                press_number = int(float(session_group['PRESS'].iloc[0]))
        except Exception:
            press_number = None

    for col in session_group.columns:
        if col not in ['Date', 'TimeStr', 'PART#', 'SCREENNO']:
            try:
                session_group[col] = clean_numeric_column(session_group[col])
            except Exception:
                continue

    time_index = session_group.index
    temp_cols = _get_temperature_columns(session_group)
    if not temp_cols:
        return False, ["No temperature columns found"], debug
    temps_df, active_cols = _get_active_temperature_columns(session_group, temp_cols)
    if not active_cols:
        return False, ["No active temperature columns found"], debug

    tonnage_col = None
    if 'TONNAGE DISPLAY' in session_group.columns:
        tonnage_col = 'TONNAGE DISPLAY'
    elif 'TONNAGE' in session_group.columns:
        tonnage_col = 'TONNAGE'

    for idx, cond in enumerate(conditions):
        cond_key = f"{idx}:{cond.get('type')}"
        met = False
        max_duration_achieved = 0.0

        if cond['type'] in ['temperature', 'soak']:
            if not cond.get('range'):
                unmet_reasons.append("Temperature requirement missing range")
                debug[cond_key] = {"met": False, "reason": "missing_range"}
                continue

            if cond['type'] == 'soak':
                # Soak is evaluated on the average of active TCs (ignoring <=0 artifacts)
                try:
                    temp_matrix = temps_df[active_cols].mask(temps_df[active_cols] <= 0)
                    avg_temp = temp_matrix.mean(axis=1, skipna=True)
                    in_range = (avg_temp >= cond['range'][0]) & (avg_temp <= cond['range'][1])
                except Exception:
                    per_col_ok = (temps_df[active_cols] >= cond['range'][0]) & (temps_df[active_cols] <= cond['range'][1])
                    in_range = per_col_ok.all(axis=1)
            else:
                per_col_ok = (temps_df[active_cols] >= cond['range'][0]) & (temps_df[active_cols] <= cond['range'][1])
                in_range = per_col_ok.all(axis=1)

            if cond.get('duration'):
                if cond['type'] == 'soak':
                    max_duration_achieved = _max_continuous_minutes(in_range, time_index)
                    met = max_duration_achieved >= float(cond['duration'])
                else:
                    try:
                        min_required = float(cond['duration'][0]) if isinstance(cond['duration'], tuple) else float(cond['duration'])
                    except Exception:
                        min_required = float(cond['duration'][0])
                    max_duration_achieved = _max_continuous_minutes(in_range, time_index)
                    met = max_duration_achieved >= min_required
            else:
                met = bool(in_range.any())

            if not met:
                if cond.get('duration'):
                    if cond['type'] == 'soak':
                        unmet_reasons.append(
                            f"Soak not met: need {cond['duration']} min at {cond['range'][0]}-{cond['range'][1]}°F (max {max_duration_achieved:.1f} min)"
                        )
                    else:
                        unmet_reasons.append(
                            f"Temp hold not met: need {cond['duration']} min at {cond['range'][0]}-{cond['range'][1]}°F (max {max_duration_achieved:.1f} min)"
                        )
                else:
                    actual_min = temps_df[active_cols].min().min()
                    actual_max = temps_df[active_cols].max().max()
                    unmet_reasons.append(
                        f"Heat-to not met: need {cond['range'][0]}-{cond['range'][1]}°F (actual {actual_min:.0f}-{actual_max:.0f}°F)"
                    )

        elif cond['type'] == 'tonnage':
            if not tonnage_col:
                unmet_reasons.append("Tonnage column missing")
                debug[cond_key] = {"met": False, "reason": "missing_tonnage_col"}
                continue

            tonnage = clean_numeric_column(session_group[tonnage_col])
            tonnage_ok = (tonnage >= cond['tons_range'][0]) & (tonnage <= cond['tons_range'][1])

            temp_ok = None
            if 'TOOL TEMP' in session_group.columns:
                try:
                    tool_temp = clean_numeric_column(session_group['TOOL TEMP'])
                    if tool_temp.notna().any():
                        temp_ok = (tool_temp >= cond['temp_range'][0]) & (tool_temp <= cond['temp_range'][1])
                except Exception:
                    temp_ok = None

            if temp_ok is None:
                per_col_temp_ok = (temps_df[active_cols] >= cond['temp_range'][0]) & (temps_df[active_cols] <= cond['temp_range'][1])
                temp_ok = per_col_temp_ok.all(axis=1)

            in_range = tonnage_ok & temp_ok

            if cond.get('duration'):
                try:
                    min_required = float(cond['duration'][0]) if isinstance(cond['duration'], tuple) else float(cond['duration'])
                except Exception:
                    min_required = float(cond['duration'][0])
                max_duration_achieved = _max_continuous_minutes(in_range, time_index)
                met = max_duration_achieved >= min_required
            else:
                met = bool(in_range.any())

            if not met:
                if cond.get('duration'):
                    unmet_reasons.append(
                        f"Tonnage+Temp hold not met: need {cond['duration']} min at {cond['tons_range'][0]:.1f}-{cond['tons_range'][1]:.1f} tons and {cond['temp_range'][0]}-{cond['temp_range'][1]}°F (max {max_duration_achieved:.1f} min)"
                    )
                else:
                    pressing_tonnage = tonnage[tonnage > get_pressing_tonnage_threshold(press_number)]
                    actual_tonnage = pressing_tonnage.median() if len(pressing_tonnage) > 0 else 0
                    unmet_reasons.append(
                        f"Tonnage+Temp not met: need {cond['tons_range'][0]:.1f}-{cond['tons_range'][1]:.1f} tons and {cond['temp_range'][0]}-{cond['temp_range'][1]}°F (actual tonnage {actual_tonnage:.1f})"
                    )
        else:
            unmet_reasons.append(f"Unknown condition type: {cond.get('type')}")

        debug[cond_key] = {"met": met, "max_duration": max_duration_achieved}

    return len(unmet_reasons) == 0, unmet_reasons, debug


def detect_press_open_time(cycle_df, tonnage_col, press_number=None, minutes_required=3):
    """Detect when the press cycle is complete based on temperature ramp-down.
    
    Cycle is complete when ALL active TCs drop below 399°F after having been hot.
    This indicates the programmed ramp-down finished and operator can open the press.
    Tonnage state is irrelevant - tonnage can still be applied during ramp-down.
    """
    try:
        if cycle_df.empty:
            return None

        temp_cols = _get_temperature_columns(cycle_df)
        if not temp_cols:
            return None
            
        temps, active_cols = _get_active_temperature_columns(cycle_df, temp_cols)
        if not active_cols:
            return None

        max_active_temp = temps[active_cols].max(axis=1).fillna(0)
        cool_mask = max_active_temp < TC_TEMP_THRESHOLD

        # Cycle complete = was hot, now all TCs below 399°F sustained
        had_hot = False
        time_index = cycle_df.index
        current_duration = 0.0
        
        for i in range(1, len(cool_mask)):
            if max_active_temp.iloc[i] >= TC_TEMP_THRESHOLD:
                had_hot = True
                current_duration = 0.0
                continue

            if not had_hot:
                current_duration = 0.0
                continue

            # Was hot, now cool - accumulate duration
            if cool_mask.iloc[i]:
                delta = (time_index[i] - time_index[i - 1]).total_seconds() / 60
                if delta <= 1.5:
                    current_duration += delta
                else:
                    current_duration = 0.0
            else:
                current_duration = 0.0

            if current_duration >= minutes_required:
                return time_index[i]

        # If file ends while cool and we were hot, cycle is complete
        # (data logging stops when operator opens press)
        if had_hot and cool_mask.iloc[-1]:
            return time_index[-1]

        return None
    except Exception:
        return None


def compute_soak_minutes(cycle_df, target_temp_range):
    """Compute max continuous minutes where ALL active TCs are within the target temperature band."""
    try:
        if cycle_df.empty or not target_temp_range:
            return None

        temp_cols = _get_temperature_columns(cycle_df)
        if not temp_cols:
            return None

        temps_df, active_cols = _get_active_temperature_columns(cycle_df, temp_cols)
        if not active_cols:
            return None

        low, high = target_temp_range
        all_tc_ok = ((temps_df[active_cols] >= low) & (temps_df[active_cols] <= high)).all(axis=1)
        if not all_tc_ok.any():
            return 0.0

        return _max_continuous_minutes(all_tc_ok, cycle_df.index)
    except Exception:
        return None


def validate_conditions_with_duration(session_group, conditions, press_number=None):
    """Run the full pass/fail validation for a completed cycle.

    Similar to evaluate_conditions_progress() but returns a simple Pass/Fail
    status string and a list of human-readable failure descriptions. This is
    the function used to generate the final status for the output report.

    For each condition from the spec sheet:
      - Temperature: Check that all active TCs were in the target range.
        If a hold duration is specified, verify it was sustained continuously.
      - Tonnage: Check that the correct total tonnage was applied at the correct
        temperature. Uses TOOL TEMP column if available, otherwise falls back
        to TC averages.

    Returns:
        status (str): "Pass" or "Fail"
        failures (list[str]): Empty for Pass; contains failure descriptions for Fail
    """
    session_group = session_group.copy()
    status = "Pass"
    failures = []
    time_index = session_group.index
    
    if press_number is None:
        try:
            if 'PRESS' in session_group.columns and len(session_group) > 0:
                press_number = int(float(session_group['PRESS'].iloc[0]))
        except Exception:
            press_number = None
    
    # Clean ALL numeric columns first
    for col in session_group.columns:
        if col not in ['Date', 'TimeStr', 'PART#', 'SCREENNO']:
            try:
                session_group[col] = clean_numeric_column(session_group[col])
            except Exception:
                continue

    for cond in conditions:
        if cond['type'] == 'temperature':
            temp_cols = [c for c in session_group.columns if 'TEMP' in c.upper() or 'TC' in c.upper()]
            if temp_cols:
                temps_df, active_cols = _get_active_temperature_columns(session_group, temp_cols)
                if not active_cols:
                    failures.append("No active temperature columns found")
                    status = "Fail"
                    continue

                if cond['range']:
                    per_col_ok = (temps_df[active_cols] >= cond['range'][0]) & (temps_df[active_cols] <= cond['range'][1])
                    in_range = per_col_ok.all(axis=1)
                    expected_range = cond['range']
                else:
                    in_range = temps_df[active_cols].notna().all(axis=1)
                    expected_range = None

                actual_min = temps_df[active_cols].min().min()
                actual_max = temps_df[active_cols].max().max()
            else:
                failures.append("No temperature columns found")
                status = "Fail"
                continue

        elif cond['type'] == 'tonnage':
            tonnage_col = None
            if 'TONNAGE DISPLAY' in session_group.columns:
                tonnage_col = 'TONNAGE DISPLAY'
            elif 'TONNAGE' in session_group.columns:
                tonnage_col = 'TONNAGE'
            if tonnage_col:
                # Explicitly clean tonnage (handles UTF-16 artifacts)
                tonnage = clean_numeric_column(session_group[tonnage_col])
                tonnage_ok = (tonnage >= cond['tons_range'][0]) & (tonnage <= cond['tons_range'][1])

                temp_cols = [c for c in session_group.columns if 'TEMP' in c.upper() or 'TC' in c.upper()]
                if temp_cols:
                    temps_df, active_cols = _get_active_temperature_columns(session_group, temp_cols)
                else:
                    temps_df = pd.DataFrame(index=session_group.index)
                    active_cols = []

                temp_ok = pd.Series(True, index=session_group.index)
                if cond.get('temp_range') is not None:
                    used_tool_temp = False
                    if 'TOOL TEMP' in session_group.columns:
                        try:
                            tool_temp = clean_numeric_column(session_group['TOOL TEMP'])
                            if tool_temp.notna().any():
                                temp_ok = (tool_temp >= cond['temp_range'][0]) & (tool_temp <= cond['temp_range'][1])
                                used_tool_temp = True
                        except Exception:
                            used_tool_temp = False

                    if not used_tool_temp:
                        if not active_cols:
                            failures.append("No active temperature columns found for tonnage validation")
                            status = "Fail"
                            continue
                        per_col_temp_ok = (temps_df[active_cols] >= cond['temp_range'][0]) & (temps_df[active_cols] <= cond['temp_range'][1])
                        temp_ok = per_col_temp_ok.all(axis=1)

                in_range = tonnage_ok & temp_ok
                # Get actual max tonnage (excluding zeros - we want the pressing tonnage)
                pressing_tonnage = tonnage[tonnage > get_idle_tonnage_threshold(press_number)]  # Filter out idle (consistent with cycle detection)
                if len(pressing_tonnage) > 0:
                    actual_tonnage = pressing_tonnage.median()  # Use median to avoid outliers
                else:
                    actual_tonnage = 0
                expected_range = cond['tons_range']
            else:
                failures.append("Tonnage column missing")
                status = "Fail"
                continue

        if cond.get('duration'):
            max_duration_achieved = _max_continuous_minutes(in_range, time_index)
            try:
                min_required = float(cond['duration'][0]) if isinstance(cond['duration'], tuple) else float(cond['duration'])
            except Exception:
                min_required = float(cond['duration'][0])

            if max_duration_achieved < min_required:
                status = "Fail"
                if cond['type'] == 'temperature' and cond.get('range'):
                    failures.append(f"Temp hold failed: needed {cond['duration']} min at {cond['range'][0]}-{cond['range'][1]}°F, achieved {max_duration_achieved:.1f} min")
                elif cond['type'] == 'tonnage' and cond.get('temp_range') is not None:
                    failures.append(
                        f"Tonnage+Temp hold failed: needed {cond['duration']} min at {cond['tons_range'][0]:.1f}-{cond['tons_range'][1]:.1f} tons and {cond['temp_range'][0]}-{cond['temp_range'][1]}°F, achieved {max_duration_achieved:.1f} min"
                    )
                else:
                    failures.append(f"{cond['type']} not sustained for {cond['duration']} min (achieved {max_duration_achieved:.1f} min)")
        else:
            if not in_range.any():
                status = "Fail"
                if cond['type'] == 'tonnage':
                    if cond.get('temp_range') is not None:
                        failures.append(
                            f"Tonnage+Temp: expected {expected_range[0]:.1f}-{expected_range[1]:.1f} tons at {cond['temp_range'][0]}-{cond['temp_range'][1]}°F, actual {actual_tonnage:.1f} tons"
                        )
                    else:
                        failures.append(f"Tonnage: expected {expected_range[0]:.1f}-{expected_range[1]:.1f} tons, actual {actual_tonnage:.1f} tons")
                elif cond['type'] == 'temperature' and expected_range:
                    failures.append(f"Temp: expected {expected_range[0]}-{expected_range[1]}°F, actual range {actual_min:.0f}-{actual_max:.0f}°F")

    return status, failures


# ============================================================================
# CYCLE DETECTION ENGINE
# ============================================================================
# This is the heart of the processor. It takes a full day's data file and
# splits it into individual production cycles (one per part pressing).
#
# The key challenge: a single .txt file may contain multiple press runs
# back-to-back, plus idle time between them. We need to:
#   1. Figure out where one cycle ends and the next begins
#   2. Discard idle/waste data between cycles
#   3. Handle part number changes as cycle boundaries
#   4. Only keep cycles that actually had pressing (not test ramps or aborts)

def identify_columns(df):
    """Auto-detect which DataFrame columns are thermocouples and which is tonnage.

    Different press models use different column naming:
      - Press 5 (15-col format): 'TONNAGE DISPLAY' and zone TCs like Z1TC1A2A
      - Standard presses (11-col format): 'TONNAGE' and simple TC1, TC2, TC3, TC4

    Returns:
        tc_cols: List of thermocouple column names found
        tonnage_col: Name of the tonnage column, or None if not found
    """
    tc_cols = []
    tonnage_col = None

    # Press 5 format: has "TONNAGE DISPLAY" and zone-based TC names
    if 'TONNAGE DISPLAY' in df.columns:
        tonnage_col = 'TONNAGE DISPLAY'
        for col in df.columns:
            if re.match(r'Z\d+TC', str(col), re.IGNORECASE):
                tc_cols.append(col)

    # Standard press format: has "TONNAGE" and simple TC names
    elif 'TONNAGE' in df.columns:
        tonnage_col = 'TONNAGE'
        for col in df.columns:
            if re.match(r'TC\d+$', str(col), re.IGNORECASE):
                tc_cols.append(col)

    return tc_cols, tonnage_col


def calculate_tc_average(df, tc_cols):
    """Compute the row-wise average of all thermocouple columns.

    Used for cycle boundary detection — when the average TC drops below
    the threshold, the cycle is cooling down. Invalid readings (zero,
    above 1500 F, below -50 F) are excluded from the average.

    Returns a Series with the mean TC temperature for each timestamp.
    """
    if not tc_cols:
        return pd.Series(0, index=df.index)

    tc_data = df[tc_cols].apply(clean_numeric_column)
    # Mask sensor errors: zero = disconnected, >1500 = impossible, <-50 = noise
    tc_data = tc_data.mask((tc_data == 0) | (tc_data > MAX_REALISTIC_TEMP_F) | (tc_data < -50))

    tc_avg = tc_data.mean(axis=1).fillna(0)

    return tc_avg


# --- Per-Press Tonnage Thresholds ---
# Different presses have different idle tonnage baselines due to their
# hydraulic systems. These functions return the right threshold for each.

def get_idle_tonnage_threshold(press_number):
    """Return the tonnage value below which the press is considered 'idle' (not pressing).
    Press 5 and 7 idle above zero; all others idle at exactly zero."""
    if press_number == 5:
        return PRESS5_IDLE_TONNAGE_DISPLAY
    if press_number == 7:
        return PRESS7_IDLE_TONNAGE
    return TONNAGE_ZERO


def get_pressing_tonnage_threshold(press_number):
    """Return the minimum tonnage that counts as 'actively pressing' (for validation).
    Must be above the idle threshold — Press 7 needs a higher bar (11 tons)."""
    if press_number == 7:
        return max(get_idle_tonnage_threshold(press_number), PRESS7_MIN_PRESSING_TONNAGE)
    return get_idle_tonnage_threshold(press_number)


def get_cycle_pressing_threshold(press_number):
    """Return the minimum tonnage for a data segment to qualify as a production cycle.
    Higher than pressing threshold — Press 7 needs 20+ tons to count as a real cycle."""
    pressing_threshold = get_pressing_tonnage_threshold(press_number)
    if press_number == 7:
        return max(pressing_threshold, PRESS7_MIN_CYCLE_TONNAGE)
    return pressing_threshold


def split_into_cycles(df, tc_cols, tonnage_col, press_number=None):
    """Split a full-day DataFrame into individual production cycles.

    This is the main cycle-detection algorithm. It identifies where one press
    run ends and the next begins by looking for "boundary" regions — sustained
    periods (>= 5 minutes) where BOTH:
      - Tonnage is at/below idle threshold (press not applying force)
      - All active TCs are below 399 F OR all readings are zero

    Cycle boundaries are also created when the part number changes (operator
    loaded a different program).

    The algorithm:
      1. Build a boolean "boundary mask" for every row (idle + cool = boundary)
      2. Find sustained boundary runs (>= 5 min continuous)
      3. Combine with part-number-change points to create cut points
      4. Split the data at cut points into segments
      5. Discard segments that are entirely boundary (idle waste between cycles)
      6. Discard segments that never had pressing tonnage (aborted or test runs)

    Returns:
        List of (start_idx, end_idx, part_number) tuples, where start_idx and
        end_idx are integer positions within df for each valid production cycle.
    """
    if df.empty:
        return []
    
    idle_threshold = get_idle_tonnage_threshold(press_number)

    # Calculate TC average
    tc_avg = calculate_tc_average(df, tc_cols)
    
    # Get tonnage series (convert to numeric using clean function)
    tonnage = clean_numeric_column(df[tonnage_col])

    temps_df = None
    temps_raw = None
    active_cols = []
    if tc_cols:
        try:
            temps_raw = df[tc_cols].apply(clean_numeric_column)
            temps_raw = temps_raw.mask((temps_raw > MAX_REALISTIC_TEMP_F) | (temps_raw < -50))
            temps_raw = temps_raw.fillna(0)
            temps_df = temps_raw.mask(temps_raw == 0)
            active_cols = [c for c in tc_cols if temps_df[c].notna().any()]
        except Exception:
            temps_df = None
            temps_raw = None
            active_cols = []

    boundary_mask = pd.Series(False, index=df.index)
    if temps_df is not None and active_cols:
        # boundary is idle-tonnage and either:
        # - all active TCs are present and <= threshold
        # - OR all TC values are exactly 0 (idle/cool state on some loggers)
        temps_ok = temps_df[active_cols].notna().all(axis=1) & (temps_df[active_cols] <= TC_TEMP_THRESHOLD).all(axis=1)
        all_zero_ok = pd.Series(False, index=df.index)
        try:
            if temps_raw is not None:
                all_zero_ok = (temps_raw.fillna(0) == 0).all(axis=1)
        except Exception:
            all_zero_ok = pd.Series(False, index=df.index)

        boundary_mask = (tonnage <= idle_threshold) & (temps_ok | all_zero_ok)
    
    # Check for unexpected NaN values - should not happen with PLC data
    nan_count = tonnage.isna().sum()
    if nan_count > 0:
        # Get sample of raw values that became NaN for debugging
        nan_mask = tonnage.isna()
        raw_nan_samples = df.loc[nan_mask, tonnage_col].head(3).tolist()
        safe_print(f"  WARNING: {nan_count} unexpected NaN tonnage values detected!")
        safe_print(f"    Raw values that failed to parse: {raw_nan_samples}")
    
    # Get part numbers - clean null bytes
    part_numbers = df['PART#'].astype(str).str.replace('\x00', '', regex=False).str.replace('\0', '', regex=False).str.strip()
    
    # Identify sustained idle+cool runs (>= 5 minutes) that represent a true part boundary.
    boundary_runs = []
    try:
        minutes_required = 5.0
        run_start = None
        run_end = None
        run_minutes = 0.0
        time_index = df.index

        for i in range(len(boundary_mask)):
            is_boundary = bool(boundary_mask.iloc[i])
            if not is_boundary:
                if run_start is not None and run_minutes >= minutes_required:
                    boundary_runs.append((run_start, run_end))
                run_start = None
                run_end = None
                run_minutes = 0.0
                continue

            if run_start is None:
                run_start = i
                run_end = i
                run_minutes = 0.0
                continue

            delta = (time_index[i] - time_index[i - 1]).total_seconds() / 60.0
            if delta <= 1.5:
                run_minutes += max(delta, 0.0)
                run_end = i
            else:
                if run_start is not None and run_minutes >= minutes_required:
                    boundary_runs.append((run_start, run_end))
                run_start = i
                run_end = i
                run_minutes = 0.0

        if run_start is not None and run_minutes >= minutes_required:
            boundary_runs.append((run_start, run_end))
    except Exception:
        boundary_runs = []

    cut_points = set([0])
    n = len(df)

    # Part-number change boundaries
    try:
        part_change_idxs = (part_numbers != part_numbers.shift(1)).fillna(False)
        for idx in part_change_idxs[part_change_idxs].index:
            pos = df.index.get_loc(idx)
            if isinstance(pos, (int,)) and pos > 0:
                cut_points.add(pos)
    except Exception:
        pass

    # Sustained idle+cool boundaries: cut at start of boundary and after boundary ends
    for start_i, end_i in boundary_runs:
        if 0 < start_i < n:
            cut_points.add(start_i)
        if 0 <= end_i + 1 < n:
            cut_points.add(end_i + 1)

    cut_points = sorted(cp for cp in cut_points if 0 <= cp < n)
    if not cut_points:
        return []
    if cut_points[0] != 0:
        cut_points = [0] + cut_points

    cycles = []
    for k in range(len(cut_points)):
        start_idx = cut_points[k]
        end_idx = (cut_points[k + 1] - 1) if (k + 1) < len(cut_points) else (n - 1)
        if end_idx < start_idx:
            continue

        seg_boundary = False
        try:
            seg_boundary = bool(boundary_mask.iloc[start_idx:end_idx + 1].all())
        except Exception:
            seg_boundary = False
        if seg_boundary:
            continue

        had_pressing = False
        try:
            pressing_threshold = max(get_pressing_tonnage_threshold(press_number), MIN_CYCLE_PRESSING_TONNAGE)
            had_pressing = bool((tonnage.iloc[start_idx:end_idx + 1] > pressing_threshold).any())
        except Exception:
            had_pressing = False
        if not had_pressing:
            continue

        part_number = part_numbers.iloc[start_idx]
        cycles.append((start_idx, end_idx, part_number))

    return cycles


# ============================================================================
# OUTPUT GENERATION: FAILURE REPORTS, STATUS FILES, AND AUDIT LOGS
# ============================================================================

def create_failure_report(output_folder, press_number, part_number, first_date, pass_status, failure_details, cycle_df, conditions=None, num_tools=None):
    """Create (or remove) FAILURE_REPORT.txt in the cycle's output folder.

    If the cycle passed or is still in progress, any existing failure report
    is deleted (a previous run may have failed but now passes on re-evaluation).

    If the cycle failed, writes a detailed human-readable report containing:
      - Basic info (press, part, date, status)
      - Cycle time range and duration
      - Specific failure reasons with expected vs. actual values
      - Temperature and tonnage data summaries
      - Expected conditions from the spec sheet (if available)

    This file is intended for quality engineers to quickly diagnose why a
    press cycle didn't meet specifications.
    """
    report_path = os.path.join(output_folder, "FAILURE_REPORT.txt")

    if pass_status in ["Pass", "In Progress"]:
        try:
            if os.path.exists(report_path):
                os.remove(report_path)
            for f in os.listdir(output_folder):
                if f.startswith("FAILURE_REPORT_ARCHIVED_"):
                    try:
                        os.remove(os.path.join(output_folder, f))
                    except Exception:
                        pass
        except Exception:
            pass
        return  # No report needed for passing jobs
    
    try:
        if not failure_details:
            failure_details = ["Failure reason not available"]

        with open(report_path, 'w') as f:
            f.write("=" * 70 + "\n")
            f.write("                    VALIDATION FAILURE REPORT\n")
            f.write("=" * 70 + "\n\n")
            
            # Basic info
            f.write(f"Press:        Press {press_number}\n")
            f.write(f"Part Number:  {part_number}\n")
            f.write(f"Date:         {first_date}\n")
            f.write(f"Status:       {pass_status}\n")
            f.write(f"Report Time:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            if num_tools:
                f.write(f"Tool Count:   {num_tools}\n")
            f.write("\n")
            
            # Cycle time range
            f.write("-" * 70 + "\n")
            f.write("CYCLE TIME RANGE\n")
            f.write("-" * 70 + "\n")
            f.write(f"Start:        {cycle_df.index[0].strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"End:          {cycle_df.index[-1].strftime('%Y-%m-%d %H:%M:%S')}\n")
            duration_min = (cycle_df.index[-1] - cycle_df.index[0]).total_seconds() / 60
            f.write(f"Duration:     {duration_min:.1f} minutes\n")
            f.write("\n")
            
            # Failure reasons
            f.write("-" * 70 + "\n")
            f.write("FAILURE REASONS - ACTION REQUIRED\n")
            f.write("-" * 70 + "\n")
            for i, detail in enumerate(failure_details, 1):
                f.write(f"  {i}. {detail}\n")
            f.write("\n")
            
            # Actual data summary
            f.write("-" * 70 + "\n")
            f.write("ACTUAL DATA SUMMARY\n")
            f.write("-" * 70 + "\n")
            
            # Temperature summary
            temp_cols = [c for c in cycle_df.columns if 'TC' in c.upper() and c not in ['SCREENNO']]
            if temp_cols:
                f.write("\nTemperature (°F):\n")
                for col in temp_cols:
                    temps = clean_numeric_column(cycle_df[col])
                    f.write(f"  {col}: Min={temps.min():.0f}, Max={temps.max():.0f}, Avg={temps.mean():.0f}\n")
                
                # Average of all TCs
                all_temps = cycle_df[temp_cols].apply(clean_numeric_column).mean(axis=1)
                f.write(f"  AVERAGE: Min={all_temps.min():.0f}, Max={all_temps.max():.0f}, Avg={all_temps.mean():.0f}\n")
            
            # Tonnage summary
            tonnage_col = 'TONNAGE DISPLAY' if 'TONNAGE DISPLAY' in cycle_df.columns else 'TONNAGE'
            if tonnage_col in cycle_df.columns:
                tonnage = clean_numeric_column(cycle_df[tonnage_col])
                pressing_tonnage = tonnage[tonnage > get_idle_tonnage_threshold(press_number)]
                f.write(f"\nTonnage:\n")
                f.write(f"  Min:     {tonnage.min():.1f} tons\n")
                f.write(f"  Max:     {tonnage.max():.1f} tons\n")
                if len(pressing_tonnage) > 0:
                    f.write(f"  Pressing Avg: {pressing_tonnage.mean():.1f} tons\n")
                    f.write(f"  Pressing Med: {pressing_tonnage.median():.1f} tons\n")
            
            f.write("\n")
            
            # Expected conditions (if available)
            if conditions:
                f.write("-" * 70 + "\n")
                f.write("EXPECTED CONDITIONS (FROM PROGRAM SPEC)\n")
                f.write("-" * 70 + "\n")
                for cond in conditions:
                    if cond['type'] == 'temperature':
                        if cond.get('range'):
                            f.write(f"  Temperature: {cond['range'][0]}°F to {cond['range'][1]}°F")
                        if cond.get('duration'):
                            f.write(f" for {cond['duration']} min")
                        f.write("\n")
                    elif cond['type'] == 'tonnage':
                        f.write(f"  Tonnage: {cond['tons_range'][0]:.1f} to {cond['tons_range'][1]:.1f} tons\n")
                f.write("\n")
            
            # Footer
            f.write("=" * 70 + "\n")
            f.write("Review the chart.pdf and raw data file for full details.\n")
            f.write("=" * 70 + "\n")
        
        safe_print(f"  Created failure report: {report_path}")
    except Exception as e:
        safe_print(f"  WARNING: Could not create failure report: {e}")


def _write_status_file(output_folder, press_number, part_number, first_date, pass_status, failure_details):
    """Write a concise STATUS.txt file summarizing the cycle result.
    Simpler than the failure report — just the key facts for quick scanning."""
    try:
        status_path = os.path.join(output_folder, "STATUS.txt")
        with open(status_path, "w", encoding="utf-8") as f:
            f.write(f"Press: {press_number}\n")
            f.write(f"Part Number: {part_number}\n")
            f.write(f"Date: {first_date}\n")
            f.write(f"Status: {pass_status}\n")
            f.write(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            if failure_details:
                f.write("Reasons:\n")
                for i, d in enumerate(failure_details, 1):
                    f.write(f"  {i}. {d}\n")
        return True
    except Exception:
        return False


def _append_results_index(press_number, output_folder, pass_status, failure_details, cycle_df, source_filename):
    """Append one row to the CSV audit log (results_index.csv).

    Two copies are maintained:
      - Per-press: Press_N/results_index.csv (for press-specific queries)
      - Global: results_index_all.csv (for cross-press reporting)

    Each row records the cycle's folder, status, time range, failure reasons,
    and whether chart/report files were generated. Thread-safe via lock.
    """
    try:
        press_root = os.path.join(OUTPUT_BASE, f"Press_{press_number}")
        os.makedirs(press_root, exist_ok=True)

        folder_name = os.path.basename(output_folder.rstrip("\\/"))
        chart_exists = os.path.isfile(os.path.join(output_folder, "chart.pdf"))
        report_exists = os.path.isfile(os.path.join(output_folder, "FAILURE_REPORT.txt"))

        try:
            start_ts = cycle_df.index[0].strftime('%Y-%m-%d %H:%M:%S')
            end_ts = cycle_df.index[-1].strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            start_ts = ""
            end_ts = ""

        reasons = " | ".join([str(x) for x in (failure_details or [])])

        row = {
            "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "press": press_number,
            "folder": folder_name,
            "output_folder": output_folder,
            "status": pass_status,
            "start": start_ts,
            "end": end_ts,
            "reasons": reasons,
            "failure_report": "1" if report_exists else "0",
            "chart_pdf": "1" if chart_exists else "0",
            "source_file": source_filename or "",
        }

        headers = list(row.keys())
        targets = [
            os.path.join(press_root, "results_index.csv"),
            os.path.join(OUTPUT_BASE, "results_index_all.csv"),
        ]

        with _results_index_lock:
            for target in targets:
                write_header = not os.path.exists(target)
                with open(target, "a", encoding="utf-8", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=headers)
                    if write_header:
                        w.writeheader()
                    w.writerow(row)

        return True
    except Exception:
        return False


# ============================================================================
# MAIN PRESS FILE PROCESSOR
# ============================================================================

def process_press_file_robust(filepath, press_number, program_df=None):
    """Process a single press data file end-to-end.

    This is the top-level function for press files. It orchestrates:
      1. Load and parse the raw data file (handling UTF-16 encoding)
      2. Stitch adjacent day files so cycles spanning midnight aren't cut off
      3. Identify TC and tonnage columns for this press model
      4. Split the data into individual production cycles
      5. For each cycle:
         a. Validate it's a real cycle (reached temp, had pressing, long enough)
         b. Look up the part number in the Excel spec sheet
         c. Extract validation conditions (temp targets, tonnage, hold times)
         d. Auto-detect tool count from observed tonnage if it doesn't match spec
         e. Determine cycle status: Pass / Fail / In Progress
         f. Generate PDF chart, failure report, status file, and audit log entry
         g. Save the cycle's data as a filtered text file

    Returns True if the file was processed without fatal errors.
    """
    try:
        filename = os.path.basename(filepath)
        safe_print(f"Processing: {filename} from Press_{press_number}")

        df = load_press_dataframe(filepath)
        if df is None or df.empty:
            safe_print(f"ERROR: No valid data rows in {filename}")
            return False
        safe_print(f"  Found {len(df)} valid data rows")

        original_file_start = df.index.min()
        original_file_end = df.index.max()

        stitched_df = df
        try:
            press_dir = os.path.dirname(filepath)
            base = os.path.basename(filepath)

            # Include same-date files (common for noon/midday rollovers) + +/-1 day
            datecodes = set()
            m = list(re.finditer(r"\d{6}", base))
            if m:
                cur_code = m[-1].group(0)
                datecodes.add(cur_code)
                cur_date = _parse_yymmdd_datecode(cur_code)
                if cur_date is not None:
                    datecodes.add(_format_yymmdd_datecode(cur_date + timedelta(days=-1)))
                    datecodes.add(_format_yymmdd_datecode(cur_date + timedelta(days=1)))

            desired_start = original_file_start - timedelta(minutes=PREPEND_PREV_FILE_MINUTES)
            desired_end = original_file_end + timedelta(minutes=APPEND_NEXT_FILE_MINUTES)

            candidates = []
            for fn in os.listdir(press_dir):
                if not fn.lower().endswith('.txt'):
                    continue
                if fn == base:
                    continue
                if datecodes and (not any(dc in fn for dc in datecodes)):
                    continue
                full = os.path.join(press_dir, fn)
                candidates.append(full)

            # Deterministic stitch order
            candidates.sort(key=lambda p: (os.path.getmtime(p), os.path.getsize(p)))

            for p in candidates:
                other_df = load_press_dataframe(p)
                if other_df is None or other_df.empty:
                    continue
                other_slice = other_df.loc[(other_df.index >= desired_start) & (other_df.index <= desired_end)]
                if other_slice.empty:
                    continue
                stitched_df = pd.concat([stitched_df, other_slice], axis=0)
                safe_print(f"  Stitched {len(other_slice)} rows from {os.path.basename(p)}")
        except Exception as _stitch_err:
            safe_print(f"  WARNING: Stitch failed: {_stitch_err}")
            stitched_df = df

        if stitched_df is not df:
            stitched_df = stitched_df[~stitched_df.index.duplicated(keep='last')].sort_index()
            df = stitched_df

        # NEW: Identify columns and split into cycles
        tc_cols, tonnage_col = identify_columns(df)
        
        if not tonnage_col:
            safe_print(f"ERROR: Could not find tonnage column in {filename}")
            safe_print(f"  Available columns: {df.columns.tolist()}")
            return False
        
        safe_print(f"  TC columns: {tc_cols}")
        safe_print(f"  Tonnage column: {tonnage_col}")
        
        # Split into production cycles
        cycles = split_into_cycles(df, tc_cols, tonnage_col, press_number=press_number)
        
        safe_print(f"  Found {len(cycles)} production cycles")
        
        # Process each cycle
        cycles_processed = 0
        for cycle_idx, (start_idx, end_idx, part_number) in enumerate(cycles, 1):
            try:
                try:
                    cycle_start_time = df.index[start_idx]
                    if cycle_start_time < original_file_start or cycle_start_time > original_file_end:
                        continue
                except Exception:
                    pass

                # Extract cycle data and expand window around pressing
                cycle_df = expand_cycle_window_around_pressing(df, start_idx, end_idx, tonnage_col)
                
                if cycle_df.empty:
                    continue
                
                # Skip bogus cycles that never got hot (temps never reached 399°F)
                try:
                    cycle_tc_cols = [c for c in tc_cols if c in cycle_df.columns]
                    if cycle_tc_cols:
                        cycle_temps = cycle_df[cycle_tc_cols].apply(clean_numeric_column)
                        valid_tc_cols = [c for c in cycle_tc_cols if cycle_temps[c].max() < 1500 and (cycle_temps[c] > 0).any()]
                        if not valid_tc_cols:
                            continue

                        max_temp_reached = cycle_temps[valid_tc_cols].max().max()
                        if max_temp_reached < CYCLE_VALID_MIN_MAX_TEMP:
                            continue
                    else:
                        continue
                    try:
                        cycle_minutes = (cycle_df.index[-1] - cycle_df.index[0]).total_seconds() / 60.0
                        if cycle_minutes < CYCLE_VALID_MIN_DURATION_MIN:
                            continue
                    except Exception:
                        pass
                except Exception:
                    pass
                
                # Clean part_number
                part_number = str(part_number).replace('\x00', '').replace('\0', '').strip()
                if not part_number:
                    continue

                missing_part_number = False
                pn_int = None
                try:
                    if 'PART#' in cycle_df.columns:
                        pn_series = cycle_df['PART#'].astype(str).str.replace('\x00', '', regex=False).str.replace('\0', '', regex=False).str.strip()
                        pn_series = pd.to_numeric(pn_series, errors='coerce')
                        pn_series = pn_series.dropna()
                        if not pn_series.empty:
                            pn_series = pn_series.astype(int)
                            pn_series = pn_series[pn_series > 0]
                            if not pn_series.empty:
                                pn_int = int(pn_series.mode().iloc[0])
                except Exception:
                    pn_int = None

                if pn_int is None:
                    try:
                        pn_int = int(float(part_number))
                    except (ValueError, TypeError):
                        pn_int = None

                if pn_int is None or pn_int <= 0:
                    missing_part_number = True
                    clean_part_number = "NO_PART"
                else:
                    clean_part_number = str(pn_int)
                
                # Create output folder per cycle
                first_date = cycle_df.index[0].date()
                start_time = cycle_df.index[0].strftime('%H-%M-%S')
                results_folder_name = f"Results_{clean_part_number}_{first_date}_{start_time}"
                output_folder = os.path.join(OUTPUT_BASE, f"Press_{press_number}", results_folder_name)
                
                os.makedirs(output_folder, exist_ok=True)
                
                # Check if job is still in progress
                last_data_time = cycle_df.index[-1]
                time_since_last_data = (datetime.now() - last_data_time).total_seconds() / 60

                try:
                    if (not missing_part_number) and program_df is not None and not program_df.empty:
                        num_tools = get_tool_quantity(pn_int, program_df)
                        raw_steps = program_df.loc[program_df['Program'] == pn_int].iloc[0, 3:].tolist()
                        steps = [s for s in raw_steps if isinstance(s, str) and str(s).strip()]
                        conditions = extract_conditions(steps, num_tools)
                        try:
                            num_tools, conditions = _infer_effective_tool_count_from_tonnage(
                                cycle_df, tonnage_col, press_number, conditions, num_tools
                            )
                        except Exception:
                            pass
                        try:
                            conditions = _widen_tonnage_range_if_close(cycle_df, tonnage_col, press_number, conditions)
                        except Exception:
                            pass
                    else:
                        conditions = None
                        num_tools = None
                except Exception:
                    conditions = None
                    num_tools = None

                tonnage_unreliable = (press_number in TONNAGE_UNRELIABLE_PRESSES)
                if tonnage_unreliable and conditions:
                    try:
                        conditions = [c for c in conditions if c.get('type') != 'tonnage']
                    except Exception:
                        pass

                soak_text = None
                open_time = detect_press_open_time(cycle_df, tonnage_col, press_number=press_number, minutes_required=3)
                cycle_complete = is_cycle_complete(cycle_df, tonnage_col, press_number=press_number)
                if time_since_last_data >= 60:
                    try:
                        idle_threshold = get_idle_tonnage_threshold(press_number)
                        last_tonnage = float(clean_numeric_column(cycle_df[tonnage_col]).fillna(0).iloc[-1])
                        last_tc_avg = float(calculate_tc_average(cycle_df, tc_cols).iloc[-1]) if tc_cols else 0.0
                        if last_tonnage <= idle_threshold or last_tc_avg < TC_TEMP_THRESHOLD:
                            cycle_complete = True
                    except Exception:
                        pass
                evaluation_df = cycle_df
                if open_time is not None:
                    try:
                        evaluation_df = cycle_df.loc[cycle_df.index <= open_time].copy()
                    except Exception:
                        evaluation_df = cycle_df

                if not conditions:
                    pass_status = "Fail" if (open_time is not None or cycle_complete) else "In Progress"
                    failure_details = ["No program data available"]
                else:
                    # Soak metric (chart): achieved / required for first soak step
                    try:
                        soak_cond = next((c for c in conditions if c.get('type') == 'soak' and c.get('range') is not None), None)
                        if soak_cond is not None:
                            achieved = compute_soak_minutes(cycle_df, soak_cond['range'])
                            if achieved is not None:
                                soak_text = f"{achieved:.1f}/{float(soak_cond['duration']):.0f} min @ {soak_cond['range'][0]}-{soak_cond['range'][1]}°F"
                    except Exception:
                        soak_text = None

                    all_met, unmet_reasons, _debug = evaluate_conditions_progress(evaluation_df, conditions, press_number=press_number)

                    if open_time is not None or cycle_complete:
                        if all_met:
                            pass_status = "Pass"
                            failure_details = []
                        else:
                            pass_status = "Fail"
                            if open_time is not None:
                                failure_details = ["Press opened before spec requirements were met"] + unmet_reasons
                            else:
                                failure_details = unmet_reasons
                    else:
                        pass_status = "In Progress"
                        if all_met:
                            failure_details = ["Currently meets spec"]
                        else:
                            failure_details = unmet_reasons

                if tonnage_unreliable and (not missing_part_number):
                    if pass_status == "Fail":
                        pass_status = "In Progress"
                    if not failure_details:
                        failure_details = []
                    if "Tonnage signal unreliable" not in failure_details:
                        failure_details = ["Tonnage signal unreliable"] + failure_details

                if missing_part_number:
                    pass_status = "Fail"
                    failure_details = ["Part number missing or invalid"] + (failure_details or [])

                # Append live recency info for In Progress
                if pass_status == "In Progress" and time_since_last_data < 60:
                    failure_details = [f"Last update: {int(time_since_last_data)} min ago"] + (failure_details or [])
                
                # Create or cleanup failure report
                create_failure_report(output_folder, press_number, clean_part_number if missing_part_number else pn_int, first_date, pass_status, failure_details, cycle_df, conditions, num_tools)

                try:
                    display_part = clean_part_number if missing_part_number else str(pn_int)
                    _write_status_file(output_folder, press_number, display_part, first_date, pass_status, failure_details)
                    _append_results_index(press_number, output_folder, pass_status, failure_details, cycle_df, filename)
                except Exception:
                    pass
                
                # Create chart
                chart_success = create_robust_chart(cycle_df, press_number, clean_part_number if missing_part_number else pn_int, first_date, output_folder, pass_status, failure_details, soak_text=soak_text)
                if not chart_success:
                    safe_print(f"ERROR: Failed to create chart for cycle {cycle_idx}")
                    
                
                # Save FILTERED text file (only this cycle's data)
                cycle_text_path = os.path.join(output_folder, filename)
                try:
                    # Reset index to include Date column in output
                    cycle_output = cycle_df.reset_index(drop=True)
                    cycle_output.to_csv(cycle_text_path, sep='\t', index=False)
                    safe_print(f"  Created cycle {cycle_idx}: {output_folder}")
                    cycles_processed += 1
                except Exception as e:
                    safe_print(f"ERROR: Could not save cycle text file: {e}")
                    continue
                
            except Exception as e:
                safe_print(f"ERROR: Error creating output for cycle {cycle_idx}: {e}")
                continue
        
        safe_print(f"SUCCESS: Processed {filename} ({cycles_processed} cycles created)")
        return True

    except Exception as e:
        safe_print(f"ERROR: Error processing {filepath}: {e}")
        return False


# ============================================================================
# PDF CHART GENERATION
# ============================================================================

def create_robust_chart(df, press_number, part_number, first_date, output_folder, pass_status="Unknown", failure_details=None, soak_text=None):
    """Generate a multi-page PDF chart for one press cycle.

    Page 1: Time-series line chart showing all sensor channels (temperatures,
            tonnage, platen temps) with Pass/Fail/In-Progress status overlay
            and optional soak duration info.

    Page 2: Raw data table with all values for every timestamp, so quality
            engineers can inspect exact readings.

    The chart width scales dynamically based on cycle duration (longer cycles
    get wider charts so the data isn't squeezed). Max width is 50 inches.

    Uses atomic file writes (write to .tmp, then os.replace) to avoid
    corrupting the PDF if the process crashes mid-write. Falls back to
    an alternative filename if chart.pdf is locked (e.g., someone has it
    open in a PDF viewer).
    """
    if failure_details is None:
        failure_details = []
    
    df = df.copy()
    chart_path = os.path.join(output_folder, "chart.pdf")
    tmp_chart_path = chart_path + f".tmp_{os.getpid()}_{int(time.time() * 1000)}"
    try:
        safe_print(f"  Creating press chart: {chart_path}")
        time_span_minutes = (df.index.max() - df.index.min()).total_seconds() / 60
        dynamic_width = min(10 + (time_span_minutes / 60) * 8, 50)

        # Plot available columns
        plot_columns = ['TC1', 'TC2', 'TC3', 'TC4', 'TONNAGE', 'PLATTEN1', 'PLATTEN2', 
                       'TONNAGE DISPLAY', 'TOOL TEMP', 'TOP PLATTEN AVG', 'BOTTOM PLATTEN',
                       'Z1TC1A2A', 'Z1TC3A4A', 'Z2TC1B2B', 'Z2TC3B4B']
        plotted = []

        for col in plot_columns:
            if col in df.columns:
                try:
                    # Use the clean_numeric_column function to handle UTF-16 artifacts
                    series = clean_numeric_column(df[col])
                    if series.notna().any():
                        # Check if series has non-zero values (not just all zeros)
                        if (series.abs() > 0).any():
                            df[col] = series
                            plotted.append(col)
                except Exception as e:
                    safe_print(f"  WARNING: Could not convert column {col}: {e}")
                    continue

        if not plotted:
            safe_print(f"ERROR: No plottable columns found")
            safe_print(f"  Available columns: {df.columns.tolist()}")
            safe_print(f"  Data shape: {df.shape}")
            # Show sample of first few rows to diagnose
            for col in ['TC1', 'TC2', 'TONNAGE', 'TONNAGE DISPLAY']:
                if col in df.columns:
                    sample = df[col].head(3).tolist()
                    safe_print(f"  {col} sample (raw): {sample}")
                    # Also show cleaned version
                    try:
                        cleaned = clean_numeric_column(df[col]).head(3).tolist()
                        safe_print(f"  {col} sample (cleaned): {cleaned}")
                    except:
                        pass
            return False

        # Create PDF with chart and data table
        with PdfPages(tmp_chart_path) as pdf:
            # Page 1: Chart
            fig, ax = plt.subplots(figsize=(max(dynamic_width, 6), 6), dpi=100)

            for col in plotted:
                ax.plot(df.index, df[col], label=col, linewidth=1)

            ax.set_xlabel('Time')
            ax.set_ylabel('Values')
            ax.set_title(f'Press_{press_number} - {part_number}', fontsize=12)
            ax.grid(True, alpha=0.3)

            # Time formatting
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            fig.autofmt_xdate(rotation=30)

            # Status text
            if pass_status == "In Progress":
                status_color = 'orange'
                status_text = "Status: In Progress"
                if failure_details:
                    status_text += "\n" + "; ".join(failure_details)
            else:
                is_pass = (pass_status == "Pass")
                status_color = 'green' if is_pass else 'red'
                status_text = f"Status: {'Pass' if is_pass else 'Fail'}"
                if not is_pass and failure_details:
                    status_text += "\nReason: " + "; ".join(failure_details)

            if soak_text:
                status_text += f"\nSoak: {soak_text}"
            ax.text(0.75, 1.0, status_text, transform=ax.transAxes, ha='left', va='bottom', fontsize=10, color=status_color)

            # Legend
            ax.legend(plotted, loc='upper right', fontsize=8)

            pdf.savefig(fig, bbox_inches='tight')
            plt.close(fig)

            # Page 2: Data table
            df_copy = df.copy()
            df_copy['TimeStr'] = df_copy.index.strftime('%H:%M')
            table_data = df_copy[['TimeStr'] + plotted].copy().astype(str)
            table_data.reset_index(drop=True, inplace=True)
            rows, cols = table_data.shape
            table_fig_height = max(2, rows * 0.25)
            table_fig, table_ax = plt.subplots(figsize=(dynamic_width, table_fig_height), dpi=100)
            table_ax.axis('off')
            table = table_ax.table(cellText=table_data.values, colLabels=table_data.columns, loc='center', cellLoc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(8)
            table.scale(1, 1.5)
            pdf.savefig(table_fig)
            plt.close(table_fig)

        try:
            os.replace(tmp_chart_path, chart_path)
        except PermissionError:
            alt_path = os.path.join(output_folder, f"chart_reprocess_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
            try:
                os.replace(tmp_chart_path, alt_path)
                safe_print(f"  WARNING: chart.pdf locked; wrote {os.path.basename(alt_path)} instead")
            except Exception:
                safe_print(f"  WARNING: chart.pdf locked; leaving temp chart at {tmp_chart_path}")
        except Exception:
            pass

        safe_print(f"  Press chart created successfully")
        return True
    except Exception as e:
        safe_print(f"ERROR creating press chart: {e}")
        import traceback
        traceback.print_exc()
        plt.close('all')
        try:
            if os.path.isfile(tmp_chart_path):
                os.remove(tmp_chart_path)
        except Exception:
            pass
        return False


# ============================================================================
# OVEN PROCESSING FUNCTIONS
# ============================================================================
# Ovens (furnaces) have simpler data than presses — just temperature over time.
# Validation checks ramp rates, soak temperatures, and hold durations against
# the OvenCyclesMaverick.xlsx spec sheet.

def parse_oven_condition(cond_str):
    """Parse a single oven step description string into a structured dict.

    The oven spec sheet has free-text step descriptions like:
      "Ramp Up: 5.0°F/min +10F/-10F to 350°F"
      "Soak: 2h +5min/-5min at 350°F +15F/-15F"
      "Soak: Hold at 350°F +15F/-15F"

    Returns a dict with 'type' (ramp/soak/hold), target values, and tolerances,
    or None if the string doesn't match any known pattern.
    """
    ramp = re.match(r"Ramp (Up|Down): ([\d.]+)[°]?F/min \+([\d.]+)F/-([\d.]+)F to (\d+)[°]?F", cond_str)
    soak = re.match(r"Soak: ([\dh]+) \+(\d+)min/-(\d+)min at (\d+)[°]?F(?: \+([\d.]+)F/-([\d.]+)F)?", cond_str)
    hold = re.match(r"Soak: Hold at (\d+)[°]?F(?: \+([\d.]+)F/-([\d.]+)F)?", cond_str)
    if ramp:
        direction, rate, plus_tol, minus_tol, target = ramp.groups()
        return {"type": "ramp", "direction": direction, "rate": float(rate), "tolerance": (float(minus_tol), float(plus_tol)), "target": int(target)}
    if soak:
        duration, plus_min, minus_min, temp, plus_tol, minus_tol = soak.groups()
        duration_min = int(duration.replace("h", "")) * 60 if "h" in duration else int(duration.replace("mins", ""))
        return {"type": "soak", "duration": duration_min, "duration_tolerance": (duration_min - int(minus_min), duration_min + int(plus_min)), "temp": int(temp), "temp_tolerance": (float(minus_tol or 15), float(plus_tol or 15))}
    if hold:
        temp, plus_tol, minus_tol = hold.groups()
        return {"type": "hold", "temp": int(temp), "temp_tolerance": (float(minus_tol or 15), float(plus_tol or 15))}
    return None


def validate_oven_conditions(temp_series, time_index, steps):
    """Check the oven temperature data against the required soak/hold conditions.

    For each soak step: verify that the temperature was continuously within
    the target range (+/- tolerance) for the required duration (+/- tolerance).

    For hold steps: just verify that the temperature was sustained in-range
    for at least 5 minutes (no specific duration requirement).

    Returns:
        results: List of success messages for conditions that were met
        failures: List of failure messages for conditions that weren't met
    """
    results = []
    failures = []
    for step in steps:
        if step["type"] in ["soak", "hold"]:
            lower = step["temp"] - step["temp_tolerance"][0]
            upper = step["temp"] + step["temp_tolerance"][1]
            in_range = (temp_series >= lower) & (temp_series <= upper)

            sustained = False
            current_duration = 0
            for i in range(1, len(in_range)):
                if in_range.iloc[i]:
                    delta = (time_index[i] - time_index[i - 1]).total_seconds() / 60
                    current_duration += delta if delta <= 2 else 0
                else:
                    current_duration = 0

                if step["type"] == "soak":
                    if step["duration_tolerance"][0] <= current_duration <= step["duration_tolerance"][1]:
                        sustained = True
                        break
                else:
                    if current_duration >= 5:
                        sustained = True
                        break

            if sustained:
                results.append(f"{step['type'].capitalize()} condition met at {step['temp']}F")
            else:
                failures.append(f"{step['type'].capitalize()} condition not met at {step['temp']}F")
    return results, failures


def process_oven_file_robust(filepath, oven_df=None):
    """Process a single oven/furnace data file end-to-end.

    Similar to process_press_file_robust but simpler (no cycle splitting needed —
    each oven file is one cycle). Steps:
      1. Parse the filename to extract the oven prefix and date
      2. Read the CSV data (handling encoding issues)
      3. Match the file prefix to a row in the OvenCyclesMaverick spec sheet
      4. Parse the spec's ramp/soak/hold conditions
      5. Validate the temperature data against those conditions
      6. Generate a PDF chart with Pass/Fail status
      7. Copy the source file to the output folder for archival
    """
    try:
        filename = os.path.basename(filepath)
        safe_print(f"Processing oven: {filename}")

        parts = filename.split("_")
        if len(parts) < 3:
            safe_print(f"ERROR: Invalid filename format: {filename}")
            return False

        prefix = "_".join(parts[:3])
        date_match = re.search(r"\d{8}", filename)
        if not date_match:
            safe_print(f"ERROR: No date in filename: {filename}")
            return False

        date_str = date_match.group()
        date_fmt = datetime.strptime(date_str, "%m%d%Y").date()

        content = safe_file_read(filepath)
        if not content:
            safe_print(f"ERROR: Could not read {filename}")
            return False

        try:
            data_io = StringIO(content)
            df = pd.read_csv(data_io)
            # Normalize column names
            df = normalize_column_names(df)
        except Exception as e:
            safe_print(f"ERROR: Error parsing {filename}: {e}")
            return False

        if df.empty or len(df) < 2:
            safe_print(f"WARNING: Not enough data in {filename} (likely partial export); skipping")
            return True

        date_col = None
        time_col = None
        temp_col = None

        for col in df.columns:
            col_lower = str(col).lower()
            if 'date' in col_lower and not date_col:
                date_col = col
            elif 'time' in col_lower and not time_col:
                time_col = col
            elif ('chamber_temp' in col_lower or 'temp' in col_lower) and not temp_col:
                temp_col = col

        if not all([date_col, time_col, temp_col]):
            safe_print(f"ERROR: Missing required columns in {filename}")
            return False

        try:
            df["Datetime"] = pd.to_datetime(df[date_col] + " " + df[time_col], format="%m/%d/%Y %I:%M %p")
            df.set_index("Datetime", inplace=True)
            df.sort_index(inplace=True)
        except Exception as e:
            safe_print(f"ERROR: Error processing dates in {filename}: {e}")
            return False

        matched_row = None
        try:
            if oven_df is not None:
                for _, row in oven_df.iterrows():
                    if isinstance(row.get("Excel File Prefix"), str):
                        prefixes = [p.strip() for p in row["Excel File Prefix"].split("or")]
                        if any(prefix.startswith(p) for p in prefixes):
                            matched_row = row
                            break
        except Exception:
            matched_row = None

        parsed_conditions = []
        if matched_row is not None:
            for col in matched_row.index:
                val = matched_row[col]
                if isinstance(val, str) and ("Ramp" in val or "Soak" in val or "Hold" in val):
                    parsed_conditions.append(val)

        steps = [c for c in (parse_oven_condition(c) for c in parsed_conditions) if c]

        temp_series = clean_numeric_column(df[temp_col])
        time_index = df.index
        
        last_data_time = time_index[-1]
        time_since_last_data = (datetime.now() - last_data_time).total_seconds() / 60
        
        if time_since_last_data < 10:
            status = "Job in Progress"
            failure_text = f"Last update: {int(time_since_last_data)} min ago"
        else:
            results, failures = validate_oven_conditions(temp_series, time_index, steps)
            status = "PASS" if not failures else "FAIL"
            failure_text = "; ".join(failures)

        output_folder = os.path.join(OVEN_OUTPUT_BASE, f"{prefix}_{date_fmt}")
        os.makedirs(output_folder, exist_ok=True)

        chart_success = create_oven_chart_robust(df, temp_col, prefix, date_fmt, output_folder, status, failure_text)
        if not chart_success:
            safe_print(f"ERROR: Failed to create chart for {filename}")
            return False

        shutil.copy2(filepath, os.path.join(output_folder, filename))

        safe_print(f"SUCCESS: Processed oven {filename}")
        return True

    except Exception as e:
        safe_print(f"ERROR: Error processing oven {filepath}: {e}")
        return False


def create_oven_chart_robust(df, temp_col, prefix, date_fmt, output_folder, status="Unknown", failure_text=""):
    """Generate a PDF chart for an oven cycle, similar to press charts but simpler.

    Page 1: Temperature vs time line chart with Pass/Fail/In-Progress status
    Page 2: Raw data table for detailed inspection

    Uses the same atomic-write strategy as press charts (tmp file + os.replace).
    """
    chart_path = os.path.join(output_folder, "chart.pdf")
    tmp_chart_path = chart_path + f".tmp_{os.getpid()}_{int(time.time() * 1000)}"
    try:
        safe_print(f"  Creating chart: {chart_path}")
        with PdfPages(tmp_chart_path) as pdf:
            fig, ax = plt.subplots(figsize=(12, 6), dpi=100)

            temp_series = clean_numeric_column(df[temp_col])
            ax.plot(df.index, temp_series, label=temp_col, linewidth=1)

            ax.set_xlabel("Time")
            ax.set_ylabel("Temperature (F)")
            ax.set_title(f'Oven {prefix}', fontsize=12)
            ax.grid(True, alpha=0.3)

            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            fig.autofmt_xdate(rotation=30)

            if status == "PASS":
                status_color = 'green'
            elif status == "FAIL":
                status_color = 'red'
            elif status == "Job in Progress":
                status_color = 'orange'
            else:
                status_color = 'black'
            
            status_text = f"Status: {status}"
            if status == "FAIL" and failure_text:
                status_text += f"\nFailures: {failure_text}"
            elif status == "Job in Progress" and failure_text:
                status_text += f"\n{failure_text}"
            ax.text(0.75, 1.0, status_text, transform=ax.transAxes, ha='left', va='bottom', fontsize=10, color=status_color)

            ax.legend([temp_col], loc='upper right', fontsize=8)

            pdf.savefig(fig, bbox_inches='tight')
            plt.close(fig)

            table_data = df.reset_index()[["Datetime", temp_col]]
            table_data['Datetime'] = table_data['Datetime'].astype(str)
            table_data[temp_col] = table_data[temp_col].astype(str)
            rows, cols = table_data.shape
            table_fig_height = max(2, rows * 0.25)
            table_fig, table_ax = plt.subplots(figsize=(12, table_fig_height), dpi=100)
            table_ax.axis('off')
            table = table_ax.table(cellText=table_data.values, colLabels=table_data.columns, loc='center', cellLoc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(8)
            table.scale(1, 1.5)
            pdf.savefig(table_fig)
            plt.close(table_fig)

        try:
            os.replace(tmp_chart_path, chart_path)
        except PermissionError:
            alt_path = os.path.join(output_folder, f"chart_reprocess_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
            try:
                os.replace(tmp_chart_path, alt_path)
                safe_print(f"  WARNING: chart.pdf locked; wrote {os.path.basename(alt_path)} instead")
            except Exception:
                safe_print(f"  WARNING: chart.pdf locked; leaving temp chart at {tmp_chart_path}")
        except Exception:
            pass

        safe_print(f"  Chart created successfully")
        return True
    except Exception as e:
        safe_print(f"ERROR creating chart: {e}")
        try:
            if os.path.isfile(tmp_chart_path):
                os.remove(tmp_chart_path)
        except Exception:
            pass
        return False
# ============================================================================
# PARALLEL PROCESSING INFRASTRUCTURE
# ============================================================================
# Files are processed in parallel using a ThreadPoolExecutor. Each file gets
# submitted as a task (press or oven), and results are collected as they complete.
# The task wrappers catch exceptions so one failed file doesn't crash the pool.

def get_file_signature(path: str):
    """Get (mtime, size) tuple for change detection. Duplicate of the earlier
    version — this one uses os.stat() and handles FileNotFoundError explicitly."""
    try:
        stat = os.stat(path)
        return stat.st_mtime, stat.st_size
    except FileNotFoundError:
        return None


def process_press_task(filepath, press_number, signature, key, program_df):
    """Thread pool wrapper for processing a single press file.
    Catches all exceptions and returns a result dict with success/failure status."""
    try:
        success = process_press_file_robust(filepath, press_number, program_df)
        return {'type': 'press', 'success': success, 'key': key, 'signature': signature, 'filepath': filepath}
    except Exception as e:
        safe_print(f"Error in press task {filepath}: {e}")
        return {'type': 'press', 'success': False, 'key': key, 'signature': signature, 'filepath': filepath}


def process_oven_task(filepath, signature, key, oven_df):
    """Thread pool wrapper for processing a single oven file.
    Same pattern as process_press_task — catches exceptions, returns result dict."""
    try:
        success = process_oven_file_robust(filepath, oven_df)
        return {'type': 'oven', 'success': success, 'key': key, 'signature': signature, 'filepath': filepath}
    except Exception as e:
        safe_print(f"Error in oven task {filepath}: {e}")
        return {'type': 'oven', 'success': False, 'key': key, 'signature': signature, 'filepath': filepath}


def process_all_files_robust(processed_press_map, processed_oven_map, program_df, oven_df, startup_date):
    """Scan all press and oven input directories, find new/changed files, and
    process them in parallel.

    This is called once per polling cycle by the main loop. It:
      1. Scans each Press_N and Oven_N directory for data files
      2. Filters to files created within the lookback window (startup_date)
      3. Skips files that are still being written (stability check)
      4. Skips files that haven't changed since last processing (signature check)
      5. Skips files processed too recently (throttle check)
      6. Submits all remaining files to the thread pool for parallel processing
      7. Collects results and updates the processed file maps

    Args:
        processed_press_map: Dict tracking processed press files {filepath: signature}
        processed_oven_map:  Dict tracking processed oven files {filepath: signature}
        program_df: Press program spec DataFrame (from Excel)
        oven_df:    Oven cycle spec DataFrame (from Excel)
        startup_date: Only process files created on or after this date

    Returns:
        (processed_count, failed_count, new_items_count)
    """
    safe_print("\nNEW CYCLE DETECTION HMI PROCESSOR")
    safe_print("=" * 50)

    os.makedirs(OUTPUT_BASE, exist_ok=True)
    os.makedirs(OVEN_OUTPUT_BASE, exist_ok=True)

    processed = 0
    failed = 0
    new_items = 0
    tasks = []
    failed_files = []

    safe_print(f"\nSCANNING PRESS FILES")
    safe_print("-" * 30)
    
    for press_number in PRESS_NUMBERS:
        press_dir = os.path.join(BASE_WATCH_PATH, f"Press_{press_number}")

        if not os.path.exists(press_dir):
            safe_print(f"WARNING: Press directory not found: {press_dir}")
            continue

        press_files = [f for f in os.listdir(press_dir) if f.endswith('.txt')]
        press_files_filtered = []
        for f in press_files:
            try:
                file_date = datetime.fromtimestamp(os.path.getctime(os.path.join(press_dir, f))).date()
                if file_date >= startup_date:
                    press_files_filtered.append(f)
            except Exception:
                continue
        press_files = press_files_filtered
        safe_print(f"Found {len(press_files)} files ({startup_date} forward) in Press_{press_number}")

        for filename in press_files:
            filepath = os.path.join(press_dir, filename)
            
            try:
                file_size = os.path.getsize(filepath)
                if file_size == 0:
                    continue
            except Exception:
                continue

            if not is_file_stable_for_processing(filepath):
                continue
            
            signature = get_file_signature(filepath)
            key = os.path.join(press_dir, filename)
            if (not FORCE_REPROCESS) and signature is not None and processed_press_map.get(key) == signature:
                continue

            try:
                now_ts = time.time()
                last_ts = float(_last_processed_ts.get(key, 0.0) or 0.0)
                if (now_ts - last_ts) < FILE_REPROCESS_THROTTLE_SECONDS:
                    continue
            except Exception:
                pass
            tasks.append(('press', filepath, press_number, signature, key))

    safe_print(f"\nSCANNING OVEN FILES")
    safe_print("-" * 30)

    for oven_number in OVEN_NUMBERS:
        oven_dir = os.path.join(BASE_WATCH_PATH, f"Oven_{oven_number}")

        if not os.path.exists(oven_dir):
            safe_print(f"WARNING: Oven directory not found: {oven_dir}")
            continue

        oven_files = [f for f in os.listdir(oven_dir) if f.endswith(('.csv', '.xlsx', '.xls'))]
        oven_files_filtered = []
        for f in oven_files:
            try:
                file_date = datetime.fromtimestamp(os.path.getctime(os.path.join(oven_dir, f))).date()
                if file_date >= startup_date:
                    oven_files_filtered.append(f)
            except Exception:
                continue
        oven_files = oven_files_filtered
        safe_print(f"Found {len(oven_files)} files ({startup_date} forward) in Oven_{oven_number}")

        for filename in oven_files:
            filepath = os.path.join(oven_dir, filename)
            
            try:
                file_size = os.path.getsize(filepath)
                if file_size == 0:
                    continue
            except Exception:
                continue

            if not is_file_stable_for_processing(filepath):
                continue
            
            signature = get_file_signature(filepath)
            key = os.path.join(oven_dir, filename)
            if (not FORCE_REPROCESS) and signature is not None and processed_oven_map.get(key) == signature:
                continue

            try:
                now_ts = time.time()
                last_ts = float(_last_processed_ts.get(key, 0.0) or 0.0)
                if (now_ts - last_ts) < FILE_REPROCESS_THROTTLE_SECONDS:
                    continue
            except Exception:
                pass
            tasks.append(('oven', filepath, None, signature, key))

    if len(tasks) == 0:
        safe_print("\nNo new files to process")
    else:
        safe_print(f"\nPROCESSING {len(tasks)} FILES IN PARALLEL")
        safe_print("-" * 30)

        with ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS) as executor:
            futures = []
            for task in tasks:
                task_type, filepath, press_number, signature, key = task
                if task_type == 'press':
                    future = executor.submit(process_press_task, filepath, press_number, signature, key, program_df)
                else:
                    future = executor.submit(process_oven_task, filepath, signature, key, oven_df)
                futures.append(future)

            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result['success']:
                        processed += 1
                        new_items += 1
                    else:
                        failed += 1
                        failed_files.append(result.get('filepath', 'Unknown'))

                    try:
                        _last_processed_ts[result['key']] = time.time()
                    except Exception:
                        pass
                    if result['signature'] is not None:
                        if result['type'] == 'press':
                            processed_press_map[result['key']] = result['signature']
                        else:
                            processed_oven_map[result['key']] = result['signature']
                except Exception as e:
                    safe_print(f"Error collecting result: {e}")
                    failed += 1

    safe_print(f"\nPROCESSING COMPLETE")
    safe_print("=" * 50)
    safe_print(f"Successfully processed: {processed} files")
    safe_print(f"Failed to process: {failed} files")
    if failed_files:
        safe_print("\nFailed files:")
        for filepath in failed_files:
            safe_print(f"  - {filepath}")
    safe_print(f"Output: {OUTPUT_BASE} and {OVEN_OUTPUT_BASE}")
    return processed, failed, new_items


def _parse_results_folder_datetime(folder_name: str):
    """Extract the datetime from a results folder name.

    Folder names follow the pattern: Results_{PartNum}_{YYYY-MM-DD}_{HH-MM-SS}
    Returns the datetime, or None if the folder name doesn't match.
    """
    try:
        if not folder_name.startswith("Results_"):
            return None
        parts = folder_name.split("_")
        if len(parts) < 4:
            return None
        date_s = parts[2]
        time_s = parts[3]
        for fmt in ["%Y-%m-%d %H-%M-%S", "%Y-%m-%d %H-%M"]:
            try:
                return datetime.strptime(f"{date_s} {time_s}", fmt)
            except Exception:
                pass
        return None
    except Exception:
        return None


def purge_output_folders(startup_time: datetime) -> int:
    """Delete output folders within the lookback window before reprocessing.

    This is used when PURGE_OUTPUTS_ON_START is enabled — it ensures a clean
    slate by removing old results that will be regenerated. Only deletes
    folders with timestamps >= startup_time to avoid destroying ancient history.

    Returns the number of folders removed.
    """
    removed = 0
    try:
        cutoff_date = startup_time.date()

        for press_number in PRESS_NUMBERS:
            press_root = os.path.join(OUTPUT_BASE, f"Press_{press_number}")

            if not os.path.isdir(press_root):
                continue
            try:
                for name in os.listdir(press_root):
                    full = os.path.join(press_root, name)
                    if not os.path.isdir(full):
                        continue
                    dt = _parse_results_folder_datetime(name)
                    if dt is None:
                        continue
                    if dt >= startup_time:
                        shutil.rmtree(full, ignore_errors=True)
                        removed += 1
            except Exception:
                continue

        if os.path.isdir(OVEN_OUTPUT_BASE):
            try:
                for name in os.listdir(OVEN_OUTPUT_BASE):
                    full = os.path.join(OVEN_OUTPUT_BASE, name)
                    if not os.path.isdir(full):
                        continue
                    parts = name.split("_")
                    if not parts:
                        continue
                    date_s = parts[-1]
                    try:
                        d = datetime.strptime(date_s, "%Y-%m-%d").date()
                    except Exception:
                        continue
                    if d >= cutoff_date:
                        shutil.rmtree(full, ignore_errors=True)
                        removed += 1
            except Exception:
                pass

        return removed
    except Exception:
        return removed


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point — sets up configuration and runs the polling loop.

    Startup sequence:
      1. Calculate the lookback window (how far back to process files)
      2. Load the Excel spec sheets (press programs + oven cycles)
      3. Clean up stale failure reports from previous runs
      4. Optionally spawn a background reprocess worker
      5. Optionally purge existing output folders for a clean slate
      6. Enter the infinite polling loop:
         - Scan for new/changed files
         - Process them in parallel
         - Save state
         - Sleep for WATCH_INTERVAL_SECONDS
         - Repeat

    Can also run in worker mode (--__reprocess_worker flag) which does a
    single pass through all files and then exits. This is used by the
    background reprocess worker spawned from the main process.
    """
    try:
        is_worker = "--__reprocess_worker" in sys.argv

        now = datetime.now()
        lookback_hours = DEFAULT_LOOKBACK_HOURS
        try:
            if LOOKBACK_DAYS:
                lookback_hours = float(LOOKBACK_DAYS) * 24.0
            elif LOOKBACK_HOURS:
                lookback_hours = float(LOOKBACK_HOURS)
        except Exception:
            lookback_hours = DEFAULT_LOOKBACK_HOURS

        startup_time = now - timedelta(hours=lookback_hours)
        startup_date = startup_time.date()
        
        safe_print("Starting NEW CYCLE DETECTION HMI Processor...")
        safe_print(f"Processing files from: {startup_date} (lookback {lookback_hours:.1f} hours) forward")
        safe_print(f"Script: {os.path.abspath(__file__)}")
        safe_print("\nLoading Excel validation files...")
        program_df, oven_df = load_excel_files()

        if CLEAN_FAILURE_REPORTS_ON_START:
            removed = cleanup_failure_reports(OUTPUT_BASE)
            if removed > 0:
                safe_print(f"Removed {removed} stale FAILURE_REPORT.txt files")

        if is_worker:
            processed_press_map = {}
            processed_oven_map = {}
            if PURGE_OUTPUTS_ON_START:
                do_purge = True
                try:
                    if sys.stdin.isatty():
                        ans = input(f"Purge existing output folders on M: since {startup_time.strftime('%Y-%m-%d %H:%M:%S')} before reprocessing? (y/N): ").strip().lower()
                        do_purge = ans in ["y", "yes"]
                except Exception:
                    do_purge = True

                if do_purge:
                    safe_print("\nPurging existing output folders for the lookback window...")
                    removed = purge_output_folders(startup_time)
                    safe_print(f"Purge complete. Removed {removed} output folders.")

            safe_print("\nWorker mode: reprocessing all eligible files once...")
            safe_print(f"Cycle start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            process_all_files_robust(processed_press_map, processed_oven_map, program_df, oven_df, startup_date)
            safe_print("\nWorker mode complete. Exiting.")
            return

        spawned = False
        try:
            if SPAWN_REPROCESS_WORKER:
                spawned = _spawn_reprocess_worker()
            elif _should_prompt_spawn_worker():
                ans = input("Spawn reprocess worker in background? (y/N): ").strip().lower()
                if ans in ["y", "yes"]:
                    spawned = _spawn_reprocess_worker()
        except Exception:
            spawned = False

        if spawned:
            safe_print("INFO: Reprocess worker spawned")
        
        if REPROCESS_ON_START:
            processed_press_map = {}
            processed_oven_map = {}
        else:
            processed_press_map, processed_oven_map = load_process_state(PROCESS_STATE_PATH)

        while True:
            try:
                safe_print(f"\nCycle start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                processed, failed, new_items = process_all_files_robust(processed_press_map, processed_oven_map, program_df, oven_df, startup_date)
                safe_print(f"Cycle summary: processed={processed}, failed={failed}")
                if not REPROCESS_ON_START:
                    save_process_state(PROCESS_STATE_PATH, processed_press_map, processed_oven_map)
                if new_items == 0:
                    safe_print(f"No new files. Next check in {WATCH_INTERVAL_SECONDS} seconds.")
                else:
                    safe_print(f"New files processed: {new_items}. Next check in {WATCH_INTERVAL_SECONDS} seconds.")
            except Exception as e:
                safe_print(f"\nERROR: Cycle crashed: {e}")
                traceback.print_exc()

            try:
                time.sleep(WATCH_INTERVAL_SECONDS)
            except Exception:
                time.sleep(30)
    except KeyboardInterrupt:
        safe_print("\nINFO: Process interrupted")
    except Exception as e:
        safe_print(f"\nERROR: Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
