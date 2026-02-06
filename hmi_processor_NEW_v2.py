#!/usr/bin/env python3
"""
HMI PROCESSOR WITH NEW CYCLE DETECTION LOGIC - FIXED VERSION v2
Properly splits production runs based on tonnage and temperature patterns
Discards waste data between runs

FIXES APPLIED:
- UTF-16 encoding support for HMI files
- clean_numeric_column helper to strip null bytes before numeric conversion
- Column name normalization (strip BOM, null bytes)
- Fixed lookback logic for leading zeros (iterate backwards)
- Filter out cycles that never had pressing
- Fixed mutable default argument
- Explicit tonnage cleaning in validation

Date: 2026-01-22
"""

import os
import sys
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import MaxNLocator
import json
import subprocess
from datetime import datetime, date, timedelta
import time
import shutil
import re
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Configuration
PRESS_NUMBERS = [3, 4, 5, 6, 7, 8]
OVEN_NUMBERS = [1, 2, 3]
BASE_WATCH_PATH = os.environ.get("HMI_BASE_WATCH_PATH", r"C:\HMI_Upload")
OUTPUT_BASE = os.environ.get("HMI_OUTPUT_BASE", r"M:\Quality\Press Charts")
OVEN_OUTPUT_BASE = os.environ.get("HMI_OVEN_OUTPUT_BASE", r"M:\Quality\Furnace Chart")
EXCEL_PATH = os.environ.get("HMI_EXCEL_PATH", r"C:\HMI_Upload\PythonScripts")
WATCH_INTERVAL_SECONDS = int(os.environ.get("HMI_WATCH_INTERVAL_SECONDS", "30"))
MAX_WORKER_THREADS = int(os.environ.get("HMI_MAX_WORKER_THREADS", "10"))
MIN_FILE_AGE_SECONDS = int(os.environ.get("HMI_MIN_FILE_AGE_SECONDS", "10"))
FILE_STABLE_SECONDS = int(os.environ.get("HMI_FILE_STABLE_SECONDS", "10"))
FILE_REPROCESS_THROTTLE_SECONDS = int(os.environ.get("HMI_FILE_REPROCESS_THROTTLE_SECONDS", "120"))
DEFAULT_LOOKBACK_HOURS = float(os.environ.get("HMI_DEFAULT_LOOKBACK_HOURS", "35"))
LOOKBACK_DAYS = os.environ.get("HMI_LOOKBACK_DAYS", "").strip()
LOOKBACK_HOURS = os.environ.get("HMI_LOOKBACK_HOURS", "").strip()
FORCE_REPROCESS = os.environ.get("HMI_FORCE_REPROCESS", "0").strip() in ["1", "true", "True", "yes", "YES"]
PURGE_OUTPUTS_ON_START = os.environ.get("HMI_PURGE_OUTPUTS_ON_START", "0").strip() in ["1", "true", "True", "yes", "YES"]
PROCESS_STATE_PATH = os.environ.get("HMI_PROCESS_STATE_PATH", os.path.join(EXCEL_PATH, "hmi_processor_state.json"))
REPROCESS_ON_START = os.environ.get("HMI_REPROCESS_ON_START", "1").strip() not in ["0", "false", "False", "no", "NO"]
SPAWN_REPROCESS_WORKER = os.environ.get("HMI_SPAWN_REPROCESS_WORKER", "0").strip() in ["1", "true", "True", "yes", "YES"]
CYCLE_PAD_MINUTES = int(os.environ.get("HMI_CYCLE_PAD_MINUTES", "3"))
MAX_REALISTIC_TONNAGE = float(os.environ.get("HMI_MAX_REALISTIC_TONNAGE", "600"))
PRESS5_IDLE_TONNAGE_DISPLAY = float(os.environ.get("HMI_PRESS5_IDLE_TONNAGE_DISPLAY", "10"))
PRESS7_IDLE_TONNAGE = float(os.environ.get("HMI_PRESS7_IDLE_TONNAGE", "10"))
PRESS7_MIN_PRESSING_TONNAGE = float(os.environ.get("HMI_PRESS7_MIN_PRESSING_TONNAGE", "11"))
PRESS7_MIN_CYCLE_TONNAGE = float(os.environ.get("HMI_PRESS7_MIN_CYCLE_TONNAGE", "20"))
MAX_TIME_GAP_MINUTES = float(os.environ.get("HMI_MAX_TIME_GAP_MINUTES", "5"))

# NEW: Cycle detection thresholds
TC_TEMP_THRESHOLD = 399  # Below this temp, cycle can end
CYCLE_VALID_MIN_MAX_TEMP = float(os.environ.get("HMI_CYCLE_VALID_MIN_MAX_TEMP", "600"))
CYCLE_VALID_MIN_DURATION_MIN = float(os.environ.get("HMI_CYCLE_VALID_MIN_DURATION_MIN", "15"))
TONNAGE_ZERO = 0  # Exactly zero
MAX_LEADING_ZEROS = 3  # Capture up to 3 zeros before pressing
MAX_TRAILING_ZEROS = 3  # Capture up to 3 zeros after pressing

CLEAN_FAILURE_REPORTS_ON_START = os.environ.get("HMI_CLEAN_FAILURE_REPORTS_ON_START", "1").strip() not in ["0", "false", "False", "no", "NO"]
FILE_BOUNDARY_GRACE_MINUTES = float(os.environ.get("HMI_FILE_BOUNDARY_GRACE_MINUTES", "5"))
PREPEND_PREV_FILE_MINUTES = float(os.environ.get("HMI_PREPEND_PREV_FILE_MINUTES", "120"))
APPEND_NEXT_FILE_MINUTES = float(os.environ.get("HMI_APPEND_NEXT_FILE_MINUTES", "720"))
MAX_REALISTIC_TEMP_F = float(os.environ.get("HMI_MAX_REALISTIC_TEMP_F", "1500"))
MIN_ACTIVE_TC_SAMPLES = int(os.environ.get("HMI_MIN_ACTIVE_TC_SAMPLES", "5"))
MIN_CYCLE_PRESSING_TONNAGE = float(os.environ.get("HMI_MIN_CYCLE_PRESSING_TONNAGE", "5"))
TONNAGE_UNRELIABLE_PRESSES = set(
    int(x.strip())
    for x in os.environ.get("HMI_TONNAGE_UNRELIABLE_PRESSES", "").split(",")
    if x.strip().isdigit()
)

_last_seen_file_signature = {}
_last_processed_ts = {}


def load_process_state(path: str):
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


def _should_prompt_spawn_worker() -> bool:
    try:
        return bool(sys.stdin and sys.stdin.isatty())
    except Exception:
        return False


def _spawn_reprocess_worker() -> bool:
    try:
        args = [sys.executable, os.path.abspath(__file__), "--__reprocess_worker"]
        subprocess.Popen(args, close_fds=True)
        return True
    except Exception:
        return False

def safe_print(text):
    """Safe print that handles encoding"""
    try:
        print(str(text))
    except:
        print("Print error")


def cleanup_failure_reports(output_base: str) -> int:
    """Remove stale FAILURE_REPORT.txt files from previous runs."""
    removed = 0
    try:
        if not output_base or not os.path.isdir(output_base):
            return 0

        for root, _dirs, files in os.walk(output_base):
            if "FAILURE_REPORT.txt" in files:
                try:
                    os.remove(os.path.join(root, "FAILURE_REPORT.txt"))
                    removed += 1
                except Exception:
                    continue
    except Exception:
        return removed

    return removed


def _parse_yymmdd_datecode(datecode: str):
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
    return f"{d.year % 100:02d}{d.month:02d}{d.day:02d}"


def get_adjacent_daily_file_path(filepath: str, delta_days: int):
    try:
        base = os.path.basename(filepath)
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


def load_press_dataframe(filepath: str):
    try:
        content = safe_file_read(filepath)
        if not content:
            return None

        first_line = content.split('\n')[0]
        has_header = 'Date' in first_line or 'TONNAGE' in first_line
        data_io = StringIO(content)
        if has_header:
            df = pd.read_csv(data_io, sep='\t')
            df = normalize_column_names(df)
            required_cols = ['Date', 'PART#']
            for col in required_cols:
                if col not in df.columns:
                    return None
        else:
            raw_df = pd.read_csv(StringIO(content), sep='\t', header=None, engine='python', dtype=str, na_filter=False)
            num_cols = raw_df.shape[1]
            headers15 = ['Date', 'TONNAGE DISPLAY', 'TOOL TEMP', 'TOP PLATTEN AVG', 'BOTTOM PLATTEN',
                         'Z1TC1A2A', 'Z1TC3A4A', 'Z2TC1B2B', 'Z2TC3B4B',
                         'Z3TC1C2C', 'Z3TC3C4C', 'Z4TC1D2D', 'Z4TC3D4D', 'SCREENNO', 'PART#']
            headers11 = ['Date', 'TC1', 'TC2', 'TC3', 'TC4',
                         'TONNAGE', 'PLATTEN1', 'PLATTEN2', 'PRESS', 'PART#', 'Useless']
            if num_cols >= 15:
                names = headers15[:num_cols]
            elif num_cols == 11:
                names = headers11
            else:
                names = ['Date'] + [f'Col{i}' for i in range(1, num_cols - 1)] + ['PART#']
            df = pd.read_csv(StringIO(content), sep='\t', names=names, engine='python')
            df = normalize_column_names(df)

        s = df['Date'].astype(str)
        s = s.str.replace(r'[\x00-\x1F\x7F\u200B\uFEFF]', '', regex=True)
        s = s.str.strip().str.replace(r'\s+', ' ', regex=True)
        time_part = s.str.extract(r'(\d{2}:\d{2}:\d{2})')[0]
        date_part = s.str.extract(r'(\d{2}-\d{2}-\d{4})')[0]
        combined_tf = time_part.str.cat(date_part, sep=' ', na_rep='')
        dt = pd.to_datetime(combined_tf, format='%H:%M:%S %m-%d-%Y', errors='coerce')
        if dt.isna().all():
            combined_df = date_part.str.cat(time_part, sep=' ', na_rep='')
            dt = pd.to_datetime(combined_df, format='%m-%d-%Y %H:%M:%S', errors='coerce')
        if dt.isna().all():
            dt = pd.to_datetime(s, errors='coerce')
        df['Date'] = dt
        df = df.dropna(subset=['Date'])
        if len(df) < 1:
            return None
        df['TimeStr'] = df['Date'].dt.strftime('%H:%M')
        df.set_index('Date', inplace=True, drop=False)
        return df
    except Exception:
        return None


def is_file_stable_for_processing(filepath: str) -> bool:
    """Avoid processing files that are still being written/updated."""
    try:
        now = time.time()
        age_seconds = now - os.path.getmtime(filepath)
        if age_seconds < MIN_FILE_AGE_SECONDS:
            return False

        return True
    except Exception:
        return False


def get_file_signature(filepath: str) -> tuple:
    try:
        return (os.path.getmtime(filepath), os.path.getsize(filepath))
    except Exception:
        return None


def expand_cycle_window_around_pressing(df, start_idx, end_idx, tonnage_col):
    """Expand a detected cycle to include a time window before/after tonnage is applied."""
    cycle_df = df.iloc[start_idx:end_idx + 1].copy()
    if cycle_df.empty or tonnage_col not in cycle_df.columns:
        return cycle_df

    try:
        press_number = None
        try:
            if 'PRESS' in cycle_df.columns and len(cycle_df) > 0:
                press_number = int(float(cycle_df['PRESS'].iloc[0]))
        except Exception:
            press_number = None

        idle_threshold = get_idle_tonnage_threshold(press_number)
        cycle_pressing_threshold = get_cycle_pressing_threshold(press_number)
        tonnage = clean_numeric_column(cycle_df[tonnage_col])
        pressing_mask = (tonnage > cycle_pressing_threshold) & (tonnage <= MAX_REALISTIC_TONNAGE)
        if not pressing_mask.any():
            return cycle_df

        press_start = cycle_df.index[pressing_mask].min()
        press_end = cycle_df.index[pressing_mask].max()
        pad = timedelta(minutes=CYCLE_PAD_MINUTES)
        window_start = min(cycle_df.index.min(), press_start - pad)
        window_end = max(cycle_df.index.max(), press_end + pad)

        expanded = df.loc[(df.index >= window_start) & (df.index <= window_end)].copy()
        if expanded.empty:
            return cycle_df
        return expanded
    except Exception:
        return cycle_df


def is_cycle_complete(cycle_df, tonnage_col, press_number=None):
    """Determine whether a cycle/job is complete based on recent idle/low-temperature behavior."""
    try:
        if cycle_df.empty or tonnage_col not in cycle_df.columns:
            return False

        last_time = cycle_df.index.max()
        window_start = last_time - timedelta(minutes=5)
        window_df = cycle_df.loc[cycle_df.index >= window_start].copy()
        if window_df.empty:
            return False

        window_minutes = (window_df.index.max() - window_df.index.min()).total_seconds() / 60
        if window_minutes < 5:
            return False

        idle_threshold = get_idle_tonnage_threshold(press_number)
        tonnage = clean_numeric_column(window_df[tonnage_col]).fillna(0)
        tonnage_idle = (tonnage <= idle_threshold).all()

        temp_cols = [c for c in window_df.columns if 'TEMP' in c.upper() or 'TC' in c.upper()]
        if not temp_cols:
            return False

        temps_window = window_df[temp_cols].apply(clean_numeric_column)

        all_zero_window = tonnage_idle and (temps_window.fillna(0) == 0).all().all()
        if all_zero_window:
            return True

        temps_full = cycle_df[temp_cols].apply(clean_numeric_column)
        active_cols = [c for c in temp_cols if temps_full[c].fillna(0).abs().max() > 0]
        if not active_cols:
            return False

        temps_active_window = temps_window[active_cols].mask(temps_window[active_cols] == 0)
        temps_active_ok = temps_active_window.notna().all(axis=1) & (temps_active_window < TC_TEMP_THRESHOLD).all(axis=1)
        return tonnage_idle and temps_active_ok.all()
    except Exception:
        return False


def safe_file_read(filepath):
    """Read file with encoding handling - FIXED for UTF-16 HMI files"""
    # Try UTF-16 FIRST since HMI files commonly use it
    encodings = ['utf-16', 'utf-16-le', 'utf-16-be', 'utf-8', 'utf-8-sig', 'cp1252', 'latin-1', 'iso-8859-1']

    for encoding in encodings:
        try:
            with open(filepath, 'r', encoding=encoding) as f:
                content = f.read()
            if content and len(content.strip()) > 0:
                # For UTF-16 decoded content, strip any remaining null bytes
                if 'utf-16' in encoding.lower():
                    content = content.replace('\x00', '')
                
                # Validate content looks reasonable
                first_line = content.split('\n')[0] if '\n' in content else content[:100]
                # Check it's not garbage (no high unicode chars in first line)
                if not any(ord(c) > 1000 for c in first_line[:50]):
                    return content
        except:
            continue
    
    # Fallback: read as binary and try to decode
    try:
        with open(filepath, 'rb') as f:
            raw_content = f.read()
        
        # Check for UTF-16 BOM
        if raw_content.startswith(b'\xff\xfe') or raw_content.startswith(b'\xfe\xff'):
            content = raw_content.decode('utf-16', errors='ignore')
            content = content.replace('\x00', '')
            return content
        
        # Check for null bytes pattern (UTF-16 without BOM)
        if b'\x00' in raw_content[:100]:
            # Likely UTF-16-LE without BOM
            content = raw_content.decode('utf-16-le', errors='ignore')
            content = content.replace('\x00', '')
            return content
        
        content = raw_content.decode('utf-8', errors='ignore')
        if content and len(content.strip()) > 0:
            return content
    except:
        pass
    
    return None


def normalize_column_names(df):
    """Normalize column names by stripping BOM, null bytes, and whitespace"""
    df.columns = (df.columns
                  .astype(str)
                  .str.replace('\x00', '', regex=False)
                  .str.replace('\ufeff', '', regex=False)
                  .str.replace('\0', '', regex=False)
                  .str.strip())
    return df


def clean_numeric_column(series):
    """Clean a series for numeric conversion - handles UTF-16 artifacts"""
    # Convert to string and strip null bytes and other artifacts
    cleaned = series.astype(str).str.replace('\x00', '', regex=False)
    cleaned = cleaned.str.replace('\0', '', regex=False)
    cleaned = cleaned.str.strip()
    # Convert to numeric
    return pd.to_numeric(cleaned, errors='coerce')


def load_excel_files():
    """Load Excel files for press programs and oven cycles"""
    program_df = pd.DataFrame()
    oven_df = pd.DataFrame()

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
    """Return tool quantity for the part number from the program sheet; default to 1"""
    if program_df is None or program_df.empty:
        return 1
    try:
        match = program_df.loc[program_df['Program'] == part_number]
        if not match.empty:
            tool_col = None
            for c in match.columns:
                if str(c).strip().lower() == 'tool quantity':
                    tool_col = c
                    break
            if tool_col is None:
                for c in match.columns:
                    s = str(c).strip().lower()
                    if 'tool' in s and ('qty' in s or 'quantity' in s):
                        tool_col = c
                        break
            if tool_col is None:
                return 1

            raw_val = match[tool_col].values[0]
            qty = pd.to_numeric(raw_val, errors='coerce')
            if pd.notna(qty):
                return int(float(qty))

            s = str(raw_val)
            m = re.search(r"(\d+)", s)
            if m:
                return int(m.group(1))
            return 1
    except Exception:
        pass
    safe_print(f"WARNING: Tool quantity not found for {part_number}, using default value 1")
    return 1


def extract_conditions(steps, num_tools):
    """Extract validation conditions from Excel steps text"""
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


def _get_temperature_columns(session_group):
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
    temps_df = session_group[temp_cols].copy()
    temps_df = temps_df.mask((temps_df == 0) | (temps_df > MAX_REALISTIC_TEMP_F) | (temps_df < -50))
    active = []
    for c in temp_cols:
        try:
            if int(temps_df[c].notna().sum()) >= MIN_ACTIVE_TC_SAMPLES:
                active.append(c)
        except Exception:
            continue
    return temps_df, active


def _max_continuous_minutes(mask, time_index):
    current_duration = 0.0
    max_duration = 0.0
    for i in range(1, len(mask)):
        if mask.iloc[i]:
            delta = (time_index[i] - time_index[i - 1]).total_seconds() / 60
            if delta <= 1.5:
                current_duration += delta
            else:
                current_duration = 0.0
        else:
            current_duration = 0.0
        max_duration = max(max_duration, current_duration)
    return max_duration


def evaluate_conditions_progress(session_group, conditions, press_number=None):
    """Evaluate which conditions have been satisfied so far.

    Returns:
        all_met (bool)
        unmet_reasons (list[str])
        debug (dict)
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

            per_col_ok = (temps_df[active_cols] >= cond['range'][0]) & (temps_df[active_cols] <= cond['range'][1])
            in_range = per_col_ok.all(axis=1)

            if cond.get('duration'):
                if cond['type'] == 'soak':
                    max_duration_achieved = _max_continuous_minutes(in_range, time_index)
                    met = max_duration_achieved >= float(cond['duration'])
                else:
                    current_duration = 0.0
                    for i in range(1, len(in_range)):
                        if in_range.iloc[i]:
                            delta = (time_index[i] - time_index[i - 1]).total_seconds() / 60
                            if delta <= 1.5:
                                current_duration += delta
                            else:
                                current_duration = 0.0
                        else:
                            current_duration = 0.0
                        max_duration_achieved = max(max_duration_achieved, current_duration)

                        if cond['duration'][0] <= current_duration <= cond['duration'][1]:
                            met = True
                            break
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

            per_col_temp_ok = (temps_df[active_cols] >= cond['temp_range'][0]) & (temps_df[active_cols] <= cond['temp_range'][1])
            temp_ok = per_col_temp_ok.all(axis=1)

            in_range = tonnage_ok & temp_ok

            if cond.get('duration'):
                current_duration = 0.0
                for i in range(1, len(in_range)):
                    if in_range.iloc[i]:
                        delta = (time_index[i] - time_index[i - 1]).total_seconds() / 60
                        if delta <= 1.5:
                            current_duration += delta
                        else:
                            current_duration = 0.0
                    else:
                        current_duration = 0.0
                    max_duration_achieved = max(max_duration_achieved, current_duration)

                    if cond['duration'][0] <= current_duration <= cond['duration'][1]:
                        met = True
                        break
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
    """Run validation checks with optional duration requirement"""
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
                temps_df = session_group[temp_cols].copy()
                temps_df = temps_df.mask(temps_df == 0)

                active_cols = [c for c in temp_cols if temps_df[c].notna().any()]
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
                temps_df = session_group[temp_cols].copy() if temp_cols else pd.DataFrame(index=session_group.index)
                if not temps_df.empty:
                    temps_df = temps_df.mask(temps_df == 0)
                    active_cols = [c for c in temp_cols if temps_df[c].notna().any()]
                else:
                    active_cols = []

                temp_ok = pd.Series(True, index=session_group.index)
                if cond.get('temp_range') is not None:
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
            sustained = False
            current_duration = 0
            max_duration_achieved = 0
            for i in range(1, len(in_range)):
                if in_range.iloc[i]:
                    delta = (time_index[i] - time_index[i - 1]).total_seconds() / 60
                    if delta <= 1.5:
                        current_duration += delta
                    else:
                        current_duration = 0
                else:
                    current_duration = 0
                
                max_duration_achieved = max(max_duration_achieved, current_duration)

                if isinstance(cond['duration'], tuple):
                    if cond['duration'][0] <= current_duration <= cond['duration'][1]:
                        sustained = True
                        break
                else:
                    if current_duration >= cond['duration']:
                        sustained = True
                        break

            if not sustained:
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


# NEW FUNCTIONS FOR CYCLE DETECTION

def identify_columns(df):
    """
    Identify TC columns and tonnage column based on press type
    
    Returns:
        tc_cols: list of TC column names
        tonnage_col: name of tonnage column
    """
    tc_cols = []
    tonnage_col = None
    
    # Check for Press 5 format (TONNAGE DISPLAY)
    if 'TONNAGE DISPLAY' in df.columns:
        tonnage_col = 'TONNAGE DISPLAY'
        # Press 5 TC pattern: Z\d+TC.*
        for col in df.columns:
            if re.match(r'Z\d+TC', str(col), re.IGNORECASE):
                tc_cols.append(col)
    
    # Standard press format (TONNAGE)
    elif 'TONNAGE' in df.columns:
        tonnage_col = 'TONNAGE'
        # Standard TC pattern: TC\d+
        for col in df.columns:
            if re.match(r'TC\d+$', str(col), re.IGNORECASE):
                tc_cols.append(col)
    
    return tc_cols, tonnage_col


def calculate_tc_average(df, tc_cols):
    """
    Calculate average of all TC columns
    Returns a Series with TC averages for each row
    """
    if not tc_cols:
        return pd.Series(0, index=df.index)
    
    # Convert TC columns to numeric using the clean function
    tc_data = df[tc_cols].apply(clean_numeric_column)
    tc_data = tc_data.mask(tc_data == 0)
    
    # Calculate mean across TC columns
    tc_avg = tc_data.mean(axis=1).fillna(0)
    
    return tc_avg


def get_idle_tonnage_threshold(press_number):
    if press_number == 7:
        return PRESS7_IDLE_TONNAGE
    return TONNAGE_ZERO


def get_pressing_tonnage_threshold(press_number):
    if press_number == 7:
        return max(get_idle_tonnage_threshold(press_number), PRESS7_MIN_PRESSING_TONNAGE)
    return get_idle_tonnage_threshold(press_number)


def get_cycle_pressing_threshold(press_number):
    pressing_threshold = get_pressing_tonnage_threshold(press_number)
    if press_number == 7:
        return max(pressing_threshold, PRESS7_MIN_CYCLE_TONNAGE)
    return pressing_threshold


def split_into_cycles(df, tc_cols, tonnage_col, press_number=None):
    """
    Split dataframe into production cycles
    
    Rules:
    1. Part number change = new cycle immediately
    2. Cycle START: up to 3 zeros before tonnage goes positive
    3. Cycle END: tonnage = 0 and TC_avg < 399
    4. WASTE: tonnage = 0 and TC_avg < 399 (discard, don't chart)
    5. Only include cycles that actually had pressing (tonnage > 0 at some point)
    
    Returns:
        List of (start_idx, end_idx, part_number) tuples
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
            temps_raw = df[tc_cols].apply(clean_numeric_column).fillna(0)
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


def create_failure_report(output_folder, press_number, part_number, first_date, pass_status, failure_details, cycle_df, conditions=None, num_tools=None):
    """Create a detailed failure report text file when validation fails"""
    report_path = os.path.join(output_folder, "FAILURE_REPORT.txt")

    if pass_status in ["Pass", "In Progress"]:
        try:
            if os.path.exists(report_path):
                os.remove(report_path)
        except Exception:
            pass
        return  # No report needed for passing jobs
    
    try:
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


def process_press_file_robust(filepath, press_number, program_df=None):
    """Process press file with robust error handling and NEW cycle detection"""
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
        prev_path = get_adjacent_daily_file_path(filepath, -1)
        if prev_path and os.path.exists(prev_path) and is_file_stable_for_processing(prev_path):
            prev_df = load_press_dataframe(prev_path)
            if prev_df is not None and not prev_df.empty:
                cutoff = original_file_start - timedelta(minutes=PREPEND_PREV_FILE_MINUTES)
                prev_tail = prev_df.loc[prev_df.index >= cutoff]
                if not prev_tail.empty:
                    stitched_df = pd.concat([prev_tail, stitched_df], axis=0)

        next_path = get_adjacent_daily_file_path(filepath, 1)
        if next_path and os.path.exists(next_path) and is_file_stable_for_processing(next_path):
            next_df = load_press_dataframe(next_path)
            if next_df is not None and not next_df.empty:
                cutoff = original_file_end + timedelta(minutes=APPEND_NEXT_FILE_MINUTES)
                next_head = next_df.loc[next_df.index <= cutoff]
                if not next_head.empty:
                    stitched_df = pd.concat([stitched_df, next_head], axis=0)

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

                partial_reason = None
                try:
                    cycle_pressing_threshold = get_cycle_pressing_threshold(press_number)
                    t_series = clean_numeric_column(cycle_df[tonnage_col]).fillna(0)
                    cycle_tc_avg = calculate_tc_average(cycle_df, tc_cols) if tc_cols else pd.Series(0, index=cycle_df.index)

                    file_start = original_file_start
                    file_end = original_file_end
                    grace = timedelta(minutes=FILE_BOUNDARY_GRACE_MINUTES)

                    if cycle_df.index.min() <= (file_start + grace):
                        if t_series.iloc[0] > cycle_pressing_threshold and float(cycle_tc_avg.iloc[0]) >= TC_TEMP_THRESHOLD:
                            partial_reason = "Partial cycle (started before file began)"
                    if partial_reason is None and cycle_df.index.max() >= (file_end - grace):
                        if t_series.iloc[-1] > cycle_pressing_threshold or float(cycle_tc_avg.iloc[-1]) >= TC_TEMP_THRESHOLD:
                            partial_reason = "Partial cycle (ended after file stopped)"
                except Exception:
                    partial_reason = None

                soak_text = None
                open_time = detect_press_open_time(cycle_df, tonnage_col, press_number=press_number, minutes_required=3)
                cycle_complete = is_cycle_complete(cycle_df, tonnage_col, press_number=press_number)
                if time_since_last_data >= 60 and partial_reason is None:
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

                if partial_reason is not None and open_time is None:
                    pass_status = "In Progress"
                    failure_details = [partial_reason]

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


def create_robust_chart(df, press_number, part_number, first_date, output_folder, pass_status="Unknown", failure_details=None, soak_text=None):
    """Create chart with error handling"""
    # FIXED: Avoid mutable default argument
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


# OVEN PROCESSING FUNCTIONS

def parse_oven_condition(cond_str):
    """Parse a single oven step description into a structured dict"""
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
    """Validate oven temperature series against soak/hold steps"""
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
    """Process oven file with encoding fixes"""
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
    """Create oven chart with error handling and validation status"""
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
def get_file_signature(path: str):
    """Get file signature for tracking processed files"""
    try:
        stat = os.stat(path)
        return stat.st_mtime, stat.st_size
    except FileNotFoundError:
        return None


def process_press_task(filepath, press_number, signature, key, program_df):
    """Task wrapper for parallel press file processing"""
    try:
        success = process_press_file_robust(filepath, press_number, program_df)
        return {'type': 'press', 'success': success, 'key': key, 'signature': signature, 'filepath': filepath}
    except Exception as e:
        safe_print(f"Error in press task {filepath}: {e}")
        return {'type': 'press', 'success': False, 'key': key, 'signature': signature, 'filepath': filepath}


def process_oven_task(filepath, signature, key, oven_df):
    """Task wrapper for parallel oven file processing"""
    try:
        success = process_oven_file_robust(filepath, oven_df)
        return {'type': 'oven', 'success': success, 'key': key, 'signature': signature, 'filepath': filepath}
    except Exception as e:
        safe_print(f"Error in oven task {filepath}: {e}")
        return {'type': 'oven', 'success': False, 'key': key, 'signature': signature, 'filepath': filepath}


def process_all_files_robust(processed_press_map, processed_oven_map, program_df, oven_df, startup_date):
    """Process all files with robust error handling using parallel processing"""
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


def main():
    """Main function"""
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
            safe_print(f"\nCycle start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            processed, failed, new_items = process_all_files_robust(processed_press_map, processed_oven_map, program_df, oven_df, startup_date)
            safe_print(f"Cycle summary: processed={processed}, failed={failed}")
            if not REPROCESS_ON_START:
                save_process_state(PROCESS_STATE_PATH, processed_press_map, processed_oven_map)
            if new_items == 0:
                safe_print(f"No new files. Next check in {WATCH_INTERVAL_SECONDS} seconds.")
            else:
                safe_print(f"New files processed: {new_items}. Next check in {WATCH_INTERVAL_SECONDS} seconds.")
            time.sleep(WATCH_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        safe_print("\nINFO: Process interrupted")
    except Exception as e:
        safe_print(f"\nERROR: Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
