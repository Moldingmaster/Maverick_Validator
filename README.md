# Maverick Validator

Automated HMI data processor and validation system for Maverick Molding hydraulic presses and curing ovens. Continuously monitors press and oven data files, detects production cycles, validates process conditions against program specifications, and generates PDF chart reports with pass/fail status.

## Files

| File | Description |
|---|---|
| `Validation_py_Engine.py` | **Primary script** - fully documented version with comprehensive comments explaining every function, configuration option, and design decision |
| `hmi_processor_NEW_v2.py` | Original working version (same logic, fewer comments) |

## Overview

The processor runs as a long-lived service that:

1. **Watches** for new or updated HMI data files across Presses 3-8 and Ovens 1-3
2. **Parses** tab-delimited press logs and CSV oven logs (with UTF-16 encoding support)
3. **Detects production cycles** by analyzing tonnage and thermocouple (TC) temperature patterns
4. **Stitches cross-midnight data** - automatically combines data from adjacent daily files so cycles that span midnight are evaluated as a single continuous cycle
5. **Validates** each cycle against program specifications from Excel reference files
6. **Generates** PDF reports (time-series charts + data tables) with Pass / Fail / In Progress status
7. **Creates failure reports** with detailed diagnostics when validation fails (deleted automatically when a cycle later passes)
8. **Maintains a results index** - per-press `results_index.csv` and a combined `results_index_all.csv` for reporting

## Key Concepts

- **Press**: A hydraulic press that applies tonnage at temperature to cure composite parts
- **Cycle**: One complete press run for a part number - heat up, apply tonnage, hold at temp/pressure for a specified time, then cool down and release
- **Thermocouple (TC)**: Temperature sensor embedded in the press tooling. Different press models log TCs as `TC1`-`TC4` (standard) or `Z1TC1A2A`-style zone columns (Press 5)
- **Tonnage**: The hydraulic force applied by the press, measured in tons. Different presses have different idle baselines (Press 5 and 7 idle above zero)
- **Part Number / Program**: Identifies the product being pressed. The Excel spec sheet maps each program number to its required temps, tonnages, and hold times
- **Soak**: A sustained hold at a target temperature range for a minimum continuous duration

## Cycle Detection Logic

The processor splits continuous HMI data into discrete production cycles using these rules:

- **Cycle boundaries** are identified when tonnage is idle AND all active thermocouples drop below 399 F for a sustained period (>= 5 minutes)
- **Part number changes** always create a new cycle boundary
- **Minimum cycle requirements**: max temperature must reach 600 F and duration must exceed 15 minutes
- **Cross-midnight file stitching**: data from the previous day's file (up to 2 hours) and next day's file (up to 12 hours) is automatically prepended/appended so cycles that span the midnight file boundary are processed as one complete cycle - no "partial cycle" failures
- **Duplicate prevention**: only cycles whose start time falls within the original file's time range are processed, preventing the same cycle from being detected in both the original and stitched file

## Validation

Press cycle validation checks are extracted from Excel program specifications:

- **Temperature targets** - all active TCs must reach specified temperature ranges
- **Tonnage targets** - applied tonnage (median of pressing period) must match spec within a tolerance band
  - Default tolerance: **+-3%** of calculated total tonnage (tons/tool x tool count)
  - Dynamic widening: if the observed pressing median is close but just outside +-3%, the tolerance widens automatically up to a maximum of **+-6%** to account for normal process variation
- **Hold durations** - tonnage + temperature must be sustained simultaneously for the specified hold time
- **Soak durations** - continuous time within a specified temperature band (uses average of active TCs)
- **Tool count auto-detection** - if the observed tonnage is far from the expected total (>1.8x ratio), the script infers the actual tool count from the median pressing tonnage and adjusts validation ranges accordingly

Oven validation checks ramp rates, soak temperatures, soak durations, and hold conditions.

## Failure Reports

- A `FAILURE_REPORT.txt` is created in the result folder only when a cycle **fails** validation
- Contains: press number, part number, failure reasons, actual vs. expected data, and cycle time range
- **Automatically deleted** when a cycle is re-evaluated and passes (no manual cleanup needed)
- No archived failure reports are retained - only the current status matters

## Status Outcomes

| Status | Meaning |
|---|---|
| **Pass** | All program conditions were met before the press opened |
| **Fail** | Cycle completed but one or more conditions were not satisfied |
| **In Progress** | Cycle is still running or data is still being collected |

## Directory Structure

```
C:\HMI_Upload\
  Press_3\              # Raw HMI data files (tab-delimited .txt)
  Press_4\
  ...
  Press_8\
  Oven_1\               # Raw oven data files (.csv)
  Oven_2\
  Oven_3\
  PythonScripts\        # Script + Excel reference files + state file

M:\Quality\Press Charts\
  results_index_all.csv   # Combined index of all press cycle results
  Press_3\
    results_index.csv     # Per-press index of cycle results
    Results_<part>_<date>_<time>\
      chart.pdf           # Time-series chart with validation status
      STATUS.txt          # Current pass/fail/in-progress status
      FAILURE_REPORT.txt  # Detailed failure diagnostics (failures only)
      <original_file>.txt # Filtered cycle data
  ...

M:\Quality\Furnace Chart\
  <prefix>_<date>\
    chart.pdf
    <original_file>.csv
```

## Configuration

All settings are configurable via environment variables with sensible defaults:

### Paths

| Variable | Default | Description |
|---|---|---|
| `HMI_BASE_WATCH_PATH` | `C:\HMI_Upload` | Root directory for HMI data files |
| `HMI_OUTPUT_BASE` | `M:\Quality\Press Charts` | Output directory for press results |
| `HMI_OVEN_OUTPUT_BASE` | `M:\Quality\Furnace Chart` | Output directory for oven results |
| `HMI_EXCEL_PATH` | `C:\HMI_Upload\PythonScripts` | Directory containing Excel reference files |
| `HMI_PROCESS_STATE_PATH` | `<EXCEL_PATH>/hmi_processor_state.json` | JSON file tracking processed files |

### Polling and Threading

| Variable | Default | Description |
|---|---|---|
| `HMI_WATCH_INTERVAL_SECONDS` | `30` | Polling interval between scan cycles |
| `HMI_MAX_WORKER_THREADS` | `10` | Max parallel file processing threads |
| `HMI_MIN_FILE_AGE_SECONDS` | `10` | Wait for file to stabilize before processing |
| `HMI_FILE_STABLE_SECONDS` | `10` | File stability check threshold |
| `HMI_FILE_REPROCESS_THROTTLE_SECONDS` | `120` | Min seconds between reprocessing same file |

### Lookback and Reprocessing

| Variable | Default | Description |
|---|---|---|
| `HMI_DEFAULT_LOOKBACK_HOURS` | `35` | How far back to look for files on startup |
| `HMI_LOOKBACK_DAYS` | _(empty)_ | Override lookback in days |
| `HMI_LOOKBACK_HOURS` | _(empty)_ | Override lookback in hours |
| `HMI_FORCE_REPROCESS` | `0` | Reprocess all files regardless of change |
| `HMI_REPROCESS_ON_START` | `1` | Clear processed-file cache on startup |
| `HMI_PURGE_OUTPUTS_ON_START` | `0` | Delete existing outputs before reprocessing |
| `HMI_SPAWN_REPROCESS_WORKER` | `0` | Spawn a background reprocess worker on startup |
| `HMI_CLEAN_FAILURE_REPORTS_ON_START` | `1` | Remove stale failure reports on startup |

### Cycle Detection Thresholds

| Variable | Default | Description |
|---|---|---|
| `HMI_CYCLE_PAD_MINUTES` | `3` | Minutes to expand cycle window around pressing |
| `HMI_MAX_REALISTIC_TONNAGE` | `600` | Max valid tonnage value (filters sensor spikes) |
| `HMI_MAX_REALISTIC_TEMP_F` | `1500` | Max valid temperature in F (filters sensor errors) |
| `HMI_CYCLE_VALID_MIN_MAX_TEMP` | `600` | Min peak temperature for a valid cycle (F) |
| `HMI_CYCLE_VALID_MIN_DURATION_MIN` | `15` | Min cycle duration in minutes |
| `HMI_MAX_TIME_GAP_MINUTES` | `5` | Max gap between data points before split |
| `HMI_STALE_IN_PROGRESS_MINUTES` | `180` | Timeout for stale in-progress cycles |
| `HMI_MIN_ACTIVE_TC_SAMPLES` | `5` | Min non-zero readings for a TC to be "active" |
| `HMI_MIN_CYCLE_PRESSING_TONNAGE` | `5` | Min tonnage for a segment to count as pressing |

### Press-Specific Overrides

| Variable | Default | Description |
|---|---|---|
| `HMI_PRESS5_IDLE_TONNAGE_DISPLAY` | `10` | Press 5 idle baseline (non-zero due to hydraulics) |
| `HMI_PRESS7_IDLE_TONNAGE` | `10` | Press 7 idle baseline |
| `HMI_PRESS7_MIN_PRESSING_TONNAGE` | `11` | Press 7 min tonnage to count as pressing |
| `HMI_PRESS7_MIN_CYCLE_TONNAGE` | `20` | Press 7 min tonnage for a valid production cycle |
| `HMI_TONNAGE_UNRELIABLE_PRESSES` | _(empty)_ | Comma-separated press numbers to skip tonnage validation |

### File Stitching

| Variable | Default | Description |
|---|---|---|
| `HMI_PREPEND_PREV_FILE_MINUTES` | `120` | Minutes of previous day's data to stitch |
| `HMI_APPEND_NEXT_FILE_MINUTES` | `720` | Minutes of next day's data to stitch |

## Code Documentation

`Validation_py_Engine.py` is organized into clearly labeled sections:

1. **Imports** - Each import group annotated with why it's needed
2. **Configuration** - Every env var documented with its purpose and rationale
3. **State Persistence** - JSON-based tracking of processed files across restarts
4. **Background Workers** - Optional subprocess spawning for bulk reprocessing
5. **General Utilities** - Safe printing, failure report cleanup, date code parsing
6. **File Encoding** - Multi-strategy UTF-16/UTF-8 decoding for HMI-exported files
7. **Press Data Loading** - Tab-delimited file parsing with auto-detected column layouts
8. **Excel Spec Loading** - Reading press program and oven cycle specifications
9. **Tool Quantity Lookup** - Fuzzy column matching to find cavity/tool counts
10. **Condition Extraction** - Regex parsing of free-text step descriptions into structured validation rules
11. **Cycle Detection Engine** - Splitting continuous data into discrete production cycles
12. **Validation Functions** - Checking actual sensor data against spec requirements
13. **Output Generation** - PDF charts, failure reports, status files, and CSV audit logs
14. **Oven Processing** - Simpler temperature-only validation for furnace cycles
15. **Parallel Processing** - ThreadPoolExecutor-based concurrent file processing
16. **Main Entry Point** - Polling loop, startup configuration, and worker management

## Dependencies

- Python 3.x
- pandas
- matplotlib
- openpyxl

## Usage

Run as a continuous service:

```bash
python Validation_py_Engine.py
```

Run a one-time reprocess of all files:

```bash
python Validation_py_Engine.py --__reprocess_worker
```

Override lookback window:

```bash
HMI_LOOKBACK_HOURS=72 python Validation_py_Engine.py
```

Force full reprocess with clean outputs:

```bash
HMI_FORCE_REPROCESS=1 HMI_PURGE_OUTPUTS_ON_START=1 python Validation_py_Engine.py --__reprocess_worker
```

## Excel Reference Files

The processor loads program specifications from two Excel workbooks (searched in order, first found wins):

- **Press programs**: `Copy of Form# 0337 - SuperImide Auto Press Programs Rev D_FIXED.xlsx` or `Copy of Form# 0337 - SuperImide Auto Press Programs Rev D.xlsx` (sheet: "Program Detail")
- **Oven cycles**: `OvenCyclesMaverick_FIXED.xlsx` or `OvenCyclesMaverick.xlsx` (sheet: "OvenCycles")
