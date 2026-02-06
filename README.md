# Maverick Validator

Automated HMI data processor and validation system for Maverick Molding hydraulic presses and curing ovens. Continuously monitors press and oven data files, detects production cycles, validates process conditions against program specifications, and generates PDF chart reports with pass/fail status.

## Overview

`hmi_processor_NEW_v2.py` runs as a long-lived service that:

1. **Watches** for new or updated HMI data files across Presses 3-8 and Ovens 1-3
2. **Parses** tab-delimited press logs and CSV oven logs (with UTF-16 encoding support)
3. **Detects production cycles** by analyzing tonnage and thermocouple (TC) temperature patterns
4. **Validates** each cycle against program specifications from Excel reference files
5. **Generates** PDF reports (time-series charts + data tables) with Pass / Fail / In Progress status
6. **Creates failure reports** with detailed diagnostics when validation fails

## Cycle Detection Logic

The processor splits continuous HMI data into discrete production cycles using these rules:

- **Cycle boundaries** are identified when tonnage is idle AND all active thermocouples drop below 399 F for a sustained period (>= 5 minutes)
- **Part number changes** always create a new cycle boundary
- **Minimum cycle requirements**: max temperature must reach 600 F and duration must exceed 15 minutes
- **Adjacent file stitching**: previous/next day's data files are prepended/appended to handle cycles that span midnight

## Validation

Press cycle validation checks extracted from Excel program specifications:

- **Temperature targets** - all active TCs must reach specified temperature ranges
- **Tonnage targets** - applied tonnage must match spec (tons/tool x tool quantity, +/- 3% tolerance)
- **Hold durations** - tonnage + temperature must be sustained for the specified hold time
- **Soak durations** - continuous time within a temperature band

Oven validation checks ramp rates, soak temperatures, soak durations, and hold conditions.

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
  PythonScripts\        # This script + Excel reference files

M:\Quality\Press Charts\
  Press_3\
    Results_<part>_<date>_<time>\
      chart.pdf           # Time-series chart with validation status
      <original_file>.txt # Filtered cycle data
      FAILURE_REPORT.txt  # Detailed failure diagnostics (failures only)
  ...

M:\Quality\Furnace Chart\
  <prefix>_<date>\
    chart.pdf
    <original_file>.csv
```

## Configuration

All settings are configurable via environment variables with sensible defaults:

| Variable | Default | Description |
|---|---|---|
| `HMI_BASE_WATCH_PATH` | `C:\HMI_Upload` | Root directory for HMI data files |
| `HMI_OUTPUT_BASE` | `M:\Quality\Press Charts` | Output directory for press results |
| `HMI_OVEN_OUTPUT_BASE` | `M:\Quality\Furnace Chart` | Output directory for oven results |
| `HMI_EXCEL_PATH` | `C:\HMI_Upload\PythonScripts` | Directory containing Excel reference files |
| `HMI_WATCH_INTERVAL_SECONDS` | `30` | Polling interval between scan cycles |
| `HMI_MAX_WORKER_THREADS` | `10` | Max parallel file processing threads |
| `HMI_DEFAULT_LOOKBACK_HOURS` | `35` | How far back to look for files on startup |
| `HMI_LOOKBACK_DAYS` | _(empty)_ | Override lookback in days |
| `HMI_FORCE_REPROCESS` | `0` | Reprocess all files regardless of change |
| `HMI_REPROCESS_ON_START` | `1` | Clear processed-file cache on startup |
| `HMI_PURGE_OUTPUTS_ON_START` | `0` | Delete existing outputs before reprocessing |
| `HMI_CYCLE_PAD_MINUTES` | `3` | Minutes to expand cycle window around pressing |
| `HMI_MAX_REALISTIC_TONNAGE` | `600` | Max valid tonnage value (filters spikes) |
| `HMI_CYCLE_VALID_MIN_MAX_TEMP` | `600` | Min peak temperature for a valid cycle (F) |
| `HMI_CYCLE_VALID_MIN_DURATION_MIN` | `15` | Min cycle duration in minutes |
| `HMI_MAX_TIME_GAP_MINUTES` | `5` | Max gap between data points before split |
| `HMI_MIN_FILE_AGE_SECONDS` | `10` | Wait for file to stabilize before processing |
| `HMI_FILE_REPROCESS_THROTTLE_SECONDS` | `120` | Min seconds between reprocessing same file |
| `HMI_TONNAGE_UNRELIABLE_PRESSES` | _(empty)_ | Comma-separated press numbers with unreliable tonnage signals |

## Dependencies

- Python 3.x
- pandas
- matplotlib
- openpyxl

## Usage

Run as a continuous service:

```bash
python hmi_processor_NEW_v2.py
```

Run a one-time reprocess of all files:

```bash
python hmi_processor_NEW_v2.py --__reprocess_worker
```

## Excel Reference Files

The processor loads program specifications from two Excel workbooks (searched in order, first found wins):

- **Press programs**: `Copy of Form# 0337 - SuperImide Auto Press Programs Rev D_FIXED.xlsx` or `Copy of Form# 0337 - SuperImide Auto Press Programs Rev D.xlsx` (sheet: "Program Detail")
- **Oven cycles**: `OvenCyclesMaverick_FIXED.xlsx` or `OvenCyclesMaverick.xlsx` (sheet: "OvenCycles")
