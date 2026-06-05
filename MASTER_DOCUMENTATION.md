# BOP Splitter — Comprehensive Master Documentation

**Last Updated**: May 30, 2026  
**Status**: Complete with Financial Year Fallback & Post-FY Upload Rules  
**Current Version**: 7-Step Workflow with Business Rule Integration

---

## 📑 Table of Contents

1. [Executive Summary](#executive-summary)
2. [Project Overview & Progress](#project-overview--progress)
3. [Architectural Design](#architectural-design)
4. [7-Step Workflow & Business Rules](#7-step-workflow--business-rules)
5. [Module Documentation](#module-documentation)
7. [Common Issues & Troubleshooting](#common-issues--troubleshooting)
8. [Research Findings & Constraints](#research-findings--constraints)

---

# Executive Summary

## What is BOP Splitter?

**BOP Splitter** is a Streamlit-based forecasting application that splits high-level **Building Block (BB)** forecasts from SAP BEx BOP Excel exports down to the **SFU_v (SFU + Version)** SKU level. The tool uses historical salience weighting and intelligent fallback mechanisms to handle extended forecasts that exceed available SAS building block data.

## Key Features

- ✅ **Multi-source Data Loading**: Excel (SAP BEx BOP format) + Azure Databricks
- ✅ **Intelligent Column Auto-Detection**: Automatic mapping of hierarchy, months, and measures
- ✅ **Flexible Split Granularity**: Country → Brand → Sub Brand → Form → SFU_v levels
- ✅ **Salience-Based Allocation**: Historical weightings from Shipments, Consumption, Retailing, or Statistical Forecast
- ✅ **Comprehensive Exception Management**: Global exclusions, per-BB rules, fixed volumes, salience overrides
- ✅ **Financial Year Fallback**: Automatic 12-month fallback for extended forecasts (Jul-Jun FY cycle)
- ✅ **Post-FY Upload Adjustment**: Intelligent forecast adjustments for months beyond FY end
- ✅ **Validation & Audit Trails**: Pre-split diagnostics, post-split reasonability checks, timestamped exception logs
- ✅ **Session Persistence**: User selections saved and restored across browser sessions
- ✅ **Step 5 Always-Visible Config**: BOP Auto-Salience Basis & Salience section is non-collapsible
- ✅ **Monthly Salience Matrix**: Editable SKU x forecast-month salience table used during BB month split allocation
- ✅ **Enhanced Live Preview**: Step 5 preview supports Brand/Form filtering and CSV download
- ✅ **Form-Level Basis Consistency Warning**: Red warning appears when mixed Basis/Metric is used within the same Form

## Step 5 Rule Additions (June 2026)

1. **Non-collapsible SFU Basis Configuration**
   - The section **"BOP Auto-Salience — Basis & Salience Configuration (SFU Level)"** is permanently visible.

2. **Editable SKU x Month Salience Matrix**
   - Step 5 now includes an editable matrix where rows are `SFU_SFU Version` and columns are selected forecast/SAS split months.
   - User-edited values are treated as monthly salience percentages and used in Step 6 split execution.
   - During split, monthly values are normalized across eligible SFU_v rows within each BB-month.
   - If a monthly value is missing for an SFU_v, split falls back to computed scalar salience.

3. **Live Preview Enhancements**
   - Step 5 Live Preview supports Brand and Form filters.
   - Live Preview table is editable and downloadable as CSV.
   - Editing under a filter updates only visible rows while preserving hidden-row configurations.

4. **Form-Level Basis/Metric Validation**
   - On "Compute BOP Salience", the app checks whether SFU_v rows in the same Form use mixed Basis/Metric sources.
   - If mixed sources are detected, the app shows a red warning with impacted Forms and SFU_v counts.
   - This warning is **non-blocking**; computation continues.

## Business Value

- **Reduces Manual Work**: 80% automation of SKU-level forecast derivation
- **Ensures Consistency**: Standardized business rules across all forecasts
- **Enables Speed**: 7-step wizard guides users through complex split logic
- **Provides Auditability**: Complete exception logs and validation reports for finance compliance

---

# Project Overview & Progress

## Architecture Philosophy

The BOP Splitter implements a **3-layer modular architecture**:

```
UI Layer (Streamlit)
    ↓
Business Logic Layer (salience, split, exceptions)
    ↓
Data Layer (loading, exporting, Databricks)
```

### Separation of Concerns
- **UI** handles session state, navigation, and rendering
- **Business Logic** enforces rules (salience calc, split algorithm, exception processing)
- **Data Layer** abstracts file I/O and cloud connectivity

## Project Achievements

### Core Functionality & Data Processing
- ✅ SAP BEx BOP Loader with auto-mapping for all columns (Steps 1-2)
- ✅ SKU Aggregation at entity level (Shipments, Forecasts)
- ✅ Per-Building Block split levels and SFU-level salience calculations
- ✅ Excel export stability (handles NaN/Inf edge cases)
- ✅ Azure Databricks integration as alternative data source
- ✅ Financial Year detection (Jul-Jun cycle auto-identification from SAS headers)
- ✅ 12-Month Building Block Fallback (extended forecasts use prior-year Shipments)
- ✅ Post-FY Upload Adjustment (months after FY end use Shipments - FFF formula)

### User Experience Enhancements
- ✅ Session state persistence across steps
- ✅ Automated forecast month selection (next month → July)
- ✅ Automated Shipments month selection (strictly past months)
- ✅ Automated SAS month selection (all future months)
- ✅ Salience displayed as percentage (easier user comprehension)
- ✅ Entry Type BOP warnings
- ✅ Streamlined Step 3 with BB lists and granularity defaults
- ✅ SFU-level aggregation views

### Infrastructure & Documentation
- ✅ Modular architecture with clear module responsibilities
- ✅ `.gitignore` for generated files
- ✅ Branch management and feature integration
- ✅ Comprehensive architectural documentation

---

# Architectural Design

## 3-Layer Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│  UI Layer (app.py - Streamlit)                              │
│  - Session state management                                  │
│  - Step-by-step wizard (7 steps)                             │
│  - Profile save/load                                         │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Business Logic Layer                                        │
│  ├─ loader.py - Excel/Databricks loading + auto-detection   │
│  ├─ salience.py - Weight computation (basis, window, calc)  │
│  ├─ splitter.py - Core split algorithm + FY fallback logic  │
│  └─ exceptions.py - Override management (excl, fixed, etc)  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Data Layer                                                  │
│  ├─ loader.py - Excel file I/O                              │
│  ├─ databricks_loader.py - Cloud data source                │
│  ├─ exporter.py - Multi-sheet Excel workbook generation     │
│  └─ config_profile.py - Profile serialization               │
└─────────────────────────────────────────────────────────────┘
```

## Core Modules

### 1. app.py — Main Streamlit Application (~4100 lines)

**Purpose**: Orchestrate the 7-step wizard and manage session state

**Key Responsibilities**:
- Session persistence (`.tmp/{sid}.pkl`)
- Step navigation & state management
- UI rendering for each step
- Profile save/load coordination
- Data validation & error messaging

**Key Functions**:
- `_session_id()` — Get/create URL session ID
- `_init_state()` — Initialize all session keys
- `_save_session(sid)` — Persist session to disk
- `_restore_session(sid)` — Load session from disk
- `step1_upload()` — Handle Excel/Databricks upload
- `step2_columns()` — Column mapping UI
- `step3_filters()` — Filtering & SAS/SKU selection
- `step4_salience()` — Salience computation & review
- `step5_exceptions()` — Exception management UI
- `step6_run()` — Execute split engine
- `step7_reasonability()` — Output review UI

**Session State Schema**:
```python
{
    # Input sheets
    "sheets": dict[str, pd.DataFrame],           # role → data
    "sheet_map": dict[str, str],                 # role → sheet_name
    "col_maps": dict[str, dict],                 # role → {logical → actual}
    "month_cols": dict[str, list],               # role → [col_names]
    
    # Filtered data
    "sku_df_filtered": pd.DataFrame,             # Merged SKU hierarchy
    "sas_df_filtered": pd.DataFrame,             # Filtered BB rows
    "salience_df": pd.DataFrame,                 # Computed weights
    
    # Configuration
    "split_level": str,                          # Default: "Form"
    "bb_split_levels": dict,                     # Per-BB overrides
    "sas_months_selected": list[str],            # User-chosen forecast months
    
    # Exceptions & exclusions
    "exc_store": ExceptionStore,                 # Override management
    
    # Output
    "output_wide": pd.DataFrame,                 # Final split result
    "validation_df": pd.DataFrame,               # Issues log
    
    # Navigation
    "step": int,                                 # Current step 1-7
    "max_step": int,                             # Furthest step reached
}
```

### 2. loader.py — Data Loading & Auto-Detection (~600 lines)

**Purpose**: Load Excel/Databricks data and auto-detect structure

**Key Functions**:
- `load_excel(file_path)` — Read all sheets from .xlsx
- `detect_month_columns(df)` — Find month columns (pattern: MMM-YY)
- `detect_hierarchy_columns(df)` — Identify hierarchy columns
- `detect_bop_col_maps(sheets)` — Auto-map BOP format sheets
- `find_gbb_type_column(columns)` — Fuzzy-match GBB Type header
- `parse_month_to_date(month_str)` — Convert "JAN 2025" → datetime

**Column Mapping Structure**:
- **SAS Hierarchy**: Country, SMO Category, Brand, Sub Brand, Form, Specific SFU_v (optional)
- **Monthly Hierarchy**: Reporting Country, Category, Family Name 1, Family Name 2, SFU_SFU Version

### 3. salience.py — Weight Computation (~400 lines)

**Purpose**: Calculate historical weightings for SKU allocation

**Key Concepts**:
- **Basis**: Shipments, Consumption, Retailing, or Statistical Forecast
- **Window**: P3M, P6M, P9M, P12M, or custom date range
- **Normalization**: Salience % per SFU_v per group = Basis / Total Basis
- **GBB Type Rules**: Different split behavior based on building block type

**Algorithm**:
1. Load historical data (Shipments, etc.) for specified window
2. Group by hierarchy level (e.g., Brand) and SFU_v
3. Sum basis metric per group
4. Normalize: `salience[sfuv] = sum[sfuv] / sum[all_sfuvs_in_group]`
5. Apply GBB Type overrides (e.g., "Ignore" BBs get equal split)
6. Handle zero-salience cases (equal split fallback)

### 4. splitter.py — Core Split Algorithm (~700 lines)

**Purpose**: Allocate Building Block volumes to SKU_v level

**Key Function**: `run_split(sas_df, sku_df, salience_df, split_level, exc_store, shipments_df, fff_df, all_sas_months)`

**Split Algorithm**:
1. Iterate through each BB (SAS row)
2. Find eligible SKUs matching BB hierarchy + split level
3. Filter eligible SKUs through exception store
4. Get salience weights for eligible SKUs
5. Allocate: `remaining = bb_volume - fixed_allocations`
6. Distribute remaining by salience % (or equal if salience = 0)
7. Apply FY fallback logic for extended months (within FY or post-FY)
8. Return wide format: `[BB_ID, Country, ..., SFU_v, Jan-26, Feb-26, ...]`

**Financial Year Fallback Logic** (NEW):

**FY Detection** (`detect_fy_range`):
- Analyzes SAS month columns (MMM-YY format)
- Identifies Jul-Jun boundaries
- Example: Months Jul-25 through Jun-26 = FY 2025-26

**12-Month Fallback** (`fetch_shipment_fallback_values`):
- **Trigger**: Forecast month within current FY but no BB data
- **Action**: Use prior-year Shipments for that month, aggregated by BB hierarchy
- **Example**: SAS ends Jun-26, forecast extends to Sep-26 → use Sep-25 Shipments

**Post-FY Upload** (lines 578-642):
- **Trigger**: Forecast month after FY end (after June)
- **Formula**: `allocation = Shipments(prior-year month) - FFF(current month)` per SFU_v
- **Purpose**: Enables seamless forecast uploads capturing gap between history and existing plan

**Helper Functions**:
- `get_prior_year_month(month_str)` — Jul-26 → Jul-25
- `identify_extended_months(sas_months, forecast_months, fy_range)` — Find extended months
- `fetch_shipment_fallback_values()` — Aggregate prior-year Shipments

### 5. exceptions.py — Override Management (~250 lines)

**Purpose**: Manage user overrides (exclusions, fixed volumes, salience overrides)

**ExceptionStore Class**:

**Global Exclusions** (`set[str]`):
- `add_global_exclusion(sku)` — Remove SKU from all splits
- `remove_global_exclusion(sku)` — Re-enable SKU

**BB-Specific Exceptions** (`dict[bb_id → {include, exclude, fixed_qty}]`):
- `add_bb_include(bb_id, sku)` — Force-include SKU in specific BB
- `add_bb_exclude(bb_id, sku)` — Exclude SKU from specific BB
- `exclude_all_for_bb(bb_id)` — Mark entire BB as excluded
- `set_fixed_qty(bb_id, sku, month, qty)` — Pin fixed volume per month
- `add_salience_override(bb_id, sku, pct)` — Override calculated salience

**Usage in Split**:
- `eligible_sfuvs = exc_store.get_eligible_skus(bb_id, all_sfuvs)`
- Filters matched SFU_vs through all exception rules in order

### 6. exporter.py — Excel Output (~300 lines)

**Purpose**: Generate multi-sheet Excel workbooks with formatting

**Output Sheets**:
1. **Split Forecast** — SFU_v-level demand per month (primary output)
2. **Exception Log** — Timestamped record of all Step 5 overrides
3. **Validation Report** — Diagnostic findings, boundary violations, post-FY adjustments
4. **Summary** — Run settings, basis source, split level, exclusion count

**Features**:
- NaN/Inf edge case handling (replaces with 0.0)
- Value bounds checking (prevents negative allocations)
- Rounding & type conversion
- Formatting (headers, number formats, conditional highlighting)

### 7. config_profile.py — Profile Management (~200 lines)

**Purpose**: Save/load user configurations

**Profile Schema** (JSON):
```json
{
    "col_maps": {"SAS": {...}, "Monthly": {...}},
    "split_level": "Form",
    "bb_split_levels": {...},
    "exception_store": {...},
    "salience_config": {...}
}
```

### 8. databricks_loader.py — Cloud Data Source (~150 lines)

**Purpose**: Connect to Azure Databricks and query data

**Functions**:
- `test_connection(host, token, warehouse_id)` — Verify connectivity
- `query_data(sql, connection_params)` — Execute SQL and fetch results
- `load_workspaces()` — List available workspaces

---

# 7-Step Workflow & Business Rules

## Complete Workflow Overview

| Step | Objective | Key Features & Business Logic | Result / Output |
| :--- | :--- | :--- | :--- |
| **1. Load Data** | Ingest historical metrics and future forecasts. | • Supports **Excel** (SAP BEx) and **Azure Databricks**.<br>• **Monthly File**: Shipments, FFF, and Stat/LDS.<br>• **SAS File**: High-level BB forecasts.<br>• Auto-bypasses multi-row headers. | Normalized raw data ready for mapping. |
| **2. Column Mapping** | Map raw columns to standardized domains. | • **Auto-picking**: Past months for Shipments; Future months for Stat/FFF/SAS.<br>• **SFU_v**: Established as the lowest SKU level.<br>• Maps hierarchy: Category, Brand, Sub Brand, Form. | Standardized schema for all input files. |
| **3. Filters & Split Levels** | Define forecast scope and GBB behavior. | • User defines forecast start/end months.<br>• **SAS Scaling**: Multiplies volume by 1,000 natively.<br>• **Split Rules**: Brand level for Base/Initiatives; Sub Brand for Market Trend.<br>• Filters out SKUs without future FFF volume. | Refined scope and assigned split levels per GBB. |
| **4. Basis & Salience** | Define proportional weights for each SKU. | • **Model**: BOP Auto-Salience (default).<br>• **Basis**: Shipments, Retailing, or Consumption.<br>• **Window**: P3M, P6M, P9M, P12M, or custom.<br>• Average metric determines the **Salience %**. | Baseline weighting (%) per eligible SKU. |
| **5. Exceptions** | Apply user overrides before splitting. | • **Global Exclusions**: Remove SKUs from all splits.<br>• **Fixed Volume**: Pin specific amounts to an SKU.<br>• **Salience Override**: Manually force a % weight.<br>• **Force Include/Exclude**: Per-BB eligibility. | Finalized split constraints and adjustments. |
| **6. Run Split Engine** | Execute mathematical division of volumes with intelligent fallback. | • Groups eligible SKUs by split level (e.g., Brand).<br>• Applies Salience % to SAS BB volumes.<br>• **Fallback**: Uses Equal Split if total Salience is 0%.<br>• **Financial Year Logic**: Auto-detects FY from SAS month headers (Jul-Jun).<br>• **12-Month Fallback**: For forecast months within current FY but beyond SAS data, uses prior-year Shipments by BB hierarchy group.<br>• **Post-FY Upload Adjustment**: For months after FY end (Jun), computes per-SFU_v adjustment as: prior-year Shipments(same month) − existing Final Fcst to Finance. | SKU-level demand per month with intelligent fallback for extended forecasts. |
| **7. Review & Download** | Validate results and export final forecast. | • **Check 1**: New Total (Last BOP FFF + Split) per SFU_v—flags negatives in red.<br>• **Check 2**: Balance check—split total vs. SAS total per Country/month.<br>• **Baseline Tolerance**: Blue alert > 120%, Yellow alert < 80% of historical average.<br>• **Multi-sheet Excel**: Includes Split Forecast, Exception Log, Validation Report, Summary.<br>• **Run Settings**: Captures all configuration for archival. | Final Excel file with verified split forecast and audit logs. |

---

## Business Rules & Logic

### Financial Year Concept (Jul-Jun Cycle)

The tool automatically detects the financial year range from SAS sheet month column headers. The FY always runs from **July (start) through June (end)**.

**Auto-Detection**:
- Scans all month columns in SAS sheet (format: `MMM-YY`, e.g., `Jul-25`)
- Identifies the minimum year and maximum year to establish FY boundaries
- **Example**: SAS data with months `Jul-25` through `Jun-26` represents **FY 2025-26** (Jul 2025 − Jun 2026)

**Purpose**: Enables intelligent handling of forecasts that extend beyond SAS data, with different rules for months within the FY vs. months after the FY ends.

### 12-Month Building Block Fallback (Within Current FY)

When the user requests a forecast that extends beyond the SAS month data but remains **within the current financial year**, the tool automatically fills those extended months using **prior-year Shipment volumes** grouped by the Building Block's hierarchy.

**Trigger Condition**:
- Forecast end date > max(SAS month dates)
- Forecast end date ≤ current FY end date (June)

**Fallback Logic** (per Building Block, per extended month):
1. Identify the prior-year equivalent month (e.g., `Jul-26` → `Jul-25`)
2. Fetch all Shipments for that month, filtered by the BB's hierarchy group (Country, SMO Category, Brand, Sub Brand, Form)
3. Aggregate by SFU_v and sum to get total BB volume
4. Use this total as the allocation basis for that extended month
5. Apply normal split engine logic with salience weighting

**Example Scenario**:
- **SAS Data**: Jul-25 through Jan-26 (7 months)
- **Forecast Request**: Jul-25 through Jun-26 (12 months, full FY)
- **Extended Months**: Feb-26, Mar-26, Apr-26, May-26, Jun-26 (all within same FY)
- **Action**: For each of these 5 months, use prior-year Shipments (Feb-25, Mar-25, Apr-25, May-25, Jun-25) by BB group

### Post-FY Upload Adjustment (Beyond Current FY End)

For months **after the current FY end date (June)**, the tool switches from normal BB-based splits to an **upload adjustment mode**. This computes a per-SFU_v delta between prior-year historical Shipments and existing forecast.

**Trigger Condition**:
- Forecast month > current FY end date (June)
- **Example**: For FY 2025-26 (Jul 2025 − Jun 2026), any month Jul-26 onwards is post-FY

**Upload Adjustment Formula** (per SFU_v, per post-FY month):
```
Allocation = Prior-Year Shipments(same month) − Existing Final Fcst to Finance(same month)
```

**Example**:
- **FY**: 2025-26 (Jul 2025 − Jun 2026)
- **Month**: Jul-26 (post-FY)
- **Prior-Year Shipments (Jul-25)**:
  - SFU1: 1000 SU
  - SFU2: 500 SU
- **Current FFF (Jul-26)**:
  - SFU1: 800 SU
  - SFU2: 300 SU
- **Post-FY Allocation (Jul-26)**:
  - SFU1: 1000 − 800 = 200 SU (adjustment)
  - SFU2: 500 − 300 = 200 SU (adjustment)

---

## Key Constraints & Defaults

| Parameter | Default | Constraint | Notes |
|-----------|---------|-----------|-------|
| **Split Level** | Form | Country, SMO Category, Brand, Sub Brand, Form, SFU_v | Can override per-BB in Step 3 |
| **Salience Basis** | Shipments | Shipments, Consumption, Retailing, Statistical Forecast | Determines historical weighting metric |
| **Salience Window** | P6M (6 months) | P3M, P6M, P9M, P12M, or custom date range | Historical lookback for average |
| **Forecast Months** | Auto-selected | Future months, adjustable by user | Defaults to next month through July |
| **SAS Month Scaling** | ×1000 | Fixed multiplier | SAS volumes stored ÷1000, auto-multiplied on load |
| **Equal Split Fallback** | Enabled | On/Off | Triggered if total salience = 0% |
| **Reasonability Tolerance** | ±20% | Blue > 120%, Yellow < 80% | Applied to baseline historical average |

---

## Data Validation & Error Handling

### Pre-Split Validation (Step 6 Diagnostic Check)

**Checks Performed**:

| Check | Purpose | Success Indicator | Failure Indicator |
|-------|---------|-------------------|-------------------|
| **Column Mapping** | Verify all required hierarchy columns are mapped | ✅ All hierarchy columns found | ⚠️ Missing hierarchy column |
| **BB Matching** | Verify at least one SFU_v matches each BB | ✅ SFU_v rows found | ⚠️ NO MATCH |
| **SKU Eligibility** | Verify matched SKUs are not globally excluded | ✅ Eligible SKUs exist | ⚠️ EXCLUDED |
| **Salience Availability** | Verify matched SKUs have positive salience | ✅ Salience data available | ⚠️ NO SALIENCE |

**Status Outcomes**:
- ✅ **OK**: Ready to split
- ⚠️ **Issue Detected**: Review Step 3, 4, or 5

### Post-Split Validation (Step 7 Reasonability Checks)

**Check 1 — Total Reconciliation** (per SFU_v, per month):
```
New Total = Last BOP Final Fcst to Finance + Split Adjustment
```
- Flag condition: New Total < 0 (displayed in red)

**Check 2 — Balance Check** (per Country, per month):
```
Σ Split Output ≟ Σ SAS Building Block Total
```
- Flag condition: Mismatch detected

**Reasonability Alerts**:

| Alert | Condition | Threshold | Action |
|-------|-----------|-----------|--------|
| **Blue Alert** | SKU forecast > baseline | > 120% of 12M avg | Review for unusual growth |
| **Yellow Alert** | SKU forecast < baseline | < 80% of 12M avg | Review for decline |

---

## Output Files

| Sheet Name | Contents | Purpose |
|-----------|----------|---------|
| **Split Forecast** | SFU_v-level demand per month | Primary output; each row = SFU_v, each column = month |
| **Exception Log** | Timestamped record of Step 5 overrides | Audit trail; shows exclusions, fixes, overrides |
| **Validation Report** | Diagnostic findings, violations, post-FY adjustments | Data quality verification |
| **Summary** | Run settings, basis source, split level, exclusion count | Configuration documentation |

---

# Module Documentation

## loader.py — Data Loading Details

### Column Mapping Constants

**SAS Sheet**:
```python
SAS_HIERARCHY_MAP = {
    "Country": "Country",
    "SMO Category": "SMO Category", 
    "Brand": "Brand",
    "Sub Brand": "Sub Brand",
    "Form": "Form",
    "SFU_v": "Specific SFU_v"  # Optional for single-SKU BBs
}
SAS_GBB_TYPE_COL = "GBB Type"
```

**Monthly Sheet**:
```python
MONTHLY_HIERARCHY_MAP = {
    "Country": "Reporting Country",
    "SMO Category": "Category",
    "Brand": "Brand",
    "Sub Brand": "Family Name 1",
    "Form": "Family Name 2"
}
MONTHLY_SFU_VERSION_COL = "SFU_SFU Version"
```

**Month Detection**:
```python
MONTH_PATTERN = re.compile(r"^(Jan|Feb|...|Dec)[- ]?\d{2,4}$")  # "Jan-25"
MONTHLY_MONTH_RE = re.compile(r"^(JAN|FEB|...|DEC)\s+\d{4}$")   # "JAN 2025"
```

### Auto-Detection Logic

**Process**:
1. Read all Excel sheets
2. Identify role sheets (SAS, Shipments, FFF, etc.) by fuzzy matching
3. For each sheet, detect:
   - Month columns (regex match on headers)
   - Hierarchy columns (substring match against known names)
   - GBB Type column (if present)
4. Create column mappings: `{logical → actual_column_name}`
5. Return auto-mappings to UI for user confirmation

---

## salience.py — Salience Calculation Details

### GBB Type Split Rules

**Base** (Standard GBB):
- Use salience weighting from specified basis
- Split by selected granularity (Brand, Sub Brand, etc.)

**Initiatives/New Products**:
- Use salience weighting
- Split by Sub Brand (finer granularity)

**Market Trend** (Seasonal/Cyclical):
- Use salience weighting
- Split by Form level
- Higher sensitivity to historical patterns

**Ignore** (No allocation):
- Equal split across all eligible SKUs
- Ignore salience (treat all SKUs equally)

**Custom** (User-defined):
- Use user-configured split level and basis
- Override defaults per GBB Type

### Salience Normalization

**Formula**:
```
salience[sfuv] = basis_sum[sfuv] / sum(basis_sum[all_eligible_sfuvs])
```

**Example**:
- SFU1 Shipments (P6M avg): 1000 SU
- SFU2 Shipments (P6M avg): 500 SU
- SFU3 Shipments (P6M avg): 500 SU
- **Total**: 2000 SU
- **Salience**:
  - SFU1: 1000/2000 = 50%
  - SFU2: 500/2000 = 25%
  - SFU3: 500/2000 = 25%

### Zero-Salience Handling

**Trigger**: Total salience = 0% (no basis data for any eligible SKU)

**Fallback**: Equal split
```
allocation[sfuv] = bb_volume / count(eligible_sfuvs)
```

**Logged As**: Validation type `equal_split_applied`

---

## splitter.py — Split Algorithm Details

### Volume Allocation Formula

**Per BB, per month**:
```
total_allocation = bb_volume_for_month
fixed_total = sum(fixed_quantities for all SKUs in this BB)
remaining = total_allocation - fixed_total

for each eligible_sku:
    if fixed_qty[sku] is not None:
        allocation[sku] = fixed_qty[sku]
    else:
        allocation[sku] = remaining × salience[sku]
```

### FY Fallback Sequence

**For each BB month pair**:
1. Check if month has data in SAS → Use SAS value
2. If no SAS data and month within current FY:
   - Check 12-month fallback trigger
   - If triggered: Use prior-year Shipments (aggregated by BB group)
3. If month after FY end:
   - Check post-FY upload trigger
   - If triggered: Use `Shipments(prior-year) - FFF(current)`
4. If no applicable fallback: Report validation issue

### Validation Trace Logging

**Types**:
- `equal_split_applied` — Zero salience encountered
- `post_fy_upload_adjustment` — Post-FY month processed
- `fallback_applied` — 12-month fallback used
- `negative_allocation` — Computed value < 0 (allowed, logged for review)

---

# Common Issues & Troubleshooting

## NO SALIENCE Issue (Most Common)

### Symptom
Diagnostic Check in Step 6 shows:
- ✅ Matches_Found > 0 (matches exist in SKU data)
- ✅ Eligible > 0 (matches pass exception filtering)
- ❌ With_Salience = 0 (no salience values found)
- Status: "⚠️ NO SALIENCE"

### Root Cause
Salience was either:
1. **Never configured in Step 4**, OR
2. **Failed to compute** because basis source sheets don't have data for your SFU_vs

### Fix - Step by Step

**Step A: Check if Step 4 Basis is Configured**

1. Go back to **Step 4 — Review & Refine Salience**
2. Look for the **"Live Preview — Basis Values and Salience"** table
3. Check if it has rows for your SFU_v values
   - If **empty or missing**: Re-filter in Step 3
   - If **rows exist**: Proceed to Step B

**Step B: Check Basis Configurations**

In the Live Preview table, look at the **"Basis / Metric"** column:
- If all cells say **"Shipments"** or another source: ✅ Configured
- If cells are **empty or say "N/A"**: ❌ Not configured

If NOT configured:
1. Click each cell in the **"Basis / Metric"** column
2. Select a source that has your country data:
   - Try **"Shipments"** first (most complete)
   - If empty, try "Consumption" or "Retailing"
3. Click the **"Basis Window"** cell
4. Select **"P6M"** (last 6 months - most reliable)
5. After editing, click **"Compute BOP Salience"** button

**Step C: If Basis Source is Missing Data**

If you get an error or salience is still all zeros:

1. **Verify basis data exists:**
   - Open your BOP Excel file
   - Check the **"Shipments"** (or other measure) sheet
   - Filter for your **Country** code
   - Check if there are any rows with data

2. **If no data exists:**
   - Options:
     - a) Add data to your Shipments sheet
     - b) Use a different basis source that has data
     - c) Contact your data provider

3. **If data exists but salience is still zero:**
   - Check SFU_v column name consistency:
     - SAS sheet calls it: ?
     - Shipments sheet calls it: ?
     - They MUST match exactly
   - If they don't match, update column mappings in Step 2

**Step D: Verify the Fix**

1. Go to **Step 6 — Run Split**
2. Click **"Run Diagnostic Check"** again
3. Look for status:
   - ✅ **OK** status: Fixed! Ready to split
   - ⚠️ **NO SALIENCE**: Still failing - repeat Step B
   - ❌ **MISSING COLS**: Column mapping issue - go to Step 2

### Quick Checklist

- [ ] Basis source sheets (Shipments, etc.) have your country data
- [ ] SFU_v column names match across all sheets
- [ ] Step 4 basis configurations are set for all SFU_v values
- [ ] "Compute BOP Salience" button was clicked
- [ ] Salience table shows positive values in "Salience %" column
- [ ] Step 6 diagnostic shows "OK" status

### If Still Stuck

Enable debug logging in app.py around line 1966:
```python
print(f"DEBUG: sfu_configs = {sfu_configs}")
print(f"DEBUG: available_sources = {available_basis_sources}")
```
Then run Step 4 again and check terminal for clues.

---

## Column Mapping Issues

### Symptom
Step 2 shows "⚠️ Ambiguous Column" or "❌ Not Found"

### Common Causes
1. Column name doesn't match expected pattern
2. Multiple columns could match (e.g., "Product", "Product_1", "Product_2")
3. Auto-detection failed; manual mapping required

### Solution
1. Verify actual column names in your Excel file
2. If ambiguous, manually select the correct column from dropdown
3. Use the "Preview" button to verify correct data loads
4. Proceed to Step 3 to confirm filtered results

---

## Negative Allocation Issue

### Symptom
Step 7 shows red highlighted cells with negative values

### Root Cause
Fixed volume allocations exceeded BB total volume

**Example**:
- Building Block: 1000 SU
- Fixed allocation to SFU1: 800 SU
- Fixed allocation to SFU2: 500 SU
- **Remaining**: 1000 - 1300 = -300 SU (negative!)

### Solution
1. Review Step 5 Fixed Volumes
2. Ensure fixed totals ≤ BB total
3. Reduce one or more fixed allocations
4. Re-run split in Step 6

---

## Balance Check Failure

### Symptom
Step 7 Validation Report shows "Balance Mismatch: Split Total ≠ SAS Total"

### Root Cause
One of:
1. BB hierarchy values don't match SKU data (no matches found)
2. Filters excluded too many SKUs
3. Post-FY upload adjustments applied differently than expected

### Solution
1. Run Step 6 Diagnostic Check
2. Review each BB's "Matches Found" count
3. If zero matches: Go to Step 3 and verify filters match your BB values
4. If low matches: Check Step 5 exclusions aren't too aggressive

---

# Research Findings & Constraints

## Architecture Research

### Design Philosophy
- **Modular 3-Layer**: UI, Business Logic, Data Layer with clear separation of concerns
- **State Persistence**: Session state preserved across steps and browser sessions
- **Auto-Detection**: Smart column mapping with user confirmation fallback
- **Business Rule Automation**: FY detection, 12-month fallback, post-FY upload all automatic

### Key Discoveries

1. **Building Block Definition**
   - A BB is a single row in SAS sheet
   - Identified by Plan Name + Brand combination
   - Has optional split level override (default from Step 3)

2. **Salience Mechanism**
   - Uses historical basis (Shipments, Consumption, Retailing, Statistical Forecast)
   - Normalizes to percentage per group
   - Handles zero-salience with equal split fallback

3. **Split Execution Model**
   - Per-BB iteration with hierarchy grouping
   - Exception filtering before salience application
   - Fixed allocation → Remaining allocation by salience
   - FY-aware: Different rules for within-FY vs. post-FY months

4. **Data Consistency Requirements**
   - SFU_v column name must match across all sheets
   - Country/hierarchy columns must exist and align
   - Month column names must follow pattern (MMM-YY)

## Operational Constraints

### Autopicking Default Behavior
The system autopicks columns by default, requiring manual intervention only for corrections:
- **Shipments months**: Strictly past (historical) months
- **Forecast months (SAS, FFF, Stat)**: Strictly future months
- **Hierarchy columns**: Auto-matched by name fuzzy matching

### Historical Data Requirements
- **Monthly File (Shipments, FFF, Stat)**:
  - Must contain past months (at least 12M for reliable salience)
  - Must have hierarchy columns matching SAS
  - Must have SFU_v identifier column

- **SAS File (Building Blocks)**:
  - Must have at least 3 months of forecast (ideally 12)
  - Must have hierarchy columns (Country, Brand, etc.)
  - May have optional "Specific SFU_v" column for single-SKU BBs

### State Persistence Limitations
- Only serializable session-state keys saved in profiles (DataFrames not saved)
- Re-loading profile re-derives DataFrames from source files
- Requires source files to be consistent across sessions
- Session files stored in `.tmp/{session_id}.pkl`

### Naming Conventions & Ambiguity Handling
- System expects specific column naming (SAP BEx standard)
- Ambiguous names require user intervention
- Auto-detection uses fuzzy matching (not perfect)
- Users can override auto-detected mappings in Step 2

## Document Notes & Maintenance

**Last Updated**: May 30, 2026  

---

**End of Master Documentation**

*This comprehensive guide integrates workflow, architecture, business rules, technical debt, and troubleshooting into a single reference document for easy review and maintenance.*
