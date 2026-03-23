# BOP Splitter — Process Corrections & Expectations

Use this file to note where the current tool behaviour differs from what you expect.
Each section maps to a step in the process. Add your corrections under the relevant
section and share with the development team (or AI assistant) to implement them.

Format for each correction:
```
### [C-XXX] Short title
- **Current behaviour:** what the tool does today
- **Expected behaviour:** what you want it to do
- **Where in code:** file + constant/function name (optional, fill if known)
- **Priority:** High / Medium / Low
```

---

## Step 1 — Load Data

> Covers: file upload, BOP auto-detection, Databricks connection, sheet-to-role mapping.

<!-- Add your corrections below this line -->


---

## Step 2 — Column Mapping

> Covers: month column auto-selection rules, hierarchy column mapping, SFU_v column, BB_ID column, mapping profile save/load.

**Current auto-selection rules recap (for reference):**
| Sheet | Months selected by default |
|---|---|
| SAS | Future months only (≥ next month) |
| Shipments | Past months only (< current month start) |
| Stat Forecast / FFF / Consumption / Retailing | Future months only (> current month start) |

<!-- Add your corrections below this line -->


---

## Step 3 — Filters & Split Level

> Covers: forecast month range default, Building Block display, per-BB split level defaults, GBB Type rules.

**Current GBB Type rules (for reference):**
| GBB Type | Split Level | Action |
|---|---|---|
| 1.Brand Building Activities | Form | split |
| 2.Promotions - Go To Market | Form | exceptions |
| 3.New Channels | Form | exceptions |
| 4.Initiatives | Form | ignore |
| 5.Pricing Strategy | Brand | split |
| 6.Market Trend | Sub Brand | split |
| 8.Customer Inventory Strategy | Form | split (user-defined) |

**Current default forecast month range:** next calendar month → July of the same year (or next year if already past July).

<!-- Add your corrections below this line -->

### [C-301] Update GBB Type Split Level Defaults
- **Current behaviour:** Brand Building Activities splits at Form level; Customer Inventory Strategy splits at Form level (user-defined)
- **Expected behaviour:** Brand Building Activities should split at Brand level; Customer Inventory Strategy should split at Brand level (user-defined)
- **Where in code:** `bop_splitter/salience.py → GBB_TYPE_RULES`
- **Priority:** High

**Updated GBB Type rules:**
| GBB Type | Split Level | Action |
|---|---|---|
|0.Base | **Brand** | split |
|1. Brand Building Activities | **Brand** | split |
| 2.Promotions - Go To Market | Form | exceptions |
| 3.New Channels | Form | exceptions |
| 4.Initiatives | Form | ignore (remind in Exclusions) |
| 5.Pricing Strategy | Brand | split |
| 6.Market Trend | Sub Brand | split |
| 8.Customer Inventory Strategy | **Brand** | split (user-defined) |

> ✅ **Implemented:** GBB rules updated in `bop_splitter/salience.py`. Auto-pick now uses `_match_gbb_type()` to strip leading number prefixes (e.g. `0.Base`, `1. Brand Building Activities`) and match to the canonical rule. Reference table in Step 3 shows both numbered SAS format and canonical name.

### [C-302] Consider Only SFUs with Future FFF
- **Current behaviour:** All SFU_vs are considered during split regardless of future forecast availability
- **Expected behaviour:** Only consider SFU_vs that have FFF (Final Forecast to Finance) values in the future months during the split
- **Where in code:** `app.py → step3_filters()` and split logic
- **Priority:** High

---

## Step 4 — Basis & Salience

> Covers: default salience method, BOP auto-salience logic, basis time window options, manual salience override table.

**Current salience methods:**
1. **Equal split** (default) — `1/N` per SFU_v in the split group.
2. **BOP Auto-Salience** — per SFU_SFU Version, choose source + time window → mean historical value proportional weight.
3. **Historical Override** — same as above but at APO-Product level for any mapped sheet.

**Current basis time window options:** last_3, last_6, last_9, last_12, manual selection.

<!-- Add your corrections below this line -->

### [C-401] Remove Equal Split and Historical Override Methods
- **Current behaviour:** Three salience methods available: Equal split (default), BOP Auto-Salience (per SFU_SFU Version), and Historical Override (per APO Product)
- **Expected behaviour:** Remove methods 1 (Equal split) and 3 (Historical Override). Only keep BOP Auto-Salience method since we always need to split at SFU_v level and methods 2 & 3 are similar
- **Where in code:** `app.py → step4_salience()`
- **Priority:** Medium

**Simplified salience approach:**
- **BOP Auto-Salience (SFU_v level only)** — per SFU_SFU Version, choose source (Shipments/Consumption/Retailing) + time window (P3M/P6M/P9M/P12M/specific months) → mean historical value determines proportional weight

---

## Step 5 — Exceptions

> Covers: global exclusions, per-BB force-include / force-exclude / fixed quantity, GBB "exceptions" and "ignore" handling.

<!-- Add your corrections below this line -->


---

## Step 6 — Run Split

> Covers: split logic, pinned SFU_v handling (Specific SFU_v column), salience zero fallback, validation checks.

**Current fallback when all salience = 0:** Equal split among eligible SFU_vs, with a validation warning.

<!-- Add your corrections below this line -->


---

## Step 7 — Download / Output

> Covers: output Excel structure, sheet names, number formatting, which columns appear in output.

**Current output sheets:** BOP_Split_Forecast, Salience_Table, Exception_Log, Validation_Report, Run_Settings.

<!-- Add your corrections below this line -->

### [C-701] Add Reasonability Check Step Before Download
- **Current behaviour:** No validation step between Step 6 (Run Split) and Step 7 (Download)
- **Expected behaviour:** Add a new validation step (Step 6.5 or separate tab in Step 6) where users can review split reasonability before downloading
- **Priority:** High

**Reasonability Check Requirements:**
- Display format: SKUs (SFU_SFU Version) in rows, future demand months in columns
- Color coding logic:
  - **Blue highlight**: Demand > 120% of baseline average
  - **Yellow highlight**: Demand < 80% of baseline average
  - **No highlight**: Demand within 80-120% range
- User-configurable options:
  - Tolerance threshold (default: 80-120%)
  - Baseline calculation options:
    - Past 3 months average (default)
    - Past 6 months average
  - Baseline metric options:
    - Shipments (default)
    - Sellout/Retailing
- Table structure:
  ```
  | SFU_SFU Version | Future Month 1 | Future Month 2 | Future Month 3 | ... |
  | --------------- | -------------- | -------------- | -------------- | --- |
  | 1000966943_00   | [value]        | [value]        | [value]        | ... |
  | 1000966945_00   | [value]        | [value]        | [value]        | ... |
  ```


---

## Column & Constant Names

> Use this section if any hard-coded column names or mapping constants are wrong for your data.

**Key constants and where to change them:**

| Constant | Current Value | File | Change to |
|---|---|---|---|
| SFU_v column in Monthly | `APO Product` | `loader.py → MONTHLY_SFU_V_COL` | |
| SFU Version column | `SFU_SFU Version` | `loader.py → MONTHLY_SFU_VERSION_COL` | |
| Monthly sheet header row | `13` (0-indexed) | `loader.py → MONTHLY_HEADER_ROW` | |
| SAS Ctry column | `Ctry` | `loader.py → SAS_HIERARCHY_MAP["Ctry"]` | |
| SAS SMO Category column | `SMO Category` | `loader.py → SAS_HIERARCHY_MAP["SMO Category"]` | |
| SAS Brand column | `Brand` | `loader.py → SAS_HIERARCHY_MAP["Brand"]` | |
| SAS Sub Brand column | `Sub Brand` | `loader.py → SAS_HIERARCHY_MAP["Sub Brand"]` | |
| SAS Form column | `Form` | `loader.py → SAS_HIERARCHY_MAP["Form"]` | |
| Monthly Ctry column | `Reporting Country` | `loader.py → MONTHLY_HIERARCHY_MAP["Ctry"]` | |
| Monthly SMO Category column | `Category` | `loader.py → MONTHLY_HIERARCHY_MAP["SMO Category"]` | |
| Monthly Sub Brand column | `Family Name 1` | `loader.py → MONTHLY_HIERARCHY_MAP["Sub Brand"]` | |
| Monthly Form column | `Family Name 2` | `loader.py → MONTHLY_HIERARCHY_MAP["Form"]` | |
| BB_ID auto-generated from | `Plan Name + Brand` | `loader.py → _load_bop_openpyxl()` | |

<!-- Fill in the "Change to" column for any that are wrong -->


---

## General / Cross-Cutting

> Anything that doesn't fit neatly in one step.

<!-- Add your corrections below this line -->

### [C-901] Combine Steps 1 & 2 with Auto-Picking
- **Current behaviour:** Step 1 (Load Data) and Step 2 (Column Mapping) are separate steps
- **Expected behaviour:** Combine both steps together; most mappings should work on auto-picking with user review/override capability
- **Where in code:** `app.py → step1_upload()` and `step2_columns()`
- **Priority:** Medium

### [C-902] Terminology & Synonym Handling
Document standard terminology and synonyms used throughout the application:

**Forecast Types:**
- "Stat", "Statistical Forecast", "LDS", "Stat Forecast" → all mean the same thing
- "Final Forecast to Finance" = "FFF"

**Product Identifiers:**
- GCAS = FPC = APO Product (8-digit code) → all mean the same
- SFU_SFU Version = SFU_v = SFU = "SKU" in this context
- One SFU_v can have multiple GCAS/FPC/APO Product codes

**Data Files:**
- Monthly file = SAP BW workbook output with key figures: Shipments, FFF, Stat Forecast
- All data (Shipments, FFF, Stat, SAS) is called "volume" with unit of measure = SU (Sales Units)

**Other Terms:**
- Sellout = Retailing (interchangeable)
- Each line in SAS = Building Block = GBB (with GBB Type and volume across months)

### [C-903] SAS Volume Scaling
- **Current behaviour:** SAS data used as-is from the input file
- **Expected behaviour:** SAS data for each building block is divided by 1000 in the input file, so **multiply by 1000** before splitting
- **Where in code:** `app.py → step3_filters()` or split preprocessing
- **Priority:** High

### [C-904] Split Level SKU Selection Rules
Document which SKUs should be considered at each split level (from Monthly sheet):

| Split Level  | SKUs to Consider                           |
| ------------ | ------------------------------------------ |
| SMO Category | All SKUs that belong to that Category      |
| Brand        | All SKUs that belong to that Brand         |
| Sub Brand    | All SKUs that belong to that Family Name 1 |
| Form         | All SKUs that belong to that Family Name 2 |
| Sub Form     | All SKUs that belong to that Family Name 3 |

**Note:** Current implementation only has levels up to Form. Sub Form would need to be added if required.


- Any SKU with less than 3 months of shipment -> confirm whether it is an initiative and move it to Exclusions
- Confirm if SKUs are under Exclusions
- For Exclusions> give the user an option to upload a list of Excluded SKUs as well as the volumes that need to be loaded in those specific SKUs. Can use an Excel to upload these volumes
  