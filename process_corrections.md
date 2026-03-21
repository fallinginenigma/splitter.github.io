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
| Brand Building Activities | Form | split |
| Promotions - Go To Market | Form | exceptions |
| New Channels | Form | exceptions |
| Initiatives | Form | ignore |
| Pricing Strategy | Brand | split |
| Market Trend | Sub Brand | split |
| Customer Inventory Strategy | Form | split (user-defined) |

**Current default forecast month range:** next calendar month → July of the same year (or next year if already past July).

<!-- Add your corrections below this line -->


---

## Step 4 — Basis & Salience

> Covers: default salience method, BOP auto-salience logic, basis time window options, manual salience override table.

**Current salience methods:**
1. **Equal split** (default) — `1/N` per SFU_v in the split group.
2. **BOP Auto-Salience** — per SFU_SFU Version, choose source + time window → mean historical value proportional weight.
3. **Historical Override** — same as above but at APO-Product level for any mapped sheet.

**Current basis time window options:** last_3, last_6, last_9, last_12, manual selection.

<!-- Add your corrections below this line -->


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

Combine both Step 1 and 2 together, also - most of this should work on autopicking and the user can review and change if needed.

General Rules:

  1. "Stat", "Statistical forecast", "LDS", and "Stat forecast" all mean the same thing.
  2. "Final Forecast to Finance" can also be called "FFF".
  3. SAS data for each building block is divided by 1000, therefore before splitting the building blocks, multiply it by 1000.
  4. Each line in SAS is called a building block, GBBs, and have GBB Type and volume across the months
  4. GCAS/FPC/APO Product is an eight digit code: all of them mean the same.
  5. SFU_SFU Version can also be called "SFU_v" and is the SKU in our context, always ensure that the numbers are split or aggregated to an SFU_v level. One SFU_v combination can have multiple GCAS/FPC/APO Product codes.
  6. The Monthly file is a SAP BW workbook output that has multiple key figures: Shipments, FFF, and Stat Forecast.
  7. The data for Shipment, FFF, Stat, and SAS is called volume. The unit of measure is SU.
  8. When splitting the GBBs, this would be the general thumb rule:
    
