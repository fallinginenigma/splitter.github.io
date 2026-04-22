# NO SALIENCE Fix Guide

## Symptom
Diagnostic Check in Step 6 shows all BBs with:
- ✅ Matches_Found > 0 (matches exist in SKU data)
- ✅ Eligible > 0 (matches pass exception filtering)
- ❌ With_Salience = 0 (no salience values found)
- Status: "⚠️ NO SALIENCE (N eligible, but no salience data)"

## Root Cause
Salience was either:
1. **Never configured in Step 4**, OR
2. **Failed to compute** because basis source sheets don't have data for your SFU_vs

## Fix - Execute These Steps

### Step A: Check if Step 4 Basis is Configured

1. Go back to **Step 4 — Review & Refine Salience**
2. Look for the **"Live Preview — Basis Values and Salience"** table
3. Check if it has rows for your SFU_v values
   - If **empty or missing**: Your SFU data wasn't filtered properly in Step 3. Go back and re-filter.
   - If **rows exist**: Proceed to Step B

### Step B: Check Basis Configurations (If table exists)

In the Live Preview table, look at the **"Basis / Metric"** column:
- If all cells say **"Shipments"** or another source: ✅ Configured
- If cells are **empty or say "N/A"**: ❌ Not configured

If NOT configured:
1. Click each cell in the **"Basis / Metric"** column
2. Select a source that has Indonesia data:
   - Try **"Shipments"** first (most complete)
   - If empty, try "Consumption" or "Retailing"
3. Click the **"Basis Window"** cell
4. Select **"P6M"** (last 6 months - most reliable)
5. After editing, click **"Compute BOP Salience"** button

### Step C: If Basis Source is Missing Indonesia Data

If you get an error or salience is still all zeros:

1. **Verify Indonesia data exists in your basis sources:**
   - Open your BOP Excel file
   - Check the **"Shipments"** (or other measure) sheet
   - Filter for **Country = ID**
   - Check if there are any rows with data

2. **If no Indonesia data exists:**
   - ❌ **This is the root cause** 
   - Options:
     - a) Add Indonesia data to your Shipments sheet
     - b) Use a different basis source that has Indonesia data
     - c) Contact your data provider for Indonesia basis data

3. **If Indonesia data exists but salience is still zero:**
   - Check SFU_v column name consistency:
     - SAS sheet calls it: ?
     - Shipments sheet calls it: ?
     - They MUST match exactly
   - If they don't match, update the column mappings in Step 2

### Step D: Verify the Fix

1. Go to **Step 6 — Run Split**
2. Click **"Run Diagnostic Check"** again
3. Look for the results:
   - ✅ **OK** status: Fixed! Ready to split
   - ⚠️ **NO SALIENCE**: Still failing - repeat Step B
   - ❌ **MISSING COLS**: Column mapping issue - go to Step 2

## Quick Checklist

- [ ] Basis source sheets (Shipments, etc.) have Indonesia (Country=ID) data
- [ ] SFU_v column names match across all sheets (SAS, Shipments, etc.)
- [ ] Step 4 basis configurations are set for all SFU_v values
- [ ] "Compute BOP Salience" button was clicked
- [ ] Salience table shows positive values in "Salience %" column (not all zeros)
- [ ] Step 6 diagnostic shows "OK" status instead of "NO SALIENCE"

## If Still Stuck

Enable debug logging:
```python
# In app.py, around line 1966 in _compute_bop_salience():
print(f"DEBUG: sfu_configs = {sfu_configs}")
print(f"DEBUG: available_sources = {available_basis_sources}")
```

Then run Step 4 again and check terminal output for clues.
