"""
Diagnostic script to identify why salience is missing for Indonesia SFU_vs.
Run this after loading your BOP Excel file in Steps 1-3.
"""
import pandas as pd
import streamlit as st

def diagnose_salience_issue():
    """Check why salience computation is failing for Indonesia."""
    
    print("\n" + "="*80)
    print("SALIENCE DIAGNOSTIC REPORT")
    print("="*80)
    
    # 1. Check if session state exists
    if not hasattr(st, 'session_state'):
        print("❌ No session state found. Run Steps 1-3 first.")
        return
    
    sku_df = st.session_state.get("sku_df_filtered")
    sal_df = st.session_state.get("salience_df")
    sfu_configs = st.session_state.get("sfu_basis_sources", {})
    sheets = st.session_state.get("sheets", {})
    
    # 2. Check filtered SFU data
    print("\n1. FILTERED SFU DATA (sku_df_filtered):")
    if sku_df is None or sku_df.empty:
        print("   ❌ Empty or None")
    else:
        print(f"   ✅ {len(sku_df)} rows")
        # Check for Indonesia
        if "Country" in sku_df.columns:
            id_count = (sku_df["Country"] == "ID").sum()
            print(f"   └─ Indonesia (Country=ID): {id_count} rows")
        # Check SFU_v columns
        sfu_cols = [c for c in sku_df.columns if "sfu" in c.lower() or "sku" in c.lower()]
        print(f"   └─ SFU_v-like columns: {sfu_cols}")
        
    # 3. Check basis source availability
    print("\n2. BASIS SOURCE SHEETS:")
    available_sources = [s for s in ["Shipments", "Consumption", "Retailing", "Sellout"] if s in sheets]
    print(f"   Available: {available_sources}")
    if not available_sources:
        print("   ❌ No basis source sheets found!")
    else:
        for src in available_sources:
            src_df = sheets.get(src)
            if src_df is None or src_df.empty:
                print(f"   └─ {src}: ❌ Empty")
            else:
                print(f"   └─ {src}: {len(src_df)} rows")
                # Check for Indonesia
                if "Country" in src_df.columns:
                    id_count = (src_df["Country"] == "ID").sum()
                    print(f"      └─ Indonesia rows: {id_count}")
                # Check for SFU_v column
                sfu_cols = [c for c in src_df.columns if "sfu" in c.lower() or "sku" in c.lower()]
                print(f"      └─ SFU_v-like columns: {sfu_cols}")
    
    # 4. Check basis configurations
    print("\n3. STEP 4 BASIS CONFIGURATIONS (sfu_basis_sources):")
    if not sfu_configs:
        print("   ❌ EMPTY - No basis configurations set in Step 4!")
        print("   👉 ACTION: Go to Step 4 and select Basis/Metric and Basis Window for each SFU_v")
    else:
        print(f"   ✅ Configured for {len(sfu_configs)} SFU_v(s):")
        for sfu_v, cfg in list(sfu_configs.items())[:5]:  # Show first 5
            print(f"      • {sfu_v}: {cfg.get('source', 'N/A')} ({cfg.get('mode', 'N/A')})")
        if len(sfu_configs) > 5:
            print(f"      ... and {len(sfu_configs) - 5} more")
    
    # 5. Check current salience table
    print("\n4. CURRENT SALIENCE TABLE (salience_df):")
    if sal_df is None or sal_df.empty:
        print("   ❌ EMPTY - No salience data computed!")
        if sfu_configs:
            print("   👉 ACTION: Click 'Compute BOP Salience' button in Step 4")
        else:
            print("   👉 ACTION: Set basis configurations first, then compute")
    else:
        print(f"   ✅ {len(sal_df)} rows")
        if "SFU_v" in sal_df.columns:
            unique_sfuv = sal_df["SFU_v"].nunique()
            print(f"      └─ Unique SFU_v: {unique_sfuv}")
        if "salience" in sal_df.columns:
            zero_sal = (sal_df["salience"] == 0).sum()
            pos_sal = (sal_df["salience"] > 0).sum()
            print(f"      └─ Salience > 0: {pos_sal} rows")
            print(f"      └─ Salience = 0: {zero_sal} rows")
            if pos_sal == 0:
                print("      ❌ ALL SALIENCE VALUES ARE ZERO!")
        if "Country" in sal_df.columns:
            id_sal = (sal_df["Country"] == "ID").sum()
            print(f"      └─ Indonesia rows: {id_sal}")
    
    # 6. Recommendation
    print("\n5. RECOMMENDATION:")
    if not sfu_configs:
        print("   ⚠️  STEP 4 NOT CONFIGURED")
        print("   ")
        print("   Steps to fix:")
        print("   1. Go to Step 4")
        print("   2. In the 'Live Preview — Basis Values and Salience' table:")
        print("      • Select 'Basis / Metric' (e.g., Shipments) for each SFU_v")
        print("      • Select 'Basis Window' (e.g., P6M for last 6 months)")
        print("   3. Click 'Compute BOP Salience' button")
    elif sal_df is None or sal_df.empty:
        print("   ⚠️  SALIENCE NOT COMPUTED")
        print("   ")
        print("   Steps to fix:")
        print("   1. Go back to Step 4")
        print("   2. Review the basis configurations again")
        print("   3. Click 'Compute BOP Salience' button")
        print("   4. If error appears, check that basis source sheets have data for your SFU_vs")
    elif sal_df is not None and (sal_df.get("salience", pd.Series([])) == 0).all():
        print("   ⚠️  ALL SALIENCE VALUES ARE ZERO")
        print("   ")
        print("   Likely causes:")
        print("   1. Basis data has all zeros or NaN for your SFU_vs")
        print("   2. SFU_v column names don't match between SAS and basis sources")
        print("   3. No basis data exists for the selected country/hierarchy")
        print("   ")
        print("   Steps to investigate:")
        print("   1. Check basis source sheets manually for your country (ID)")
        print("   2. Verify SFU_v values in SAS sheet match those in basis sheets")
        print("   3. Try different basis sources (Consumption, Retailing, etc.)")
    
    print("\n" + "="*80)

if __name__ == "__main__":
    # This is meant to be run interactively in the Streamlit app
    diagnose_salience_issue()
