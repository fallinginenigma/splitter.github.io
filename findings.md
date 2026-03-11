# Findings

## Research
*(Pending)*

## Discoveries
- **App Context (Extracted from Claude log)**: The existing codebase is a Streamlit application ("BOP Splitter") that takes high-level "Building Block" (BB) forecasts (from SAP BEx BOP Excel exports or Databricks) and splits them down to the SKU level.
- **Data Entities**: 
  - `BB_ID` is defined as `Plan Name` + `Brand`
  - `SFU_SFU Version` is used for aggregation and matching (as opposed to APO Product)
- **Salience Calculation**: It uses "salience %" (historical weightings) based on past actuals (Shipments) or forecasts (Stat, FFF) to execute the split.
- **Workflow Steps**: The app uses a 7-step wizard (Upload->Map Columns->Filters->Salience->Exceptions->Run->Download).

## Constraints
*(Pending)*
