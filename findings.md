# Findings

## Research
- **Architecture**: The project implements a modular 3-layer architecture (SOPs, logic, tools) with clear separation for loading, splitting, salience calculation, and exception handling.
- **Workflow Change**: Steps 1 and 2 should be combined into a single "Load & Map" step with autopicking as the default.

## Discoveries
- **App Context**: "BOP Splitter" is a Streamlit application used for splitting high-level Building Block (BB/GBB) forecasts down to the SFU_v level.
- **BB Definition**: A building block is a single line in the SAS file (identified by `Plan Name` + `Brand`).
- **Data Sources**: Supports both SAP BEx BOP Excel exports and Azure Databricks (using `databricks-sql-connector`).
- **Salience & Split Logic**: Uses percentage historical weightings from Shipments, Stat, or FFF. The split engine supports per-BB split levels and exception routing.
- **Exception Management**: Allows global and per-BB exceptions (exclusions, inclusions, fixed allocations, overrides) with timestamped tracking.
- **User Experience**: The app preserves user selections, automates month picking, and provides clear validation feedback.

## Constraints
- **Autopicking Default**: The system should autopick columns by default, requiring manual intervention only for corrections.
- **Historical Data**: The SAP BW Monthly file contains Shipments, FFF, and Stat Forecast outputs.
- **State Persistence**: Only serializable session-state keys are saved in user profiles; raw DataFrames are re-derived.
- **Naming Conventions**: The system expects specific column naming for mapping and may require user intervention for ambiguous cases.
