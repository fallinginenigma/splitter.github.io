# Findings


## Research
- The project implements a robust SKU-level splitter for BOP (Business Operation Planning) data, supporting both SAP BEx Excel exports and Azure Databricks sources.
- The architecture is modular, with clear separation for loading, splitting, salience calculation, exception handling, and exporting.


## Discoveries
- **Data Handling**: The loader supports flexible month and hierarchy column detection, auto-mapping, and normalization of various date formats.
- **Salience & Split Logic**: Salience is calculated per business block and can be overridden or set to user-defined levels. The split engine supports per-BB split levels and exception routing.
- **Exception Management**: The system allows global and per-BB exceptions, including exclusions, inclusions, fixed allocations, and salience overrides, all tracked with timestamps.
- **Databricks Integration**: Native support for fetching tables from Azure Databricks using the `databricks-sql-connector`.
- **Export**: Output is generated as a multi-sheet Excel file, with robust handling for NaN/Inf values and custom formatting.
- **User Experience**: The Streamlit app preserves user selections, automates month picking, and provides clear warnings and validation feedback.


## Constraints
- Only serializable session-state keys are saved in user profiles; raw DataFrames are always re-derived.
- The system expects specific column naming conventions for mapping and may require user intervention for ambiguous cases.
- Databricks integration requires the optional `databricks-sql-connector` package.
