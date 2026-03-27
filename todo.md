# To-Do / Pending Corrections

The following process corrections from `process_corrections.md` still need to be implemented:

## [C-901] Combine Steps 1 & 2 with Auto-Picking
- **Current behaviour:** Step 1 (Load Data) and Step 2 (Column Mapping) are separate steps in the wizard.
- **Expected behaviour:** Combine both steps together into a "Load & Map" phase. Most column assignments should work via auto-picking seamlessly, allowing the user to simply review and amend if needed before proceeding.
- **Priority:** Medium

## [C-904] Split Level SKU Selection Rules & Exclusions
- **Current behaviour:** Exclusions and SKU mapping defaults need stricter rules and bulk actions.
- **Expected behaviour:** 
  1. Implement SKU filtering logic based on Split Level (e.g., Brand splits only include SKUs belonging to that Brand).
  2. Sub Form split level should be added if required.
  3. Flag SKUs with less than 3 months of shipments to confirm if they are initiatives, then prompt to move to Exclusions.
  4. Provide the user with an option to **upload an Excel file containing a list of Excluded SKUs** as well as specific volumes that need to be loaded for those SKUs.
- **Priority:** High
