# Project Constitution

## Behavioral Rules
- Prioritize reliability over speed
- Never guess at business logic
- "Data-First" Rule: Define JSON Data Schema (Input/Output shapes) in gemini.md before coding
- Operate within the A.N.T 3-layer architecture
- Layer 1: Architecture (SOPs, logic)
- Layer 2: Navigation (Routing, reasoning)
- Layer 3: Tools (Deterministic Python scripts)

- **Domain Rules**:
  1. "Stat", "Statistical forecast", and "Stat forecast" all mean the same thing.
  2. SAS data for each building block is divided by 1000; when splitting building blocks, you must multiply it by 100.
  3. "Final Forecast to Finance" can also be called "FFF".
  4. GCAS/FPC/APO Product is an eight digit code: all of them mean the same.
  5. SFU_SFU Version can also be called "SFU_v" and is the SKU in our context, always ensure that the numbers are split or aggregated to an SFU_v level. One SFU_v combination can have multiple GCAS/FPC/APO Product codes.
  6. The Monthly file is a SAP BW workbook output that has multiple key figures: Shipments, FFF, and Stat Forecast.
  7. The data for Shipment, FFF, Stat, and SAS is called volume

## Architectural Invariants
- 3-Layer Architecture must be strictly followed
- All external service connections must be verified in the "Link" phase
- Environment variables/tokens are stored in `.env`
- Use `.tmp/` for intermediate file operations
- Tools must be deterministic, atomic, and testable

