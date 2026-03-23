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
  - **Terminology Pairs**: 
    - "Stat" = "Statistical forecast" = "LDS" = "Stat forecast"
    - "Final Forecast to Finance" = "FFF"
    - "GCAS" = "FPC" = "APO Product" (8-digit code)
    - "SFU_SFU Version" = "SFU_v" (The SKU level)
  - **Data Scaling**: SAS data is divided by 1000; MUST multiply by 1000 before splitting.
  - **Entities**: Each SAS line is a "Building Block" (BB/GBB) with volume across months.
  - **UoM**: All volume data (Shipment, FFF, Stat, SAS) is in Statistical Units (SU).
  - **aggregation**: One SFU_v can map to multiple GCAS codes. Always split/aggregate to SFU_v level.


## Architectural Invariants
- 3-Layer Architecture must be strictly followed
- All external service connections must be verified in the "Link" phase
- Environment variables/tokens are stored in `.env`
- Use `.tmp/` for intermediate file operations
- Tools must be deterministic, atomic, and testable

