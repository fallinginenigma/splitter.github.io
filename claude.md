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

## Architectural Invariants
- 3-Layer Architecture must be strictly followed
- All external service connections must be verified in the "Link" phase
- Environment variables/tokens are stored in `.env`
- Use `.tmp/` for intermediate file operations
- Tools must be deterministic, atomic, and testable

