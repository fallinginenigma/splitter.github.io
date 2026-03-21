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


## Architectural Invariants
- 3-Layer Architecture must be strictly followed
- All external service connections must be verified in the "Link" phase
- Environment variables/tokens are stored in `.env`
- Use `.tmp/` for intermediate file operations
- Tools must be deterministic, atomic, and testable

