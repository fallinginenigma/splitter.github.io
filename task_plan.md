# Task Plan

## Phase 1: Blueprint (Vision & Logic)
- [ ] Answer Discovery Questions
- [ ] Blueprint approved

## Phase 2: Link (Connectivity)
- [ ] Verify all API connections and `.env` credentials
- [ ] Build minimal scripts to verify external service responses

## Phase 3: Architect (The 3-Layer Build)
- [ ] Define Architecture SOPs (`architecture/`)
- [ ] Implement Navigation Logic
  - [ ] Combine Step 1 (Upload) and Step 2 (Mapping) into a single "Load & Review" phase.
  - [ ] Implement smart "autopick" defaults to minimize manual column mapping.
- [ ] Implement Tools (`tools/`)
  - [ ] Add SAS data multiplication factor (x1000) to the processing engine.
  - [ ] Update entity aggregation logic to handle multi-GCAS per SFU_v.

## Phase 4: Stylize (Refinement & UI)
- [ ] Format outputs and payloads
- [ ] Implement UI/UX (if applicable)
- [ ] Present stylized results for feedback

## Phase 5: Trigger (Deployment)
- [ ] Move logic to production cloud environment
- [ ] Setup automation/triggers
