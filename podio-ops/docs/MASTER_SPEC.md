# podio-ops Master Specification

## Purpose
`podio-ops` is a production-grade automation operating system for a real estate acquisition engine powered by Podio.

## Scope
This specification governs:
- Multi-app data synchronization
- Deterministic upsert behavior
- Category option lifecycle management
- Relationship linking
- SMS safety gating
- Import pipeline orchestration
- Cinematic record renderers

## Connected Podio Apps
1. Properties
2. Owners
3. Prospects
4. Phone Numbers
5. Emails
6. Zip Codes

## Non-Functional Requirements
- Production safety first (idempotent operations and duplicate prevention)
- Modular and testable components
- Per-app token authentication (`APP_ID`, `APP_TOKEN`) only
- Deterministic behavior across retries

## Phase Plan
- **Phase 1 (current):** Repository scaffold, docs templates, Zip Codes schema baseline, import validators
- **Phase 2:** Category options fetch/ensure workflows and caching strategy
- **Phase 3:** Upsert scripts for all apps by unique keys
- **Phase 4:** Cross-app relationship linking
- **Phase 5:** GlobiFlow XML generation
- **Phase 6:** Cinematic renderer generation

## Open Inputs Required
- App-specific `external_id` values for all fields in each Podio app
- Required category fields and canonical option naming policy
- Final confirmation of unique key implementation for each app

## Definition of Done (Per Phase)
Each phase is complete only when:
1. Artifacts are generated in the target folder
2. Validation/tests pass for touched components
3. Changes are committed atomically
4. Next required inputs are explicitly requested
