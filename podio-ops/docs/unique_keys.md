# Unique Key Contract

## Goal
Guarantee idempotent upserts and duplicate prevention across all Podio apps.

## Proposed Unique Keys (Pending Confirmation)
| App | Unique Key Field(s) | Key Type | Status |
|---|---|---|---|
| Properties | `property-id` | Single-field | Pending confirmation |
| Owners | `seller-id` | Single-field | Pending confirmation |
| Prospects | `seller-id` + `linked-owner` | Composite | Pending confirmation |
| Phone Numbers | `phone-hidden` | Single-field | Pending confirmation |
| Emails | `email-hidden` | Single-field | Pending confirmation |
| Zip Codes | `zip_code` | Single-field | Pending confirmation |

## Upsert Rules
1. Query Podio by the app's unique key.
2. If an item is found, update the existing item.
3. If no item is found, create a new item.
4. Never create a second record for an existing key.

## Open Questions
- Confirm exact Podio `external_id` for each key field.
- For `Prospects`, confirm deterministic composite-key serialization format (e.g., `<seller-id>::<owner-item-id>`).
