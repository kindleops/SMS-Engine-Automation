# Data Dictionary Template

## Instructions
Populate this file with **actual Podio field metadata** once `external_id` inputs are provided.

---

## App: Zip Codes
| Field Label | external_id | Field Type | Required | Category? | Notes |
|---|---|---|---|---|---|
| Zip Code | `zip_code` (proposed unique key) | text/number | TBD | No | Unique intelligence-layer record key |
| Market Grade | TBD | category | TBD | Yes | Market strength classification |
| Crime Index | TBD | number | TBD | No | Public safety score |
| Median Rent | TBD | money/number | TBD | No | Rental market baseline |
| YoY Appreciation % | TBD | number | TBD | No | Home value growth |
| Distress Ratio | TBD | number | TBD | No | Foreclosure/distress pressure |
| Population | TBD | number | TBD | No | Demographic mass |
| Median Household Income | TBD | money/number | TBD | No | Economic signal |
| Unemployment Rate | TBD | number | TBD | No | Labor market signal |

---

## App: Owners
| Field Label | external_id | Field Type | Required | Category? | Notes |
|---|---|---|---|---|---|
| Seller ID | TBD | text | TBD | No | Proposed unique key |

## App: Prospects
| Field Label | external_id | Field Type | Required | Category? | Notes |
|---|---|---|---|---|---|
| Seller ID | TBD | text | TBD | No | Part of composite unique key |
| Linked Owner | TBD | app-reference | TBD | No | Part of composite unique key |

## App: Phone Numbers
| Field Label | external_id | Field Type | Required | Category? | Notes |
|---|---|---|---|---|---|
| Phone Hidden | TBD | text | TBD | No | Proposed unique key |

## App: Emails
| Field Label | external_id | Field Type | Required | Category? | Notes |
|---|---|---|---|---|---|
| Email Hidden | TBD | text | TBD | No | Proposed unique key |

## App: Properties
| Field Label | external_id | Field Type | Required | Category? | Notes |
|---|---|---|---|---|---|
| Property ID | TBD | text | TBD | No | Proposed unique key |
