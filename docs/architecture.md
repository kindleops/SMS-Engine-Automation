# REI SMS Engine Architecture

## Layered Module Order

1. `sms.runtime` – shared logging, timing, retry, and phone helpers.
2. `sms.datastore` – schema-aware Airtable access with in-memory fallbacks.
3. `sms.autoresponder` – intent detection, quiet-hour enforcement, drip queue orchestration.
4. `sms.inbound_webhook` – FastAPI handler that normalises inbound payloads and logs conversations.
5. `sms.textgrid_sender` – TextGrid transport + outbound logging.
6. `sms.message_processor` – high-level send orchestrator (reused by campaign/retry flows).
7. `sms.campaign_runner` – dequeues Drip Queue records and dispatches outbound messages.
8. `sms.retry_runner` – retries failed outbound deliveries with exponential backoff.
9. `sms.workers.*` – batch utilities (`autolinker`, `intent_worker`, `lead_promoter`, `ai_enrichment`).

## Inter-Module Dependencies

```
              ┌────────────────────┐
              │    sms.runtime     │
              └────────┬───────────┘
                       │
              ┌────────▼────────┐
              │  sms.datastore  │◄─────────────┐
              └───▲───────┬─────┘              │
                  │       │                    │
        ┌─────────┘   ┌───┴──────────┐        │
        │             │ sms.templates │        │
        │             └──────────────┘        │
        │                                      │
┌───────▼──────────┐   ┌──────────────────┐   │
│ sms.inbound_*    │   │ sms.autoresponder│   │
└───────┬──────────┘   └───────┬──────────┘   │
        │                      │              │
        │                      ▼              │
        │            ┌──────────────────┐     │
        │            │ sms.message_...  │─────┤
        │            └───────┬──────────┘     │
        │                    │                │
        ▼                    ▼                │
┌────────────────┐   ┌──────────────────┐     │
│sms.campaign_runner│ │ sms.retry_runner│─────┘
└────────┬────────┘   └────────┬────────┘
         │                     │
         ▼                     ▼
      ┌────────────────────────────────────┐
      │         sms.textgrid_sender        │
      └────────────────────────────────────┘

workers: sms.workers.autolinker / intent_worker / lead_promoter / ai_enrichment
  ↳ depend on `sms.datastore` (TableFacade) and reuse `sms.autoresponder.classify_intent` where needed.
```

Every component interacts with Airtable exclusively through `sms.datastore`, ensuring schema-driven field resolution and safe retries while allowing the test suite to swap in the lightweight `FakeTable` adapters.
