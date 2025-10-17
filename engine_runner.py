"""CLI entry point for running maintenance jobs locally."""

from __future__ import annotations

import argparse
import sys
from typing import Dict

from sms import campaign_runner
from sms.workers import ai_enrichment, autolinker_worker, intent_worker, lead_promoter


def run_autolinker(limit: int | None) -> Dict[str, int]:
    return autolinker_worker.run(limit=limit)


def run_intent_batch(limit: int | None) -> Dict[str, int]:
    return intent_worker.run(limit=limit)


def run_lead_promoter(limit: int | None) -> Dict[str, int]:
    return lead_promoter.run(limit=limit)


def run_campaigns() -> Dict[str, int]:
    return campaign_runner.run_campaigns()


def run_ai_enrichment(limit: int | None) -> Dict[str, int]:
    return ai_enrichment.run(limit=limit)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SMS engine jobs")
    parser.add_argument("--autolinker", action="store_true", help="Link conversations to prospects/leads")
    parser.add_argument("--intent-batch", action="store_true", help="Classify recent inbound messages")
    parser.add_argument("--lead-promoter", action="store_true", help="Promote qualified prospects to leads")
    parser.add_argument("--run-campaigns", action="store_true", help="Execute campaign runner")
    parser.add_argument("--ai-enrichment", action="store_true", help="Run enrichment worker")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit for batch jobs")
    parser.add_argument("--all", action="store_true", help="Run every job")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    limit = args.limit

    run_all = args.all or not any(
        [args.autolinker, args.intent_batch, args.lead_promoter, args.run_campaigns, args.ai_enrichment]
    )

    results: Dict[str, Dict[str, int]] = {}

    if args.autolinker or run_all:
        results["autolinker"] = run_autolinker(limit)
    if args.intent_batch or run_all:
        results["intent_batch"] = run_intent_batch(limit)
    if args.lead_promoter or run_all:
        results["lead_promoter"] = run_lead_promoter(limit)
    if args.run_campaigns or run_all:
        results["campaigns"] = run_campaigns()
    if args.ai_enrichment or run_all:
        results["ai_enrichment"] = run_ai_enrichment(limit)

    for name, outcome in results.items():
        print(f"{name}: {outcome}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

