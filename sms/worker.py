import os, time
from sms.campaign_runner import run_campaigns
from sms.outbound_batcher import send_batch
from sms.autoresponder import run_autoresponder
from sms.retry_runner import run_retry
from sms.metrics_tracker import update_metrics

while True:
    # queue new work
    run_campaigns(limit=None, send_after_queue=False)

    # send within rules (quiet hours + rate limiter live here)
    send_batch(limit=500)

    # followups / retries / metrics
    run_retry(limit=100)
    run_autoresponder(limit=50, view="Unprocessed Inbounds")
    update_metrics()

    time.sleep(30)  # every 30s