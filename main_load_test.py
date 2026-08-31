"""
Tier 3 load test entry point

This file replays synthetic ticks at increasing rates through the exact same queue and worker pipeline built in Tier 2.
The load harness doesnt care where ticks came from, only how fast the pipeline can absorb them.
"""

import logging

from workers.worker import spawn_workers, stop_workers
from load_test.harness import run_load_test
from load_test.report import print_summary, save_report, save_chart

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RATES_TO_TEST = [50, 200, 500, 1000, 2000, 5000]
DURATION_PER_RATE = 8.0
N_WORKERS = 3 


def main():
    logger.info("Spawning %d worker processes for load test...", N_WORKERS)
    processes, stop_event = spawn_workers(N_WORKERS)

    try: 
        results = run_load_test(RATES_TO_TEST, duration_per_rate=DURATION_PER_RATE)
    finally:
        stop_workers(processes, stop_event)
        logger.info("Workers stopped.")

    print_summary(results)
    save_report(results)
    save_chart(results)

if __name__ == "__main__":
    main()