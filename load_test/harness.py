"""
Load harness.

Producer publishes ticks onto the same redis stream but at a precisely controlled rate instead of however fast the live feed sends them.
Check if worker pool can keep backlog from growing (see where consumers stop keeping pace).

Two things are measured at each rate step:
    - queue depth (XLEN) (total entries currently in the stream)
    - pending count (XPENDING) (messages delivered to a worker but not yet acked). This grows when workers cant process messages as fast as they're arriving
"""

import logging 
import threading                    # create and control threads (multiple paths of execution with the same python process)
import time 

from dataclasses import dataclass
from mq.redis_stream import get_redis_client, publish_tick, STREAM_NAME, GROUP_NAME

logger = logging.getLogger(__name__)


@dataclass
class RateStepResult:
    target_rate: float
    duration_sec: float 
    published: int 
    actual_rate: float
    max_queue_depth: int 
    max_pending: int 
    avg_pending: float
    fell_behind: bool

def replay_at_rate(client, ticks: list, target_rate: float, duration_sec: float):
    """
    Publish ticks (cycling through given list) at ~target_rate msgs/sec for duration_sec.
    Paces itself against wall-clock time rather than sleeping a fixed interval per message (per-message
    overhead doesnt accumulate into rate drift over a long run as its small).
    """
    interval = 1.0 / target_rate if target_rate > 0 else 0              # calculates how many seconds should pass between messages 
    start = time.monotonic()                                            # returns 15234.58291 (number repping current reading of time)
    published = 0
    n = len(ticks)

    while time.monotonic() - start < duration_sec:
        publish_tick(client, ticks[published % n])
        published += 1

        if interval > 0:
            next_send_at = start + published * interval
            sleep_for = next_send_at - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)

    elapsed = time.monotonic() - start
    actual_rate = published / elapsed if elapsed > 0 else 0.0
    return published, actual_rate, elapsed


def _monitor_backlog(client, stop_event: threading.Event, samples: list, interval: float):
    """
    Background sampling loop - runs in its own thread while replay_at_rate publishes.
    """
    while not stop_event.is_set():
        try: 
            queue_depth = client.xlen(STREAM_NAME)
            pending_summary = client.xpending(STREAM_NAME, GROUP_NAME)                      # xpending returns a dict
            pending_count = pending_summary["pending"] if pending_summary else 0            # dict access. pending = number of pending messages
        except Exception as e:
            logger.warning("Backlog sample failed: %s", e)
            queue_depth, pending_count = 0, 0

        samples.append({"t": time.monotonic(), "queue_depth": queue_depth, "pending": pending_count})
        time.sleep(interval)


def detect_fell_behind(pendings: list[int], growth_ratio: float = 15.0, min_pending: int = 20) -> bool:         # growth_ratio = threshold for how much larger the second half backlog must be compared with the first half before we can say workers are behind
    """
    Heuristic: did the backlog grow substantially over the course of the step, rather than staying roughly flat?
    Compares average pending count in the first half of the sample window against the second half.
    """
    if len(pendings) < 4:
        return False
    
    mid = len(pendings) // 2
    first_half_avg = sum(pendings[:mid]) / mid
    second_half_avg = sum(pendings[mid:]) / (len(pendings) - mid)

    return second_half_avg > first_half_avg * growth_ratio and second_half_avg > min_pending


def run_rate_at_step(client, ticks: list, target_rate: float, duration_sec: float, sample_interval: float = 0.5) -> RateStepResult:
    """
    Run one load test step at a fixed target rate, sampling backlog throughout.
    """
    samples = []                                # pass this list into run_rate_step(). shared list that _monitor_backlog() fills, run_rate_step() reads
    stop_event = threading.Event()
    monitor = threading.Thread(                 # creating thread object whose job will be to run _monitor_backlog() aka watch redis backlog
        target=_monitor_backlog, args=(client, stop_event, samples, sample_interval), daemon=True
    )
    monitor.start()                             # starts executing _monitor_backlog() in separate thread

    published, actual_rate, elapsed = replay_at_rate(client, ticks, target_rate, duration_sec)

    time.sleep(sample_interval)             # one more sample after publishing stops to catch any trailing backlog workers that havent drained yet before we shut monitor down
    stop_event.set()
    monitor.join(timeout=2)

    queue_depths = [s["queue_depth"] for s in samples] or [0]
    pendings = [s["pending"] for s in samples] or [0]

    return RateStepResult(
        target_rate=target_rate,
        duration_sec=elapsed,
        published=published,
        actual_rate=actual_rate,
        max_queue_depth=max(queue_depths),
        max_pending=max(pendings),
        avg_pending=sum(pendings) / len(pendings),
        fell_behind=detect_fell_behind(pendings),
    )


def run_load_test(rates: list[float], duration_per_rate: float = 8.0, redis_host: str = "localhost") -> list[RateStepResult]:
    """
    Step through each target rate in order, running run_rate_at_step for each and collecting results.
    """
    from load_test.tick_generator import generate_synthetic_ticks

    client = get_redis_client(host=redis_host)
    ticks = generate_synthetic_ticks(2000, seed=7)

    results = []
    for rate in rates:
        logger.info("Load step: target= %s msgs/sec for %ss", rate, duration_per_rate)
        result = run_rate_at_step(client, ticks, rate, duration_per_rate)
        results.append(result)
        logger.info("Published= %d, actual=%.1f/s max_pending=%d fell_behind=%s", result.published, result.actual_rate, result.max_pending, result.fell_behind,)

    return results
