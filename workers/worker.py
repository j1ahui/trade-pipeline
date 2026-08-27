"""
Worker processes.

Each worker is a standalone process (not a thread - real multiprocessing, each with its own Python interpreter and no shared memory).
that pulls ticks off the Redis stream and writes them to SQLite.

A worker doesnt know that ingest is written with asyncio, it only speaks the queues language (read a batch, write it, ack)
"""

import logging 
import multiprocessing                      # tools for creating and controlling separate processes

from mq.redis_stream import get_redis_client, ensure_consumer_groups, read_batch, ack
from storage.sqlite_store import TickStore

logger = logging.getLogger(__name__)


def run_worker(worker_id: int, stop_event: multiprocessing.Event, redis_host: str = "localhost"):               # multiprocessing.Event = multiprocessing event object
    """
    The function that each worker process runs.
    Loops reading batches from the stream, writing them to storage, acking, until told stop.

    stop_event is a multiprocessing.Event (standard way to signal "shut down" across process boundaries, 
    since worker processes dont share memory with the parent and cant just check a variable)
    """
    consumer_name = f"worker-{worker_id}"
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [worker-{worker_id} %(message)s]")             # asctime = time wen log message was created. returns 2026-08-27 16:01:23,456 [worker-1 Worker started]. message = messaged passed to logger.info(), logger.warning()

    client = get_redis_client(host=redis_host)
    ensure_consumer_groups(client)
    store = TickStore()                         # each worker opens its own SQLite connection

    written = 0
    logger.info("Worker %d started, consumer name '%s'", worker_id, consumer_name)

    try:
        while not stop_event.is_set():              # is_set() = method of event object
            batch = read_batch(client, consumer_name, count=10, block_ms=1000)

            if not batch:                           # when batch returns [], return to start of while loop and try reading again
                continue

            for message_id, tick in batch:
                store.write_tick(tick)
                ack(client, message_id)
                written += 1

            if written % 50 < len(batch):
                logger.info("Written %d ticks so far", written)

    finally:
        store.close()
        logger.info("Worker %d shutting down, wrote %d ticks total", worker_id, written)


def spawn_workers(n_workers: int, redis_host: str = "localhost"):
    """
    Start n_workers worker processes and return (processes, stop_event).

    Caller is responsible for calling stop_event.is_set() and then joining the processes when its time to shut down
    """
    stop_event = multiprocessing.Event()                # creating event object. each worker is intended to be a separate python process
    processes = []

    for worker_id in range(n_workers):
        p = multiprocessing.Process(                    # creating Process object
            target=run_worker, args=(worker_id, stop_event, redis_host), daemon=False, name=f"Cute worker-{worker_id}"       # target = specifies which function the new process should execute. args (which is a tuple) = args that will be passed into run_worker(). daemon process = process that is tied to the lifetime of its parent process (dependent)
        )   
        p.start()                                       # .start() = method of multiprocessing.Process class. tells python to create and stat a separate operating system process
        processes.append(p)

    return processes, stop_event


def stop_workers(processes: list[multiprocessing.Process], stop_event: multiprocessing.Event, timeout: int = 5):
    """
    Signal all workers to stop and then wait for them to exit cleanly.
    """
    stop_event.set()

    for p in processes:
        p.join(timeout=timeout)         # join = join back to main process
        if p.is_alive():
            logger.warning("Worker %s did not exit cleanly, terminating", p.name)               # name property is a Process class attribute
            p.terminate()

            