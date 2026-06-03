import time
import logging
import sys
from multiprocessing import Process, Queue, cpu_count
from logging.handlers import QueueListener
from producer import producer_loop
from worker import worker_loop


logger = logging.getLogger(__name__)


# ---------------------------
# 1. setup listener
# ---------------------------
def setup_listener(log_queue):
    handler = logging.StreamHandler(sys.stdout)

    listener = QueueListener(
        log_queue,
        handler
    )

    return listener


if __name__ == "__main__":

    # ---------------------------
    # 2. logging queue 생성
    # ---------------------------
    log_queue = Queue()

    listener = setup_listener(log_queue)
    listener.start()

    q = Queue(maxsize=1000)

    WORKERS = 2

    logger.info("Starting system")
    logger.info("Worker count=%d", WORKERS)

    # ---------------------------
    # 3. producer (log_queue 전달)
    # ---------------------------
    p = Process(
        target=producer_loop,
        args=(q, log_queue),
        name="producer"
    )
    p.start()

    logger.info("Producer started pid=%s", p.pid)

    # ---------------------------
    # 4. workers (log_queue 전달)
    # ---------------------------
    workers = []
    for i in range(WORKERS):
        w = Process(
            target=worker_loop,
            args=(q, log_queue),
            name=f"worker-{i}"
        )
        w.start()
        workers.append(w)

        logger.info("Worker-%d started pid=%s", i, w.pid)

    # ---------------------------
    # 5. monitor loop
    # ---------------------------
    try:
        while True:
            time.sleep(5)

    except KeyboardInterrupt:
        logger.warning("Shutdown signal received")

        # 1. producer 먼저 정상 종료 시도
        logger.info("Stopping producer gracefully...")
        p.join(timeout=5)

        if p.is_alive():
            logger.warning("Producer still alive, terminating...")
            p.terminate()

        # 2. sentinel 넣기 (worker wake-up)
        logger.info("Sending shutdown signals to workers...")
        for _ in workers:
            q.put(None)

        # 3. worker 정상 종료 대기
        for w in workers:
            w.join(timeout=10)

        # 4. still alive면 강제 종료
        for w in workers:
            if w.is_alive():
                logger.warning("Force killing worker %s", w.pid)
                w.terminate()

        # 5. listener stop
        logger.info("Stopping listener...")
        listener.stop()

        logger.info("Shutdown complete")