# worker.py
import os
import numpy as np
from sentence_transformers import SentenceTransformer
from db import save_news
from logging.handlers import QueueHandler
import logging
import queue
import time


logger = logging.getLogger(__name__)

BATCH_SIZE = 32

model = SentenceTransformer("all-MiniLM-L6-v2")


def setup_logging(log_queue):
    handler = QueueHandler(log_queue)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers = []
    root.addHandler(handler)


def build_text(item):
    return " ".join(
        filter(None, [
            item.get("title", ""),
            item.get("summary", "")
        ])
    ).strip()


def embed(texts):
    return model.encode(
        texts,
        batch_size=BATCH_SIZE,
        normalize_embeddings=True
    )


def process(buffer):
    if not buffer:
        return

    texts = [build_text(b) for b in buffer]
    emb = np.asarray(embed(texts), dtype=np.float32)

    try:
        save_news(buffer, emb)
    except Exception as e:
        logger.exception("DB save failed for batch size=%s", len(buffer))


def worker_loop(q, log_queue):
    setup_logging(log_queue)
    logger = logging.getLogger()

    logger.info("[WORKER] started pid=%s", os.getpid())

    buffer = []
    step = 0

    LAST_FLUSH_TIME = time.time()
    TIMEOUT = 5

    while True:
        step += 1

        logger.info("[WORKER][%d] waiting queue...", step)

        # ✅ FIX 1: blocking 문제 해결 (timeout)
        try:
            item = q.get(timeout=1)
        except queue.Empty:
            item = None

        # shutdown signal
        if item is None:
            now = time.time()

            # 🔥 timeout flush도 여기서 체크
            if buffer and (now - LAST_FLUSH_TIME > TIMEOUT):
                logger.info("[WORKER][%d] timeout flush (idle)", step)
                try:
                    texts = [
                        " ".join(filter(None, [
                            b.get("title", ""),
                            b.get("summary", "")
                        ])).strip()
                        for b in buffer
                    ]

                    emb = np.asarray(model.encode(texts), dtype=np.float32)
                    save_news(buffer, emb)

                except Exception as e:
                    logger.exception("[WORKER] idle flush failed")

                buffer.clear()
                LAST_FLUSH_TIME = now

            logger.info("[WORKER] shutdown signal received")
            break

        logger.info("[WORKER][%d] got item url=%s", step, item.get("url"))

        buffer.append(item)
        logger.info("[WORKER][%d] buffer size=%d", step, len(buffer))

        now = time.time()

        # ✅ FIX 2: clean flush condition
        should_flush = (
            len(buffer) >= BATCH_SIZE or
            (buffer and now - LAST_FLUSH_TIME > TIMEOUT)
        )

        if should_flush:
            logger.info("[WORKER][%d] batch trigger start", step)

            try:
                texts = [
                    " ".join(filter(None, [
                        b.get("title", ""),
                        b.get("summary", "")
                    ])).strip()
                    for b in buffer
                ]

                logger.info("[WORKER][%d] before embed", step)
                emb = np.asarray(model.encode(texts), dtype=np.float32)
                logger.info("[WORKER][%d] after embed", step)

                logger.info("[WORKER][%d] before DB save", step)
                save_news(buffer, emb)
                logger.info("[WORKER][%d] after DB save", step)

            except Exception as e:
                logger.exception("[WORKER][%d] processing failed", step)

            buffer.clear()
            LAST_FLUSH_TIME = now
            logger.info("[WORKER][%d] buffer cleared", step)

    logger.info("[WORKER] worker exit complete")