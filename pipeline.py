# pipeline.py
import threading
from queue import Queue
from rss import fetch_one, RSS_FEEDS
from embed import embed_news
from db import save_news

q = Queue()
seen = set()
seen_lock = threading.Lock()


def producer():
    for url in RSS_FEEDS:
        news_list = fetch_one(url)

        for n in news_list:
            with seen_lock:
                if n["url"] in seen:
                    continue
                seen.add(n["url"])

            q.put(n)

    for _ in range(2):
        q.put(None)


def worker():
    buffer = []

    while True:
        news = q.get()

        try:
            if news is None:
                if buffer:
                    process(buffer)
                return

            buffer.append(news)

            if len(buffer) >= 32:
                process(buffer)
                buffer.clear()

        finally:
            q.task_done()


def process(buffer):
    embeddings = embed_news(buffer)
    save_news(buffer, embeddings)


def start_workers(n=2):
    for _ in range(n):
        t = threading.Thread(target=worker, daemon=True)
        t.start()