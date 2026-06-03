# producer.py
import time
import hashlib
from calendar import timegm
from datetime import datetime, timezone
from logging.handlers import QueueHandler
import logging
import feedparser
import redis
import requests


logger = logging.getLogger(__name__)


RSS_MACRO = [
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "https://news.google.com/rss/search?q=stocks+market",
]


RSS_SECTOR = [
    "https://news.google.com/rss/search?q=AI+stocks",
    "https://news.google.com/rss/search?q=semiconductors",
    "https://news.google.com/rss/search?q=interest+rates",
    "https://news.google.com/rss/search?q=inflation",
]


RSS_ASSET = [
    "https://news.google.com/rss/search?q=AAPL+stock",
    "https://news.google.com/rss/search?q=NVDA+stock",
    "https://news.google.com/rss/search?q=TSLA+stock",
    "https://news.google.com/rss/search?q=Bitcoin",
    "https://news.google.com/rss/search?q=Ethereum",
]

RSS_FEEDS = {
    "macro": RSS_MACRO,
    "sector": RSS_SECTOR,
    "asset": RSS_ASSET,
}

FETCH_INTERVAL = 300  # 5 minutes
SEEN_NEWS_TTL = 60 * 60 * 24 * 30  # 30 days

REQUEST_TIMEOUT = (5, 10)

redis_client = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True,
)

session = requests.Session()


def setup_logging(log_queue):
    handler = QueueHandler(log_queue)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers = []
    root.addHandler(handler)


def fetch(url):
    logger.info("fetch start %s", url)
    try:

        response = session.get(
            url,
            timeout=REQUEST_TIMEOUT,
        )

        response.raise_for_status()

        feed = feedparser.parse(response.content)

        items = []
        now = datetime.now(timezone.utc)

        for entry in feed.entries:

            published = entry.get("published_parsed")

            if published:
                published_at = datetime.fromtimestamp(
                    timegm(published),
                    tz=timezone.utc,
                )
            else:
                published_at = now

            items.append(
                {
                    "title": entry.get("title", ""),
                    "summary": entry.get("summary", ""),
                    "url": entry.get("link", ""),
                    "published_at": published_at,
                    "collected_at": now,
                }
            )

        logger.info("[PIPELINE][PRODUCER] fetch_done url=%s count=%d", url, len(items))
        return items

    except Exception as e:
        logger.exception("RSS fetch error [%s]", url)
        return []


def producer_loop(q, log_queue):
    setup_logging(log_queue)
    logger = logging.getLogger() 

    while True:

        for layer, urls in RSS_FEEDS.items():
            for url in urls:
            
                try:

                    items = fetch(url)

                    for item in items:

                        item["layer"] = layer
                        
                        news_url = item.get("url")

                        if not news_url:
                            continue

                        url_hash = hashlib.sha256(
                            news_url.encode("utf-8")
                        ).hexdigest()

                        redis_key = f"seen_news:{url_hash}"

                        try:
                            added = redis_client.set(
                                redis_key,
                                "1",
                                ex=SEEN_NEWS_TTL,
                                nx=True
                            )

                            if not added:
                                continue

                        except Exception as e:
                            logger.warning("Redis error: %s", e)
                            continue

                        try:
                            q.put(item, timeout=1)
                            logger.info("[PIPELINE][PRODUCER] queued url=%s", news_url)

                        except Exception as e:
                            logger.info("Queue error: %s", e)

                except Exception as e:
                    logger.info("RSS fetch error [%s]", url)

        time.sleep(FETCH_INTERVAL)