# db.py
import os
from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector
import csv
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def configure(conn):
    register_vector(conn)


pool = ConnectionPool(
    conninfo=f"""
        dbname=news
        user={os.getenv("DB_USER")}
        password={os.getenv("DB_PASSWORD")}
        host={os.getenv("DB_HOST")}
        port={os.getenv("DB_PORT")}
    """,
    min_size=1,
    max_size=10,
    configure=configure
)


def save_news(news_list, embeddings):
    if len(news_list) != len(embeddings):
        raise ValueError(
            f"Length mismatch: {len(news_list)} != {len(embeddings)}"
        )

    with pool.connection() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                i = -1
                try:
                    with cur.copy("""
                        COPY news (title, summary, url, embedding, timestamp)
                        FROM STDIN WITH (FORMAT csv)
                    """) as copy:

                        writer = csv.writer(copy)

                        for i, (n, emb) in enumerate(zip(news_list, embeddings)):
                            writer.writerow([
                                n["title"],
                                n["summary"],
                                n["url"],
                                emb.tolist(),
                                n["published_at"]
                            ])

                except Exception:
                    logger.exception("COPY failed near row %s", i)
                    raise