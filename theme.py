# theme.py
from datetime import datetime, timezone
from db import get_cursor
from pgvector.psycopg import Vector


def match_theme(emb):
    cur = get_cursor()

    cur.execute("""
        SELECT theme_id, name, strength, article_count, last_seen,
               1 - (embedding <=> %s) AS score
        FROM themes
        ORDER BY embedding <=> %s
        LIMIT 1
    """, (Vector(emb), Vector(emb)))

    row = cur.fetchone()

    if not row:
        return None, 0

    theme = {
        "id": row[0],
        "name": row[1],
        "strength": row[2],
        "article_count": row[3],
        "last_seen": row[4]
    }

    return theme, row[5]


def update_theme(theme_id, strength, momentum, article_count):
    cur = get_cursor()

    cur.execute("""
        UPDATE themes
        SET strength=%s,
            momentum=%s,
            article_count=%s,
            last_seen=%s
        WHERE theme_id=%s
    """, (
        strength,
        momentum,
        article_count,
        datetime.now(timezone.utc),
        theme_id
    ))

    cur.connection.commit()


def create_theme(name, emb):
    cur = get_cursor()

    cur.execute("""
        INSERT INTO themes
        (name, embedding, strength, momentum, article_count, created_at, last_seen)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (
        name,
        emb.tolist(),
        1.0,
        0,
        1,
        datetime.now(timezone.utc),
        datetime.now(timezone.utc)
    ))

    cur.connection.commit()


def calculate_momentum(theme):
    age_hours = (datetime.now(timezone.utc) - theme["last_seen"]).total_seconds() / 3600
    freshness = 1 / max(age_hours, 1)
    return freshness * theme["article_count"]


def process_theme(title, emb):
    theme, score = match_theme(emb)

    if theme and score > 0.87:
        update_theme(
            theme["id"],
            theme["strength"] + 1,
            calculate_momentum(theme),
            theme["article_count"] + 1
        )
    else:
        create_theme(title[:40], emb)