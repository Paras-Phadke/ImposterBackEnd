import psycopg2
import pandas as pd
import os

def get_conn():
    return psycopg2.connect(
        host=os.getenv("dpg-d5fs3vdactks739s5jmg-a.singapore-postgres.render.com"),
        database=os.getenv("imposterdb"),
        user=os.getenv("imposterdb_user"),
        password=os.getenv("SHptLjNvQ16KE1QrbhrzVZ0N6Ql2KUSz")
    )

def read_db():
    conn = get_conn()

    cats = pd.read_sql("SELECT id, name, updated_at FROM categories", conn)
    words = pd.read_sql("SELECT id, category_id, word, clue, updated_at, deleted FROM words", conn)

    conn.close()
    return cats, words

def apply_db_updates(cats_df, words_df):
    conn = get_conn()
    cur = conn.cursor()

    # Update categories
    for _, row in cats_df.iterrows():
        cur.execute("""
            UPDATE categories
            SET name=%s, updated_at=%s
            WHERE id=%s
        """, (row['name'], row['updated_at'], row['id']))

    # Update words
    for _, row in words_df.iterrows():
        cur.execute("""
            UPDATE words
            SET category_id=%s, word=%s, clue=%s, updated_at=%s, deleted=%s
            WHERE id=%s
        """, (row['category_id'], row['word'], row['clue'], row['updated_at'], row['deleted'], row['id']))

    conn.commit()
    cur.close()
    conn.close()

def mark_word_deleted(word_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE words SET deleted = TRUE, updated_at = now() WHERE id = %s
    """, (word_id,))
    conn.commit()
    cur.close()
    conn.close()
