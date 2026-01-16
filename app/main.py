import os
import json
import psycopg2
from fastapi import FastAPI,HTTPException
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from app.sheets import read_all, write_all
from app.db import read_db, apply_db_updates,mark_word_deleted,insert_new_categories,insert_new_words
from app.sync import resolve_conflicts,merge_back_to_sheet
import pandas as pd

app = FastAPI()

# ======== CONFIG ========

DATABASE_URL = os.environ.get("DATABASE_URL")
SYNC_TOKEN = os.environ.get("SYNC_TOKEN")

# ======== MODELS ========

class UploadPayload(BaseModel):
    user_id: str
    games: dict   # your dict keyed by game number

# ======== DB INIT ========

def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id SERIAL PRIMARY KEY,
            user_id TEXT,
            game_json JSONB,
            created_at TIMESTAMP DEFAULT NOW(),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    """)

    conn.commit()
    conn.close()

init_db()

# ======== ROUTES ========

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/upload")
def upload_data(payload: UploadPayload):
    user_id = payload.user_id
    games = payload.games

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # ensure user exists
    cur.execute(
        "INSERT INTO users(id) VALUES (%s) ON CONFLICT DO NOTHING",
        (user_id,)
    )

    for _, game in games.items():
        cur.execute(
            "INSERT INTO games(user_id, game_json) VALUES (%s, %s)",
            (user_id, json.dumps(game))
        )

    conn.commit()
    conn.close()

    return {"status": "ok", "games_received": len(games)}

@app.get("/db")
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("SELECT id, user_id, created_at, game_json FROM games ORDER BY id DESC")
    rows = cur.fetchall()
    conn.close()

    return JSONResponse(content=[
        {
            "id": r[0],
            "user_id": r[1],
            "created_at": str(r[2]),
            "game": r[3]
        } for r in rows
    ])

@app.post("/sync")
def sync(token: str):
    if token != SYNC_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    sheet_cats, sheet_words = read_all()
    db_cats, db_words = read_db()

    # normalize
    for df in [sheet_cats, sheet_words, db_cats, db_words]:
        if 'updated_at' in df:
            df['updated_at'] = pd.to_datetime(df['updated_at'])

    # NEW ROWS (no id) → DB
    insert_new_categories(sheet_cats[sheet_cats['id'] == ''])
    insert_new_words(sheet_words[sheet_words['id'] == ''])

    # reload DB after inserts
    db_cats, db_words = read_db()

    # resolve conflicts
    cats_to_db, cats_to_sheet = resolve_conflicts(sheet_cats, db_cats)
    words_to_db, words_to_sheet = resolve_conflicts(sheet_words, db_words)

    now = pd.Timestamp.utcnow()

    if not cats_to_db.empty:
        cats_to_db['updated_at'] = now

    if not words_to_db.empty:
        words_to_db['updated_at'] = now


    apply_db_updates(cats_to_db, words_to_db)

    # deletions
    for _, row in sheet_words.iterrows():
        if row['deleted'] in ['TRUE', True]:
            mark_word_deleted(int(row['id']))

    # reload DB again
    db_cats, db_words = read_db()

    # merge back
    sheet_cats = merge_back_to_sheet(sheet_cats, db_cats, cats_to_sheet)
    sheet_words = merge_back_to_sheet(sheet_words, db_words, words_to_sheet)

    write_all(sheet_cats, sheet_words)

    return {"status": "synced"}
