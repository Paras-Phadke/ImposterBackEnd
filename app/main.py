import os
import json
import psycopg2
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import JSONResponse

app = FastAPI()

# ======== CONFIG ========

DATABASE_URL = os.environ.get("DATABASE_URL")

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
