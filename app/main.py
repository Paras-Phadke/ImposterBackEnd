from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import json, os
from datetime import datetime

app = FastAPI()

# allow requests from mobile app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_FILE = "data/saved_games.json"

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

@app.get("/db")
def get_all_data():
    path = "data/saved_games.json"
    if not os.path.exists(path):
        return {"message": "No data saved yet"}

    with open(path, "r") as f:
        content = json.load(f)
    return content



@app.post("/upload")
async def upload(request: Request):
    new_game = await request.json()  # expects your JSON chunk

    # load existing
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            try:
                stored = json.load(f)
            except:
                stored = []
    else:
        stored = []

    # append new game (store as an object with timestamp server-side)
    stored.append({
        "received_at": datetime.utcnow().isoformat(),
        "game": new_game
    })

    # write back
    with open(DATA_FILE, "w") as f:
        json.dump(stored, f, indent=2)

    return {"success": True, "count": len(stored)}
