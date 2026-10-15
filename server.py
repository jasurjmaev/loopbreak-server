import asyncio
import os
import secrets
import string
import time
from typing import Dict, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

app = FastAPI(title="LOOPBREAK Multiplayer Server", version="1.0.0")

# Mini App is hosted on Netlify; allow web clients to call this API.
# In production, set ALLOWED_ORIGINS to your exact Netlify domain.
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in allowed_origins if origin.strip()],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

ROOM_TTL_SECONDS = int(os.getenv("ROOM_TTL_SECONDS", "21600"))
rooms: Dict[str, Dict[str, Any]] = {}
lock = asyncio.Lock()


class CreateRoomRequest(BaseModel):
    player_id: str = Field(min_length=1, max_length=80)
    name: str = Field(default="Kuzatuvchi", min_length=1, max_length=24)


class JoinRoomRequest(BaseModel):
    player_id: str = Field(min_length=1, max_length=80)
    name: str = Field(default="Kuzatuvchi", min_length=1, max_length=24)


def new_code() -> str:
    alphabet = string.ascii_uppercase.replace("I", "").replace("O", "") + "23456789"
    return "".join(secrets.choice(alphabet) for _ in range(6))


def clean_expired():
    now = time.time()
    expired = [
        code for code, room in rooms.items()
        if now - room["updated_at"] > ROOM_TTL_SECONDS and not room["clients"]
    ]
    for code in expired:
        rooms.pop(code, None)


def public_room(room: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "code": room["code"],
        "status": room["status"],
        "host_id": room["host_id"],
        "players": [
            {"player_id": p["player_id"], "name": p["name"], "online": p["online"]}
            for p in room["players"].values()
        ],
        "scene": room["scene"],
        "choice": room["choice"],
        "created_at": int(room["created_at"]),
    }


async def broadcast(code: str, payload: Dict[str, Any]):
    room = rooms.get(code)
    if not room:
        return
    stale = []
    for player_id, ws in list(room["clients"].items()):
        try:
            await ws.send_json(payload)
        except Exception:
            stale.append(player_id)
    for player_id in stale:
        room["clients"].pop(player_id, None)


@app.get("/")
async def root():
    return {"ok": True, "service": "LOOPBREAK multiplayer", "version": "1.0.0"}


@app.get("/health")
async def health():
    return {"ok": True, "rooms": len(rooms)}


@app.post("/api/rooms")
async def create_room(req: CreateRoomRequest):
    async with lock:
        clean_expired()
        code = new_code()
        while code in rooms:
            code = new_code()
        now = time.time()
        rooms[code] = {
            "code": code,
            "host_id": req.player_id,
            "status": "waiting",
            "players": {
                req.player_id: {
                    "player_id": req.player_id,
                    "name": req.name,
                    "online": False,
                    "joined_at": now,
                }
            },
            "clients": {},
            "scene": 0,
            "choice": None,
            "created_at": now,
            "updated_at": now,
        }
        return {"ok": True, "room": public_room(rooms[code])}


@app.post("/api/rooms/{code}/join")
async def join_room(code: str, req: JoinRoomRequest):
    code = code.strip().upper()
    async with lock:
        clean_expired()
        room = rooms.get(code)
        if not room:
            raise HTTPException(status_code=404, detail="Xona topilmadi yoki muddati tugagan.")
        if room["status"] == "finished":
            raise HTTPException(status_code=409, detail="Bu xona o‘yinni yakunlagan.")
        if req.player_id not in room["players"] and len(room["players"]) >= 8:
            raise HTTPException(status_code=409, detail="Xona to‘la (maksimum 8 o‘yinchi).")
        room["players"][req.player_id] = {
            "player_id": req.player_id,
            "name": req.name,
            "online": req.player_id in room["clients"],
            "joined_at": time.time(),
        }
        room["updated_at"] = time.time()
        result = public_room(room)
    await broadcast(code, {"type": "room_update", "room": result})
    return {"ok": True, "room": result}


@app.get("/api/rooms/{code}")
async def get_room(code: str):
    clean_expired()
    room = rooms.get(code.strip().upper())
    if not room:
        raise HTTPException(status_code=404, detail="Xona topilmadi yoki muddati tugagan.")
    return {"ok": True, "room": public_room(room)}


@app.websocket("/ws/{code}")
async def room_socket(websocket: WebSocket, code: str, player_id: str = "guest", name: str = "Kuzatuvchi"):
    code = code.strip().upper()
    await websocket.accept()
    room = rooms.get(code)
    if not room:
        await websocket.send_json({"type": "error", "message": "Xona topilmadi."})
        await websocket.close(code=1008)
        return

    player_id = player_id[:80]
    name = name[:24] or "Kuzatuvchi"
    if player_id not in room["players"] and len(room["players"]) >= 8:
        await websocket.send_json({"type": "error", "message": "Xona to‘la."})
        await websocket.close(code=1008)
        return

    room["players"].setdefault(player_id, {
        "player_id": player_id, "name": name, "online": False, "joined_at": time.time()
    })
    room["players"][player_id]["name"] = name
    room["players"][player_id]["online"] = True
    room["clients"][player_id] = websocket
    room["updated_at"] = time.time()

    await websocket.send_json({"type": "connected", "room": public_room(room)})
    await broadcast(code, {"type": "room_update", "room": public_room(room)})

    try:
        while True:
            message = await websocket.receive_json()
            kind = message.get("type")
            room = rooms.get(code)
            if not room:
                break
            room["updated_at"] = time.time()

            if kind == "chat":
                text = str(message.get("text", "")).strip()[:240]
                if text:
                    await broadcast(code, {
                        "type": "chat",
                        "player_id": player_id,
                        "name": room["players"][player_id]["name"],
                        "text": text,
                    })
            elif kind == "start" and player_id == room["host_id"]:
                room["status"] = "playing"
                room["scene"] = 0
                room["choice"] = None
                await broadcast(code, {"type": "room_update", "room": public_room(room)})
                await broadcast(code, {"type": "game_event", "event": "started", "scene": 0})
            elif kind == "vote":
                try:
                    choice = int(message.get("choice"))
                except (ValueError, TypeError):
                    continue
                if choice not in (0, 1, 2):
                    continue
                room.setdefault("votes", {})[player_id] = choice
                votes = room["votes"]
                await broadcast(code, {
                    "type": "votes",
                    "count": len(votes),
                    "total": len(room["players"]),
                    "votes": len(votes),
                })
                # Host can advance after everyone online has voted.
                online_players = [pid for pid, p in room["players"].items() if p["online"]]
                if online_players and all(pid in votes for pid in online_players):
                    counts = {i: sum(1 for v in votes.values() if v == i) for i in (0, 1, 2)}
                    winning = max(counts, key=lambda i: (counts[i], -i))
                    room["scene"] += 1
                    room["choice"] = winning
                    room["votes"] = {}
                    await broadcast(code, {
                        "type": "game_event",
                        "event": "choice_result",
                        "choice": winning,
                        "scene": room["scene"],
                    })
                    await broadcast(code, {"type": "room_update", "room": public_room(room)})
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        room = rooms.get(code)
        if room:
            room["clients"].pop(player_id, None)
            if player_id in room["players"]:
                room["players"][player_id]["online"] = False
            room["updated_at"] = time.time()
            await broadcast(code, {"type": "room_update", "room": public_room(room)})


@app.post("/api/rooms/{code}/leave")
async def leave_room(code: str, req: JoinRoomRequest):
    code = code.strip().upper()
    room = rooms.get(code)
    if not room:
        raise HTTPException(status_code=404, detail="Xona topilmadi.")
    if req.player_id == room["host_id"]:
        room["status"] = "finished"
    else:
        room["players"].pop(req.player_id, None)
        room["clients"].pop(req.player_id, None)
    room["updated_at"] = time.time()
    await broadcast(code, {"type": "room_update", "room": public_room(room)})
    return {"ok": True}
