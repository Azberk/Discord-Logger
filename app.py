import asyncio
import hashlib
import hmac
import json
import os
import shutil
import urllib.parse
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import Optional, List
import aiohttp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, UploadFile, File, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import database as db
from discord_gateway import DiscordStalkerGateway

ws_clients: List[WebSocket] = []
SECRET_SALT = "discord_radar_auth_salt_v1"


def make_auth_token(password: str) -> str:
    return hmac.new(SECRET_SALT.encode(), password.encode(), hashlib.sha256).hexdigest()


def is_request_authenticated(request: Request) -> bool:
    expected_pass = db.get_setting("panel_password", "")
    if not expected_pass:
        # If no password is set yet, allow access so the owner can set one
        return True
    expected_token = make_auth_token(expected_pass)
    cookie_token = request.cookies.get("radar_session", "")
    header_token = request.headers.get("X-Radar-Auth", "")
    return hmac.compare_digest(cookie_token, expected_token) or hmac.compare_digest(header_token, expected_token)


def is_ws_authenticated(websocket: WebSocket) -> bool:
    expected_pass = db.get_setting("panel_password", "")
    if not expected_pass:
        return True
    expected_token = make_auth_token(expected_pass)
    cookie_token = websocket.cookies.get("radar_session", "")
    query_token = websocket.query_params.get("auth", "")
    return hmac.compare_digest(cookie_token, expected_token) or hmac.compare_digest(query_token, expected_token)


async def broadcast_to_clients(payload: dict):
    dead = []
    msg = json.dumps(payload, ensure_ascii=False)
    for client in ws_clients:
        try:
            await client.send_text(msg)
        except Exception:
            dead.append(client)
    for d in dead:
        if d in ws_clients:
            ws_clients.remove(d)


gateway = DiscordStalkerGateway(on_event_callback=broadcast_to_clients)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    env_token = os.environ.get("DISCORD_TOKEN", "").strip()
    if env_token and not db.get_setting("discord_token", ""):
        db.set_setting("discord_token", env_token)
    env_webhook = os.environ.get("WEBHOOK_URL", "").strip()
    if env_webhook and not db.get_setting("webhook_url", ""):
        db.set_setting("webhook_url", env_webhook)
    env_pass = os.environ.get("PANEL_PASSWORD", "").strip()
    if env_pass and not db.get_setting("panel_password", ""):
        db.set_setting("panel_password", env_pass)

    saved_token = db.get_setting("discord_token", "")
    is_bot = db.get_setting("is_bot", "0") == "1"
    if saved_token:
        await gateway.start(saved_token, is_bot=is_bot)
    yield
    await gateway.stop()


app = FastAPI(title="Discord Stalker & Activity Tracker", lifespan=lifespan)


@app.middleware("http")
async def security_middleware(request: Request, call_next):
    path = request.url.path
    # Public routes: index page (renders lock screen), health check, and auth endpoints
    if path in ("/", "/health", "/api/auth", "/api/auth-status", "/api/logout"):
        return await call_next(request)

    if path.startswith("/api/"):
        if not is_request_authenticated(request):
            return JSONResponse({"ok": False, "error": "Yetkisiz erişim! Lütfen şifre ile giriş yapın."}, status_code=401)

    return await call_next(request)


STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
os.makedirs(STATIC_DIR, exist_ok=True)


class TokenConfigRequest(BaseModel):
    token: str = ""
    is_bot: bool = False
    track_all_friends: bool = False
    webhook_url: str = ""
    panel_password: str = ""
    activity_cooldown: int = 180
    henrik_api_key: str = ""


class GuildSelectionRequest(BaseModel):
    guild_ids: List[str]


class ManualLogRequest(BaseModel):
    user_id: str
    event_type: str = "invis_detected"
    summary: str
    custom_timestamp: Optional[str] = None
    activity_name: Optional[str] = ""


class ValorantAccountRequest(BaseModel):
    user_id: str
    valorant_name: str
    valorant_tag: str
    valorant_region: str = "eu"


class ManualValorantMatchRequest(BaseModel):
    user_id: str
    map_name: str
    mode_name: str = "Rekabetçi"
    agent_name: str
    kills: int = 0
    deaths: int = 0
    assists: int = 0
    score_str: str = "13 - 9"
    started_at: str
    duration_minutes: int = 35


class AddUserRequest(BaseModel):
    user_id: str
    note: Optional[str] = ""


class AuthRequest(BaseModel):
    password: str


@app.get("/health")
async def health_check():
    """Used by UptimeRobot to keep the free cloud server awake 24/7 (Exposes no sensitive info)."""
    return {"ok": True, "status": "alive"}


@app.get("/api/auth-status")
async def api_auth_status(request: Request):
    has_password = bool(db.get_setting("panel_password", ""))
    authenticated = is_request_authenticated(request)
    return {
        "has_password": has_password,
        "authenticated": authenticated
    }


@app.post("/api/auth")
async def api_auth(req: AuthRequest, response: Response):
    expected = db.get_setting("panel_password", "")
    if not expected:
        # First time setup: if no password exists yet, set this as the new password!
        if req.password.strip():
            db.set_setting("panel_password", req.password.strip())
            expected = req.password.strip()
        else:
            return {"ok": True}

    if req.password == expected:
        token = make_auth_token(expected)
        res = JSONResponse({"ok": True, "token": token})
        res.set_cookie(
            key="radar_session",
            value=token,
            max_age=60 * 60 * 24 * 30,  # 30 days
            httponly=True,
            samesite="lax"
        )
        return res
    return JSONResponse({"ok": False, "error": "Hatalı panel şifresi!"}, status_code=401)


@app.post("/api/logout")
async def api_logout():
    res = JSONResponse({"ok": True})
    res.delete_cookie("radar_session")
    return res


@app.get("/", response_class=HTMLResponse)
async def serve_index():
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_path):
        with open(index_path, "r", encoding="utf-8") as f:
            return f.read()
    return "<h1>Dashboard yükleniyor...</h1>"


@app.get("/api/status")
async def api_status():
    try:
        cd_val = int(db.get_setting("activity_cooldown", "180") or "180")
    except Exception:
        cd_val = 180
    return {
        "gateway": gateway.get_status_info(),
        "settings": {
            "has_token": bool(db.get_setting("discord_token", "")),
            "is_bot": db.get_setting("is_bot", "0") == "1",
            "track_all_friends": db.get_setting("track_all_friends", "0") == "1",
            "webhook_url": db.get_setting("webhook_url", ""),
            "has_password": bool(db.get_setting("panel_password", "")),
            "activity_cooldown": cd_val,
            "henrik_api_key": db.get_setting("henrik_api_key", "")
        }
    }


@app.get("/api/guilds")
async def api_get_guilds():
    try:
        selected = json.loads(db.get_setting("selected_guilds", "[]") or "[]")
    except Exception:
        selected = []
    selected_set = set(selected)
    guilds_list = []
    for gid in gateway.guilds:
        info = gateway.guild_details.get(gid, {"id": gid, "name": f"Sunucu {gid}", "icon": "", "member_count": 0})
        item = dict(info)
        item["selected"] = gid in selected_set
        guilds_list.append(item)
    return {"guilds": guilds_list, "selected_ids": selected}


@app.post("/api/guilds")
async def api_save_guilds(req: GuildSelectionRequest):
    clean_ids = [str(g) for g in req.guild_ids]
    db.set_setting("selected_guilds", json.dumps(clean_ids))
    # Immediately trigger resync on the newly selected guilds
    tracked = db.get_tracked_users(only_tracked=True)
    uids = [u["user_id"] for u in tracked]
    if uids:
        asyncio.create_task(gateway.subscribe_to_users_in_guilds(uids))
    return {"ok": True, "selected_count": len(clean_ids)}


@app.post("/api/config")
async def api_save_config(req: TokenConfigRequest):
    token = req.token.strip()
    if token:
        db.set_setting("discord_token", token)
    db.set_setting("is_bot", "1" if req.is_bot else "0")
    db.set_setting("track_all_friends", "1" if req.track_all_friends else "0")
    db.set_setting("webhook_url", req.webhook_url.strip())
    db.set_setting("activity_cooldown", str(req.activity_cooldown))
    if req.henrik_api_key is not None:
        db.set_setting("henrik_api_key", req.henrik_api_key.strip())

    new_cookie_token = None
    if req.panel_password is not None and req.panel_password.strip() != "***keep***":
        clean_pw = req.panel_password.strip()
        db.set_setting("panel_password", clean_pw)
        if clean_pw:
            new_cookie_token = make_auth_token(clean_pw)

    db.checkpoint_db()

    active_token = token or db.get_setting("discord_token", "")
    if active_token:
        await gateway.start(active_token, is_bot=req.is_bot)

    res = JSONResponse({"ok": True, "status": gateway.get_status_info()})
    if new_cookie_token:
        res.set_cookie(
            key="radar_session",
            value=new_cookie_token,
            max_age=60 * 60 * 24 * 30,
            httponly=True,
            samesite="lax"
        )
    elif req.panel_password is not None and req.panel_password.strip() == "":
        res.delete_cookie("radar_session")
    return res


@app.post("/api/disconnect")
async def api_disconnect():
    await gateway.stop()
    return {"ok": True, "status": gateway.get_status_info()}


@app.get("/api/backup")
async def api_backup_db():
    # Flush all WAL/journal settings including discord_token into stalker.db before download!
    db.checkpoint_db()
    if os.path.exists(db.DB_PATH):
        return FileResponse(
            path=db.DB_PATH,
            filename="stalker_backup.db",
            media_type="application/octet-stream"
        )
    return JSONResponse({"ok": False, "error": "Veritabanı bulunamadı."}, status_code=404)


@app.post("/api/restore")
async def api_restore_db(file: UploadFile = File(...)):
    await gateway.stop()
    temp_path = db.DB_PATH + ".upload"
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    shutil.move(temp_path, db.DB_PATH)
    db.init_db()

    # Automatically reconnect to Discord using the token saved inside the restored database!
    restored_token = db.get_setting("discord_token", "")
    restored_is_bot = db.get_setting("is_bot", "0") == "1"
    if restored_token:
        await gateway.start(restored_token, is_bot=restored_is_bot)

    await broadcast_to_clients({"type": "users_updated", "data": db.get_tracked_users(only_tracked=True)})
    return {"ok": True, "token_restored": bool(restored_token)}


@app.get("/api/users")
async def api_get_users():
    return {"users": db.get_tracked_users(only_tracked=True)}


@app.get("/api/known-users")
async def api_get_known_users(q: Optional[str] = Query(None)):
    tracked_ids = {u["user_id"] for u in db.get_tracked_users(only_tracked=True)}
    items = []
    query_str = (q or "").lower().strip()
    for uid, info in gateway.known_users.items():
        if query_str:
            if (
                query_str not in (info.get("username") or "").lower()
                and query_str not in (info.get("global_name") or "").lower()
                and query_str not in uid
            ):
                continue
        item = dict(info)
        item["already_tracked"] = uid in tracked_ids
        items.append(item)

    items.sort(key=lambda x: (not x.get("is_friend", False), x.get("status", "offline") == "offline", x.get("global_name", "")))
    return {"known_users": items[:200]}


@app.post("/api/users")
async def api_add_user(req: AddUserRequest):
    uid = req.user_id.strip()
    if not uid.isdigit():
        return JSONResponse({"ok": False, "error": "Kullanıcı ID sadece rakamlardan oluşmalıdır."}, status_code=400)

    info = gateway.known_users.get(uid)
    if not info:
        info = await gateway.fetch_user_profile_rest(uid) or {}

    user_row = db.upsert_tracked_user(
        user_id=uid,
        username=info.get("username"),
        global_name=info.get("global_name"),
        avatar=info.get("avatar"),
        note=req.note or "",
        is_tracked=1
    )

    if info and info.get("status"):
        log_entry = db.record_presence_update(
            user_id=uid,
            username=info.get("username"),
            global_name=info.get("global_name"),
            avatar=info.get("avatar"),
            status=info.get("status", "offline"),
            client_status=info.get("client_status") or {},
            activities=info.get("activities") or [],
            initial_sync=True
        )
        if log_entry:
            await broadcast_to_clients({
                "type": "presence_log",
                "log": log_entry,
                "user": db.get_tracked_user(uid)
            })

    asyncio.create_task(gateway.subscribe_to_users_in_guilds([uid]))

    user_row = db.get_tracked_user(uid)
    await broadcast_to_clients({"type": "users_updated", "data": db.get_tracked_users(only_tracked=True)})
    return {"ok": True, "user": user_row}


@app.delete("/api/users/{user_id}")
async def api_delete_user(user_id: str):
    db.remove_tracked_user(user_id)
    await broadcast_to_clients({"type": "users_updated", "data": db.get_tracked_users(only_tracked=True)})
    return {"ok": True}


@app.post("/api/resync")
async def api_resync_presences():
    """Forces an immediate Opcode 8 + Opcode 14 presence & activity scan across all shared guilds."""
    tracked = db.get_tracked_users(only_tracked=True)
    uids = [u["user_id"] for u in tracked]
    if uids:
        asyncio.create_task(gateway.subscribe_to_users_in_guilds(uids))
    return {"ok": True, "synced_count": len(uids)}


@app.get("/api/logs")
async def api_get_logs(
    user_id: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    date_str: Optional[str] = Query(None),
    search_q: Optional[str] = Query(None),
    limit: int = Query(500)
):
    return {
        "logs": db.get_logs(user_id=user_id, event_type=event_type, date_str=date_str, search_q=search_q, limit=limit),
        "total_count": db.get_total_logs_count()
    }


@app.get("/api/timeline")
async def api_get_timeline(
    user_id: Optional[str] = Query(None),
    date_str: Optional[str] = Query(None)
):
    return db.get_detailed_timeline(user_id=user_id, date_str=date_str)


@app.post("/api/manual-log")
async def api_add_manual_log(req: ManualLogRequest):
    act_obj = None
    if req.activity_name and req.activity_name.strip():
        act_obj = {
            "name": req.activity_name.strip(),
            "type": 0,
            "type_name": "Manuel Gözlem",
            "details": req.summary,
            "state": ""
        }
    prefix = "🚨 [MANUEL INVIS KAYDI] " if req.event_type == "invis_detected" else "📝 [MANUEL GÖZLEM] "
    full_summary = prefix + req.summary.strip()
    log_entry = db.insert_special_or_manual_log(
        user_id=req.user_id,
        event_type=req.event_type,
        summary=full_summary,
        custom_timestamp=req.custom_timestamp,
        activity_obj=act_obj
    )
    if not log_entry:
        return JSONResponse({"ok": False, "error": "Kullanıcı bulunamadı."}, status_code=404)

    await broadcast_to_clients({
        "type": "presence_log",
        "log": log_entry,
        "user": db.get_tracked_user(req.user_id)
    })
    return {"ok": True, "log": log_entry}


@app.post("/api/valorant/link")
async def api_link_valorant(req: ValorantAccountRequest):
    db.update_user_valorant_account(
        user_id=req.user_id,
        val_name=req.valorant_name,
        val_tag=req.valorant_tag,
        val_region=req.valorant_region
    )
    await broadcast_to_clients({"type": "users_updated", "data": db.get_tracked_users(only_tracked=True)})
    return {"ok": True}


@app.get("/api/valorant/matches")
async def api_get_valorant_matches(user_id: Optional[str] = Query(None)):
    return {"matches": db.get_valorant_matches(user_id=user_id, limit=150)}


@app.post("/api/valorant/manual-match")
async def api_add_manual_valorant_match(req: ManualValorantMatchRequest):
    u = db.get_tracked_user(req.user_id)
    if not u:
        return JSONResponse({"ok": False, "error": "Kullanıcı bulunamadı."}, status_code=404)

    riot_id = f"{u.get('valorant_name') or u.get('global_name') or u.get('username')}#{u.get('valorant_tag') or 'TR1'}"
    try:
        dt_start = datetime.fromisoformat(req.started_at)
        if dt_start.tzinfo is None:
            dt_start = dt_start.astimezone()
    except Exception:
        dt_start = datetime.now().astimezone()

    dur_sec = max(60, req.duration_minutes * 60)
    dt_end = dt_start + timedelta(seconds=dur_sec)
    match_id = f"manual_{int(dt_start.timestamp())}_{req.user_id[-4:]}"

    m = db.record_valorant_match(
        match_id=match_id,
        user_id=req.user_id,
        riot_id=riot_id,
        map_name=req.map_name,
        mode_name=req.mode_name,
        agent_name=req.agent_name,
        kills=req.kills,
        deaths=req.deaths,
        assists=req.assists,
        score_str=req.score_str,
        started_at=dt_start.isoformat(timespec="seconds"),
        ended_at=dt_end.isoformat(timespec="seconds"),
        duration_seconds=dur_sec
    )
    await broadcast_to_clients({"type": "users_updated", "data": db.get_tracked_users(only_tracked=True)})
    return {"ok": True, "match": m}


@app.post("/api/valorant/sync")
async def api_sync_valorant_matches(user_id: Optional[str] = Query(None)):
    """
    Polls HenrikDev Valorant API (v3 matches) for tracked users who have a linked Riot ID (Name#Tag),
    logs any new matches, and automatically cross-references them with Discord presence logs!
    """
    users = db.get_tracked_users(only_tracked=True)
    if user_id:
        users = [u for u in users if u["user_id"] == user_id]

    linked_users = [u for u in users if u.get("valorant_name") and u.get("valorant_tag")]
    if not linked_users:
        return {"ok": False, "error": "Henüz Valorant Riot ID (İsim#Etiket) bağlanmış bir kişi yok.", "new_matches": 0}

    api_key = db.get_setting("henrik_api_key", "").strip()
    headers = {"User-Agent": "DiscordPresenceRadar/2.0"}
    if api_key:
        headers["Authorization"] = api_key

    new_count = 0
    errors = []

    async with aiohttp.ClientSession() as session:
        for u in linked_users:
            v_name = urllib.parse.quote(u["valorant_name"].strip())
            v_tag = urllib.parse.quote(u["valorant_tag"].strip().lstrip("#"))
            v_reg = (u.get("valorant_region") or "eu").strip().lower()
            riot_id_display = f"{u['valorant_name']}#{u['valorant_tag'].lstrip('#')}"

            url = f"https://api.henrikdev.xyz/valorant/v3/matches/{v_reg}/{v_name}/{v_tag}?size=5"
            try:
                async with session.get(url, headers=headers, timeout=12) as resp:
                    if resp.status == 200:
                        payload = await resp.json()
                        for m_data in (payload.get("data") or []):
                            meta = m_data.get("metadata") or {}
                            match_id = str(meta.get("matchid") or "")
                            if not match_id:
                                continue

                            map_name = meta.get("map") or "Bilinmeyen Harita"
                            mode_name = meta.get("mode") or "Rekabetçi"
                            game_start_ts = meta.get("game_start") or 0
                            game_len_ms = meta.get("game_length") or 0
                            # In v3, game_length can be in seconds or ms depending on endpoint version
                            dur_sec = int(game_len_ms / 1000) if game_len_ms > 50000 else int(game_len_ms or 1800)

                            dt_start = datetime.fromtimestamp(game_start_ts, tz=timezone.utc).astimezone() if game_start_ts else datetime.now().astimezone()
                            dt_end = dt_start + timedelta(seconds=dur_sec)

                            # Find player stats inside m_data.players.all_players
                            all_players = (m_data.get("players") or {}).get("all_players") or []
                            target_p = None
                            for p in all_players:
                                if (
                                    (p.get("name") or "").lower() == u["valorant_name"].strip().lower()
                                    and (p.get("tag") or "").lower() == u["valorant_tag"].strip().lstrip("#").lower()
                                ):
                                    target_p = p
                                    break
                            if not target_p and all_players:
                                target_p = all_players[0]

                            agent_name = (target_p or {}).get("character") or "Ajan"
                            stats = (target_p or {}).get("stats") or {}
                            kills = int(stats.get("kills") or 0)
                            deaths = int(stats.get("deaths") or 0)
                            assists = int(stats.get("assists") or 0)

                            team_color = ((target_p or {}).get("team") or "Blue").lower()
                            teams_obj = m_data.get("teams") or {}
                            t_info = teams_obj.get(team_color) or {}
                            rounds_won = t_info.get("rounds_won")
                            rounds_lost = t_info.get("rounds_lost")
                            score_str = f"{rounds_won} - {rounds_lost}" if rounds_won is not None else "Tamamlandı"

                            inserted = db.record_valorant_match(
                                match_id=match_id,
                                user_id=u["user_id"],
                                riot_id=riot_id_display,
                                map_name=map_name,
                                mode_name=mode_name,
                                agent_name=agent_name,
                                kills=kills,
                                deaths=deaths,
                                assists=assists,
                                score_str=score_str,
                                started_at=dt_start.isoformat(timespec="seconds"),
                                ended_at=dt_end.isoformat(timespec="seconds"),
                                duration_seconds=dur_sec
                            )
                            if inserted:
                                new_count += 1
                    else:
                        errors.append(f"{riot_id_display}: HTTP {resp.status} (API Anahtarı gerekebilir veya profil gizli)")
            except Exception as e:
                errors.append(f"{riot_id_display}: {str(e)}")

    return {
        "ok": True,
        "new_matches": new_count,
        "errors": errors,
        "matches": db.get_valorant_matches(user_id=user_id, limit=150)
    }


@app.get("/api/users/{user_id}/analytics")
async def api_user_analytics(user_id: str):
    return db.get_user_analytics(user_id)


@app.post("/api/demo-data")
async def api_seed_demo_data():
    demo_users = [
        {
            "user_id": "289104819283746819",
            "username": "berkay.dev",
            "global_name": "Berkay",
            "avatar": "https://cdn.discordapp.com/embed/avatars/1.png",
            "note": "Yakın Arkadaş",
            "status": "online",
            "client_status": {"desktop": "online", "mobile": "online"},
            "activities": [
                {"name": "Valorant", "type": 0, "type_name": "Oynuyor", "details": "Rekabetçi - Ascent", "state": "Skor: 9 - 6"},
                {"name": "Spotify", "type": 2, "type_name": "Dinliyor", "details": "After Dark", "state": "Mr.Kitty"}
            ]
        },
        {
            "user_id": "591827364519283746",
            "username": "zeynep_x",
            "global_name": "Zeynep ✨",
            "avatar": "https://cdn.discordapp.com/embed/avatars/4.png",
            "note": "Sunucu Moderatörü",
            "status": "dnd",
            "client_status": {"mobile": "dnd"},
            "activities": [
                {"name": "Özel Durum", "type": 4, "type_name": "Özel Durum", "details": "🌙 Ders çalışıyorum yazmayın", "state": ""}
            ]
        },
        {
            "user_id": "718293049581726354",
            "username": "emre_k",
            "global_name": "Emre",
            "avatar": "https://cdn.discordapp.com/embed/avatars/2.png",
            "note": "Oyun Ekibi",
            "status": "idle",
            "client_status": {"desktop": "idle", "web": "online"},
            "activities": [
                {"name": "Visual Studio Code", "type": 0, "type_name": "Oynuyor", "details": "app.py üzerinde çalışıyor", "state": "Workspace: Discord-Stalker"}
            ]
        }
    ]

    for du in demo_users:
        db.upsert_tracked_user(
            user_id=du["user_id"],
            username=du["username"],
            global_name=du["global_name"],
            avatar=du["avatar"],
            note=du["note"],
            is_tracked=1
        )
        db.record_presence_update(
            user_id=du["user_id"],
            username=du["username"],
            global_name=du["global_name"],
            avatar=du["avatar"],
            status=du["status"],
            client_status=du["client_status"],
            activities=du["activities"]
        )

    await broadcast_to_clients({"type": "users_updated", "data": db.get_tracked_users(only_tracked=True)})
    return {"ok": True}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    if not is_ws_authenticated(websocket):
        await websocket.close(code=4401)
        return
    await websocket.accept()
    ws_clients.append(websocket)
    try:
        await websocket.send_text(json.dumps({
            "type": "init",
            "gateway": gateway.get_status_info(),
            "users": db.get_tracked_users(only_tracked=True),
            "logs": db.get_logs(limit=500),
            "total_logs_count": db.get_total_logs_count()
        }, ensure_ascii=False))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in ws_clients:
            ws_clients.remove(websocket)
    except Exception:
        if websocket in ws_clients:
            ws_clients.remove(websocket)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8050))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
