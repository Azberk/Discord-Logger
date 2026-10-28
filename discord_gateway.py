import asyncio
import json
import logging
import traceback
from typing import Dict, Any, List, Optional, Callable
import aiohttp

import database as db

logger = logging.getLogger("discord_gateway")

GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.0.0 Safari/537.36"
)

ACTIVITY_TYPES = {
    0: "Oynuyor",
    1: "Yayında",
    2: "Dinliyor",
    3: "İzliyor",
    4: "Özel Durum",
    5: "Yarışıyor"
}


def format_avatar_url(user_id: str, avatar_hash: Optional[str]) -> str:
    if not avatar_hash:
        # Default discord avatar
        try:
            idx = (int(user_id) >> 22) % 6
        except Exception:
            idx = 0
        return f"https://cdn.discordapp.com/embed/avatars/{idx}.png"
    ext = "gif" if avatar_hash.startswith("a_") else "png"
    return f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.{ext}?size=128"


def parse_activities(raw_activities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    parsed = []
    for act in raw_activities or []:
        act_type = act.get("type", 0)
        name = act.get("name", "")
        details = act.get("details") or ""
        state = act.get("state") or ""

        # Custom status handling (type 4)
        if act_type == 4:
            emoji = act.get("emoji") or {}
            emoji_name = emoji.get("name", "")
            custom_text = state or details or ""
            full_custom = f"{emoji_name} {custom_text}".strip()
            if not full_custom:
                continue
            parsed.append({
                "name": "Özel Durum",
                "type": 4,
                "type_name": "Özel Durum",
                "details": full_custom,
                "state": ""
            })
            continue

        # Spotify / Listening (type 2)
        if act_type == 2 and name == "Spotify":
            song = details or "Bilinmeyen Şarkı"
            artist = state or ""
            album = (act.get("assets") or {}).get("large_text", "")
            parsed.append({
                "name": "Spotify",
                "type": 2,
                "type_name": "Dinliyor",
                "details": song,
                "state": f"{artist}" + (f" • {album}" if album else "")
            })
            continue

        parsed.append({
            "name": name or "Aktivite",
            "type": act_type,
            "type_name": ACTIVITY_TYPES.get(act_type, "Aktivite"),
            "details": details,
            "state": state
        })
    return parsed


class DiscordStalkerGateway:
    def __init__(self, on_event_callback: Optional[Callable[[Dict[str, Any]], Any]] = None):
        self.token: str = ""
        self.is_bot: bool = False
        self.running: bool = False
        self.connected: bool = False
        self.self_user: Dict[str, Any] = {}
        self.last_error: str = ""
        self.sequence: Optional[int] = None
        self.session_id: Optional[str] = None
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self._hb_task: Optional[asyncio.Task] = None
        self.on_event_callback = on_event_callback

        # In-memory cache of known users (friends + guild members)
        self.known_users: Dict[str, Dict[str, Any]] = {}
        # Guilds list and metadata {guild_id: {"id", "name", "icon", "member_count"}}
        self.guilds: List[str] = []
        self.guild_details: Dict[str, Dict[str, Any]] = {}
        # Remember which guilds each user was seen in so we don't spam all guilds!
        self.user_guilds_map: Dict[str, set] = {}

    def get_status_info(self) -> Dict[str, Any]:
        return {
            "running": self.running,
            "connected": self.connected,
            "self_user": self.self_user,
            "last_error": self.last_error,
            "known_users_count": len(self.known_users),
            "guilds_count": len(self.guilds),
            "has_token": bool(self.token)
        }

    async def broadcast_ui(self, payload: Dict[str, Any]):
        if self.on_event_callback:
            try:
                res = self.on_event_callback(payload)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.error(f"UI callback error: {e}")

    async def start(self, token: str, is_bot: bool = False):
        await self.stop()
        clean_token = token.strip().strip('"').strip("'")
        if clean_token.lower().startswith("bot "):
            clean_token = clean_token[4:].strip()
            is_bot = True
        elif clean_token.lower().startswith("bearer "):
            clean_token = clean_token[7:].strip()

        self.token = clean_token
        self.is_bot = is_bot
        self.running = True
        self.last_error = ""
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self.running = False
        self.connected = False
        if self._hb_task and not self._hb_task.done():
            self._hb_task.cancel()
        if self.ws and not self.ws.closed:
            await self.ws.close()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self.session and not self.session.closed:
            await self.session.close()
        await self.broadcast_ui({"type": "gateway_status", "data": self.get_status_info()})

    async def fetch_user_profile_rest(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Fetches user info via Discord REST API if not already in cache."""
        if user_id in self.known_users:
            return self.known_users[user_id]
        if not self.token:
            return None

        auth_header = f"Bot {self.token}" if self.is_bot else self.token
        headers = {
            "Authorization": auth_header,
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json"
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"https://discord.com/api/v10/users/{user_id}", headers=headers, timeout=8) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        u_info = {
                            "user_id": str(data["id"]),
                            "username": data.get("username", ""),
                            "global_name": data.get("global_name") or data.get("username", ""),
                            "avatar": format_avatar_url(str(data["id"]), data.get("avatar"))
                        }
                        self.known_users[user_id] = u_info
                        return u_info
        except Exception as e:
            logger.warning(f"REST user fetch failed for {user_id}: {e}")
        return None

    async def subscribe_to_users_in_guilds(self, user_ids: List[str]):
        """
        Sends Opcode 8 + Opcode 14 only to user-selected guilds (or guilds where the user is known to be),
        preventing Discord Gateway rate-limiting.
        """
        if not self.connected or not self.ws or self.ws.closed or not user_ids:
            return
        clean_uids = [str(uid) for uid in user_ids[:100]]

        # Check if user configured specific target guilds
        try:
            selected_guilds = json.loads(db.get_setting("selected_guilds", "[]") or "[]")
        except Exception:
            selected_guilds = []

        if selected_guilds:
            target_guilds = [g for g in selected_guilds if g in self.guilds]
        else:
            target_guilds = list(self.guilds[:40])

        try:
            for guild_id in target_guilds:
                if self.ws.closed:
                    break
                # Filter clean_uids: if we already know which guild a user belongs to, only query that guild
                uids_for_this_guild = [
                    u for u in clean_uids
                    if (u not in self.user_guilds_map) or (guild_id in self.user_guilds_map[u]) or bool(selected_guilds)
                ]
                if not uids_for_this_guild:
                    continue

                op8_payload = {
                    "op": 8,
                    "d": {
                        "guild_id": str(guild_id),
                        "user_ids": uids_for_this_guild,
                        "presences": True
                    }
                }
                await self.ws.send_json(op8_payload)

                if not self.is_bot:
                    op14_payload = {
                        "op": 14,
                        "d": {
                            "guild_id": str(guild_id),
                            "typing": True,
                            "activities": True,
                            "threads": False,
                            "members": uids_for_this_guild
                        }
                    }
                    await self.ws.send_json(op14_payload)
                await asyncio.sleep(0.15)
        except Exception as e:
            logger.debug(f"Guild subscription warning: {e}")

    async def _run_loop(self):
        backoff = 3
        while self.running:
            try:
                if not self.session or self.session.closed:
                    self.session = aiohttp.ClientSession()

                logger.info("Connecting to Discord Gateway...")
                async with self.session.ws_connect(
                    GATEWAY_URL,
                    headers={"User-Agent": USER_AGENT},
                    max_msg_size=0,
                    heartbeat=45.0
                ) as ws:
                    self.ws = ws
                    backoff = 3
                    await self._handle_connection(ws)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                self.last_error = f"Bağlantı hatası: {str(e)}"
                logger.error(f"Gateway loop error: {e}\n{traceback.format_exc()}")
                await self.broadcast_ui({"type": "gateway_status", "data": self.get_status_info()})

            if not self.running:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def _handle_connection(self, ws: aiohttp.ClientWebSocketResponse):
        async for msg in ws:
            if not self.running:
                break
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(msg.data)
                op = data.get("op")
                t = data.get("t")
                s = data.get("s")
                d = data.get("d")

                if s is not None:
                    self.sequence = s

                if op == 10:  # HELLO
                    interval = d.get("heartbeat_interval", 41250) / 1000.0
                    if self._hb_task and not self._hb_task.done():
                        self._hb_task.cancel()
                    self._hb_task = asyncio.create_task(self._heartbeat_loop(ws, interval))
                    await self._send_identify(ws)

                elif op == 11:  # HEARTBEAT ACK
                    pass

                elif op == 0:  # DISPATCH
                    await self._handle_dispatch(t, d)

                elif op in (7, 9):  # RECONNECT or INVALID SESSION
                    logger.warning(f"Received op {op}, reconnecting...")
                    break

            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                close_code = ws.close_code
                if close_code == 4004:
                    self.last_error = "Geçersiz Discord Token! Lütfen tokeninizi kontrol edin (Hata Kodu: 4004)."
                    self.running = False
                elif close_code == 4014:
                    self.last_error = "Privileged Intent (Presence Intent) kapalı! Bot kullanıyorsanız Developer Portal'dan açın."
                    self.running = False
                else:
                    self.last_error = f"WebSocket kapandı (Kod: {close_code})"
                self.connected = False
                await self.broadcast_ui({"type": "gateway_status", "data": self.get_status_info()})
                break

    async def _heartbeat_loop(self, ws: aiohttp.ClientWebSocketResponse, interval: float):
        tick = 0
        try:
            while self.running and not ws.closed:
                await asyncio.sleep(interval)
                if not ws.closed:
                    await ws.send_json({"op": 1, "d": self.sequence})
                    tick += 1
                    # Every 2 heartbeats (~80 seconds), quietly resync tracked users in target guilds
                    # so non-friend users who went offline or changed activity without pushing an event are caught
                    if tick % 2 == 0 and self.connected:
                        tracked = db.get_tracked_users(only_tracked=True)
                        uids = [u["user_id"] for u in tracked]
                        if uids:
                            asyncio.create_task(self.subscribe_to_users_in_guilds(uids))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug(f"Heartbeat stopped: {e}")

    async def _send_identify(self, ws: aiohttp.ClientWebSocketResponse):
        if self.is_bot:
            # Standard Bot Identify with GUILD_MEMBERS (1<<1) + GUILD_PRESENCES (1<<8) + GUILDS (1<<0)
            identify_payload = {
                "op": 2,
                "d": {
                    "token": f"Bot {self.token}",
                    "intents": (1 << 0) | (1 << 1) | (1 << 8),
                    "properties": {
                        "os": "windows",
                        "browser": "disco",
                        "device": "disco"
                    }
                }
            }
        else:
            # Personal Account (User) Identify with capabilities bitmask for initial presence & relationship state
            identify_payload = {
                "op": 2,
                "d": {
                    "token": self.token,
                    "capabilities": 16381,
                    "properties": {
                        "os": "Windows",
                        "browser": "Chrome",
                        "device": "",
                        "system_locale": "tr-TR",
                        "browser_user_agent": USER_AGENT,
                        "browser_version": "128.0.0.0",
                        "os_version": "10",
                        "referrer": "",
                        "referring_domain": "",
                        "referrer_current": "",
                        "referring_domain_current": "",
                        "release_channel": "stable",
                        "client_build_number": 325950,
                        "client_event_source": None
                    },
                    "presence": {
                        "status": "online",
                        "since": 0,
                        "activities": [],
                        "afk": False
                    },
                    "compress": False,
                    "client_state": {
                        "guild_versions": {},
                        "highest_last_message_id": "0",
                        "read_state_version": 0,
                        "user_guild_settings_version": -1,
                        "user_settings_version": -1,
                        "private_channels_version": "0",
                        "api_code_version": 0
                    }
                }
            }
        await ws.send_json(identify_payload)

    async def _handle_dispatch(self, event_name: str, d: Any):
        if event_name == "READY":
            self.connected = True
            self.last_error = ""
            user_obj = d.get("user") or {}
            uid = str(user_obj.get("id", ""))
            self.self_user = {
                "id": uid,
                "username": user_obj.get("username", ""),
                "global_name": user_obj.get("global_name") or user_obj.get("username", ""),
                "avatar": format_avatar_url(uid, user_obj.get("avatar"))
            }
            logger.info(f"Connected to Discord as {self.self_user['username']} ({uid})")

            # Extract relationships / friends list
            users_map: Dict[str, Dict[str, Any]] = {}
            for u in d.get("users", []):
                u_id = str(u.get("id", ""))
                if u_id:
                    users_map[u_id] = u

            # Also check relationships array
            friend_ids = set()
            for rel in d.get("relationships", []):
                r_id = str(rel.get("id") or (rel.get("user") or {}).get("id") or "")
                if rel.get("user"):
                    users_map[r_id] = rel["user"]
                if rel.get("type") == 1 and r_id:  # type 1 = Friend
                    friend_ids.add(r_id)

            # Extract guild IDs and members if provided in READY
            self.guilds = []
            self.guild_details = {}
            for g in d.get("guilds", []):
                gid = str(g.get("id", ""))
                if gid:
                    self.guilds.append(gid)
                    props = g.get("properties") or g
                    g_name = props.get("name") or g.get("name") or f"Sunucu {gid[-4:]}"
                    g_icon = props.get("icon") or g.get("icon")
                    icon_url = f"https://cdn.discordapp.com/icons/{gid}/{g_icon}.png?size=64" if g_icon else ""
                    self.guild_details[gid] = {
                        "id": gid,
                        "name": g_name,
                        "icon": icon_url,
                        "member_count": g.get("member_count") or 0
                    }
                for m in g.get("members", []):
                    mu = m.get("user") or {}
                    mu_id = str(mu.get("id", ""))
                    if mu_id:
                        users_map[mu_id] = mu
                        if gid:
                            self.user_guilds_map.setdefault(mu_id, set()).add(gid)

            # Populate known_users cache
            for u_id, u_data in users_map.items():
                self.known_users[u_id] = {
                    "user_id": u_id,
                    "username": u_data.get("username", ""),
                    "global_name": u_data.get("global_name") or u_data.get("username", ""),
                    "avatar": format_avatar_url(u_id, u_data.get("avatar")),
                    "is_friend": u_id in friend_ids
                }

            # Auto-add friends if "track_all_friends" setting is enabled
            if db.get_setting("track_all_friends", "0") == "1":
                for fid in friend_ids:
                    u_info = self.known_users.get(fid, {})
                    db.upsert_tracked_user(
                        user_id=fid,
                        username=u_info.get("username"),
                        global_name=u_info.get("global_name"),
                        avatar=u_info.get("avatar"),
                        is_tracked=1
                    )

            # Process initial presences from READY payload & guilds
            tracked_map = {u["user_id"]: u for u in db.get_tracked_users(only_tracked=True)}
            all_presences = list(d.get("presences", []))
            for g in d.get("guilds", []):
                all_presences.extend(g.get("presences", []))

            seen_in_ready = set()
            for p in all_presences:
                pu = p.get("user") or {}
                p_uid = str(pu.get("id", ""))
                if not p_uid:
                    continue
                seen_in_ready.add(p_uid)
                # Cache presence even for non-tracked known users so adding them later shows immediate status
                if p_uid in self.known_users:
                    self.known_users[p_uid]["status"] = p.get("status", "offline")
                    self.known_users[p_uid]["client_status"] = p.get("client_status") or {}
                    self.known_users[p_uid]["activities"] = parse_activities(p.get("activities") or [])

                if p_uid in tracked_map:
                    await self._process_presence_payload(p, initial_sync=True)

            # Subscribe to tracked users across guilds so non-friend server members also send PRESENCE_UPDATE
            if tracked_map:
                asyncio.create_task(self.subscribe_to_users_in_guilds(list(tracked_map.keys())))

            await self.broadcast_ui({"type": "gateway_status", "data": self.get_status_info()})
            await self.broadcast_ui({"type": "users_updated", "data": db.get_tracked_users(only_tracked=True)})

        elif event_name == "READY_SUPPLEMENTAL":
            # Discord sends extra initial presences for friends and guilds in READY_SUPPLEMENTAL!
            tracked_map = {u["user_id"]: u for u in db.get_tracked_users(only_tracked=True)}
            merged_presences = list((d.get("merged_presences") or {}).get("friends", []))
            for g_pres in (d.get("merged_presences") or {}).get("guilds", []):
                if isinstance(g_pres, list):
                    merged_presences.extend(g_pres)

            for p in merged_presences:
                p_uid = str(p.get("user_id") or (p.get("user") or {}).get("id") or "")
                if not p_uid:
                    continue
                if p_uid in self.known_users:
                    self.known_users[p_uid]["status"] = p.get("status", "offline")
                    self.known_users[p_uid]["client_status"] = p.get("client_status") or {}
                    self.known_users[p_uid]["activities"] = parse_activities(p.get("activities") or [])
                if p_uid in tracked_map:
                    await self._process_presence_payload(p, user_id_override=p_uid, initial_sync=True)

            await self.broadcast_ui({"type": "users_updated", "data": db.get_tracked_users(only_tracked=True)})

        elif event_name == "GUILD_MEMBERS_CHUNK":
            # Response to Opcode 8: contains members and their live presences/activities!
            # IMPORTANT DISCORD PROTOCOL RULE: If a member is in `members` but OMITTED from `presences`,
            # it means they are currently OFFLINE with no activities!
            chunk_guild_id = str(d.get("guild_id") or "")
            tracked_map = {u["user_id"]: u for u in db.get_tracked_users(only_tracked=True)}

            chunk_member_ids = set()
            for m in d.get("members", []):
                mu = m.get("user") or {}
                mu_id = str(mu.get("id", ""))
                if mu_id:
                    chunk_member_ids.add(mu_id)
                    if chunk_guild_id:
                        self.user_guilds_map.setdefault(mu_id, set()).add(chunk_guild_id)
                    existing = self.known_users.get(mu_id, {})
                    self.known_users[mu_id] = {
                        "user_id": mu_id,
                        "username": mu.get("username") or existing.get("username", ""),
                        "global_name": mu.get("global_name") or mu.get("username") or existing.get("global_name", ""),
                        "avatar": format_avatar_url(mu_id, mu.get("avatar")),
                        "is_friend": existing.get("is_friend", False),
                        "status": existing.get("status", "offline"),
                        "client_status": existing.get("client_status", {}),
                        "activities": existing.get("activities", [])
                    }

            present_user_ids = set()
            for p in d.get("presences", []):
                p_uid = str((p.get("user") or {}).get("id") or p.get("user_id") or "")
                if not p_uid:
                    continue
                present_user_ids.add(p_uid)
                if p_uid in self.known_users:
                    self.known_users[p_uid]["status"] = p.get("status", "offline")
                    self.known_users[p_uid]["client_status"] = p.get("client_status") or {}
                    self.known_users[p_uid]["activities"] = parse_activities(p.get("activities") or [])
                if p_uid in tracked_map:
                    await self._process_presence_payload(p, user_id_override=p_uid)

            # Anyone in chunk_member_ids who is NOT in present_user_ids is 100% OFFLINE!
            for m_uid in chunk_member_ids:
                if m_uid not in present_user_ids:
                    if m_uid in self.known_users:
                        self.known_users[m_uid]["status"] = "offline"
                        self.known_users[m_uid]["client_status"] = {}
                        self.known_users[m_uid]["activities"] = []
                    if m_uid in tracked_map:
                        offline_payload = {
                            "user": {"id": m_uid},
                            "status": "offline",
                            "client_status": {},
                            "activities": []
                        }
                        await self._process_presence_payload(offline_payload, user_id_override=m_uid)

            await self.broadcast_ui({"type": "users_updated", "data": db.get_tracked_users(only_tracked=True)})

        elif event_name == "PRESENCE_UPDATE":
            p_uid = str((d.get("user") or {}).get("id") or d.get("user_id") or "")
            if not p_uid:
                return

            # Update known_users cache if present
            u_obj = d.get("user") or {}
            if p_uid not in self.known_users and u_obj.get("username"):
                self.known_users[p_uid] = {
                    "user_id": p_uid,
                    "username": u_obj.get("username", ""),
                    "global_name": u_obj.get("global_name") or u_obj.get("username", ""),
                    "avatar": format_avatar_url(p_uid, u_obj.get("avatar"))
                }
            if p_uid in self.known_users:
                if u_obj.get("username"):
                    self.known_users[p_uid]["username"] = u_obj.get("username")
                if u_obj.get("global_name"):
                    self.known_users[p_uid]["global_name"] = u_obj.get("global_name")
                if u_obj.get("avatar"):
                    self.known_users[p_uid]["avatar"] = format_avatar_url(p_uid, u_obj.get("avatar"))
                self.known_users[p_uid]["status"] = d.get("status", "offline")
                self.known_users[p_uid]["client_status"] = d.get("client_status") or {}
                self.known_users[p_uid]["activities"] = parse_activities(d.get("activities") or [])

            # Check if user is tracked
            tracked_user = db.get_tracked_user(p_uid)
            if tracked_user and tracked_user.get("is_tracked") == 1:
                await self._process_presence_payload(d, user_id_override=p_uid)

        elif event_name == "VOICE_STATE_UPDATE":
            v_uid = str(d.get("user_id") or (d.get("member") or {}).get("user", {}).get("id") or "")
            if not v_uid:
                return
            tracked_user = db.get_tracked_user(v_uid)
            if not tracked_user or tracked_user.get("is_tracked") != 1:
                return

            channel_id = d.get("channel_id")
            guild_id = str(d.get("guild_id") or "")
            guild_name = (self.guild_details.get(guild_id) or {}).get("name") or f"Sunucu ({guild_id[-4:] if guild_id else 'DM'})"
            is_Offline = (tracked_user.get("current_status") or "offline") == "offline"

            flags = []
            if d.get("self_stream"):
                flags.append("Ekran Yayını Açık")
            if d.get("self_video"):
                flags.append("Kamera Açık")
            if d.get("self_deaf"):
                flags.append("Kulaklık Kapalı")
            elif d.get("self_mute"):
                flags.append("Mikrofon Kapalı")
            flag_str = f" ({', '.join(flags)})" if flags else ""

            state_key = f"{channel_id}:{flag_str}"
            if tracked_user.get("last_voice_state") == state_key:
                return

            conn = db.get_conn()
            conn.execute("UPDATE tracked_users SET last_voice_state = ? WHERE user_id = ?", (state_key, v_uid))
            conn.commit()
            conn.close()

            if channel_id:
                if is_Offline:
                    summary = f"🚨 [GÖRÜNMEZ (INVIS) YAKALANDI] Discord'da Çevrimdışı görünmesine rağmen '{guild_name}' sunucusunda sesli kanala (#{channel_id}) katıldı!{flag_str}"
                    ev_type = "invis_detected"
                else:
                    summary = f"🎙️ Sesli kanala katıldı veya durumunu güncelledi: '{guild_name}' (#{channel_id}){flag_str}"
                    ev_type = "voice_activity"
            else:
                summary = f"🎙️ '{guild_name}' sunucusundaki sesli kanaldan ayrıldı."
                ev_type = "voice_activity"

            log_entry = db.insert_special_or_manual_log(
                user_id=v_uid,
                event_type=ev_type,
                summary=summary,
                activity_obj={"name": "Sesli Kanal", "type": 0, "type_name": "Sesli Oda", "details": guild_name, "state": flag_str.strip(" ()")} if channel_id else None
            )
            if log_entry:
                await self.broadcast_ui({"type": "presence_log", "log": log_entry, "user": db.get_tracked_user(v_uid)})
                webhook_url = db.get_setting("webhook_url", "").strip()
                if webhook_url:
                    asyncio.create_task(self._send_webhook_notification(webhook_url, log_entry))

        elif event_name == "TYPING_START":
            t_uid = str(d.get("user_id") or "")
            if not t_uid:
                return
            tracked_user = db.get_tracked_user(t_uid)
            if not tracked_user or tracked_user.get("is_tracked") != 1:
                return
            # Only alert if the user is pretending to be OFFLINE (Invisible)!
            if (tracked_user.get("current_status") or "offline") == "offline":
                now_ep = asyncio.get_event_loop().time()
                last_invis_t = getattr(self, "_invis_typing_cd", {}).get(t_uid, 0)
                if (now_ep - last_invis_t) < 120:
                    return
                if not hasattr(self, "_invis_typing_cd"):
                    self._invis_typing_cd = {}
                self._invis_typing_cd[t_uid] = now_ep

                guild_id = str(d.get("guild_id") or "")
                loc_name = (self.guild_details.get(guild_id) or {}).get("name") if guild_id else "Özel Mesaj (DM)"
                summary = f"🚨 [GÖRÜNMEZ (INVIS) YAKALANDI] Discord'da Çevrimdışı (Invisible) görünmesine rağmen '{loc_name or 'Sunucu'}' içinde şu an bir mesaj yazıyor (Typing)!"
                log_entry = db.insert_special_or_manual_log(
                    user_id=t_uid,
                    event_type="invis_detected",
                    summary=summary
                )
                if log_entry:
                    await self.broadcast_ui({"type": "presence_log", "log": log_entry, "user": db.get_tracked_user(t_uid)})
                    webhook_url = db.get_setting("webhook_url", "").strip()
                    if webhook_url:
                        asyncio.create_task(self._send_webhook_notification(webhook_url, log_entry))

        elif event_name == "MESSAGE_CREATE":
            author = d.get("author") or {}
            m_uid = str(author.get("id") or "")
            if not m_uid:
                return
            tracked_user = db.get_tracked_user(m_uid)
            if not tracked_user or tracked_user.get("is_tracked") != 1:
                return
            if (tracked_user.get("current_status") or "offline") == "offline":
                now_ep = asyncio.get_event_loop().time()
                last_invis_m = getattr(self, "_invis_msg_cd", {}).get(m_uid, 0)
                if (now_ep - last_invis_m) < 120:
                    return
                if not hasattr(self, "_invis_msg_cd"):
                    self._invis_msg_cd = {}
                self._invis_msg_cd[m_uid] = now_ep

                guild_id = str(d.get("guild_id") or "")
                loc_name = (self.guild_details.get(guild_id) or {}).get("name") if guild_id else "Özel Mesaj (DM)"
                summary = f"🚨 [GÖRÜNMEZ (INVIS) YAKALANDI] Discord'da Çevrimdışı (Invisible) görünmesine rağmen '{loc_name or 'Sunucu'}' kanalına mesaj gönderdi!"
                log_entry = db.insert_special_or_manual_log(
                    user_id=m_uid,
                    event_type="invis_detected",
                    summary=summary
                )
                if log_entry:
                    await self.broadcast_ui({"type": "presence_log", "log": log_entry, "user": db.get_tracked_user(m_uid)})
                    webhook_url = db.get_setting("webhook_url", "").strip()
                    if webhook_url:
                        asyncio.create_task(self._send_webhook_notification(webhook_url, log_entry))

    async def _process_presence_payload(
        self,
        p: Dict[str, Any],
        user_id_override: Optional[str] = None,
        initial_sync: bool = False
    ):
        u_obj = p.get("user") or {}
        uid = user_id_override or str(u_obj.get("id") or p.get("user_id") or "")
        if not uid:
            return

        cached = self.known_users.get(uid, {})
        username = u_obj.get("username") or cached.get("username")
        global_name = u_obj.get("global_name") or cached.get("global_name") or username
        avatar_hash = u_obj.get("avatar")
        avatar_url = format_avatar_url(uid, avatar_hash) if avatar_hash else cached.get("avatar")

        status = p.get("status", "offline")
        client_status = p.get("client_status") or {}
        activities = parse_activities(p.get("activities") or [])

        log_entry = db.record_presence_update(
            user_id=uid,
            username=username,
            global_name=global_name,
            avatar=avatar_url,
            status=status,
            client_status=client_status,
            activities=activities,
            initial_sync=initial_sync
        )

        if log_entry:
            await self.broadcast_ui({
                "type": "presence_log",
                "log": log_entry,
                "user": db.get_tracked_user(uid)
            })
            webhook_url = db.get_setting("webhook_url", "").strip()
            if webhook_url:
                asyncio.create_task(self._send_webhook_notification(webhook_url, log_entry))

    async def _send_webhook_notification(self, webhook_url: str, log_entry: Dict[str, Any]):
        try:
            colors = {
                "login": 0x23A55A,
                "logout": 0xF23F43,
                "platform_change": 0x0EA5E9,
                "activity_change": 0xF59E0B,
                "status_change": 0x6366F1
            }
            color = colors.get(log_entry.get("event_type", ""), 0x5865F2)
            plats = log_entry.get("new_platforms") or {}
            active_plats = [k.upper() for k, v in plats.items() if v != "offline"]
            plat_text = ", ".join(active_plats) if active_plats else "Çevrimdışı"

            embed = {
                "author": {
                    "name": f"{log_entry.get('username')} ({log_entry.get('user_id')})",
                    "icon_url": log_entry.get("avatar") or "https://cdn.discordapp.com/embed/avatars/0.png"
                },
                "description": f"**{log_entry.get('summary')}**",
                "color": color,
                "fields": [
                    {"name": "Durum", "value": f"`{log_entry.get('new_status', 'offline').upper()}`", "inline": True},
                    {"name": "Aktif Platformlar", "value": f"`{plat_text}`", "inline": True}
                ],
                "timestamp": log_entry.get("timestamp")
            }
            async with aiohttp.ClientSession() as s:
                await s.post(webhook_url, json={"username": "Discord Presence Radar", "embeds": [embed]}, timeout=8)
        except Exception as e:
            logger.debug(f"Webhook delivery error: {e}")

