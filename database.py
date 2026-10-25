import sqlite3
import json
import os
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stalker.db")

# In-memory cooldown tracker for noisy activity sub-text updates: {(user_id, activity_name): last_logged_epoch}
_activity_update_cooldowns: Dict[Tuple[str, str], float] = {}


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # Settings table (stores discord token, track_all_friends flag, etc.)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # Tracked users table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tracked_users (
            user_id TEXT PRIMARY KEY,
            username TEXT,
            global_name TEXT,
            avatar TEXT,
            note TEXT,
            is_tracked INTEGER DEFAULT 1,
            current_status TEXT DEFAULT 'offline',
            desktop_status TEXT DEFAULT 'offline',
            mobile_status TEXT DEFAULT 'offline',
            web_status TEXT DEFAULT 'offline',
            embedded_status TEXT DEFAULT 'offline',
            current_activities TEXT DEFAULT '[]',
            last_seen_online TEXT,
            last_updated TEXT,
            added_at TEXT
        )
    """)

    # Event logs table (every transition: status change, platform change, activity change)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS presence_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            username TEXT,
            event_type TEXT,
            old_status TEXT,
            new_status TEXT,
            old_platforms TEXT,
            new_platforms TEXT,
            old_activities TEXT,
            new_activities TEXT,
            summary TEXT,
            timestamp TEXT
        )
    """)

    # Online sessions table (tracks exact start/end times and duration of online sessions)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS status_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            status TEXT,
            platforms TEXT,
            started_at TEXT,
            ended_at TEXT,
            duration_seconds INTEGER DEFAULT 0
        )
    """)

    # Activity sessions table (tracks games, Spotify, Custom Status, Voice/Streaming duration)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS activity_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            activity_name TEXT,
            activity_type TEXT,
            details TEXT,
            state TEXT,
            started_at TEXT,
            ended_at TEXT,
            duration_seconds INTEGER DEFAULT 0
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_user_time ON presence_logs(user_id, timestamp DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_time ON presence_logs(timestamp DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user ON status_sessions(user_id, started_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_act_sessions_user ON activity_sessions(user_id, started_at DESC)")

    conn.commit()
    conn.close()


def get_setting(key: str, default: str = "") -> str:
    conn = get_conn()
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def set_setting(key: str, value: str):
    conn = get_conn()
    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value)
    )
    conn.commit()
    conn.close()


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def upsert_tracked_user(
    user_id: str,
    username: str = None,
    global_name: str = None,
    avatar: str = None,
    note: str = None,
    is_tracked: int = 1
) -> Dict[str, Any]:
    conn = get_conn()
    existing = conn.execute("SELECT * FROM tracked_users WHERE user_id = ?", (user_id,)).fetchone()
    ts = now_iso()

    if existing:
        conn.execute("""
            UPDATE tracked_users SET
                username = COALESCE(?, username),
                global_name = COALESCE(?, global_name),
                avatar = COALESCE(?, avatar),
                note = COALESCE(?, note),
                is_tracked = COALESCE(?, is_tracked)
            WHERE user_id = ?
        """, (username, global_name, avatar, note, is_tracked, user_id))
    else:
        conn.execute("""
            INSERT INTO tracked_users (
                user_id, username, global_name, avatar, note, is_tracked,
                current_status, desktop_status, mobile_status, web_status, embedded_status,
                current_activities, last_seen_online, last_updated, added_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'offline', 'offline', 'offline', 'offline', 'offline', '[]', NULL, ?, ?)
        """, (
            user_id,
            username or f"User_{user_id[-4:]}",
            global_name or username or f"User {user_id[-4:]}",
            avatar or "",
            note or "",
            is_tracked,
            ts,
            ts
        ))

    conn.commit()
    row = conn.execute("SELECT * FROM tracked_users WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    return dict(row) if row else {}


def remove_tracked_user(user_id: str):
    conn = get_conn()
    conn.execute("DELETE FROM tracked_users WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def get_tracked_users(only_tracked: bool = True) -> List[Dict[str, Any]]:
    conn = get_conn()
    if only_tracked:
        rows = conn.execute("SELECT * FROM tracked_users WHERE is_tracked = 1 ORDER BY CASE WHEN current_status != 'offline' THEN 0 ELSE 1 END, last_updated DESC").fetchall()
    else:
        rows = conn.execute("SELECT * FROM tracked_users ORDER BY CASE WHEN current_status != 'offline' THEN 0 ELSE 1 END, last_updated DESC").fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["current_activities"] = json.loads(d["current_activities"] or "[]")
        except Exception:
            d["current_activities"] = []
        result.append(d)
    return result


def get_tracked_user(user_id: str) -> Optional[Dict[str, Any]]:
    conn = get_conn()
    row = conn.execute("SELECT * FROM tracked_users WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    try:
        d["current_activities"] = json.loads(d["current_activities"] or "[]")
    except Exception:
        d["current_activities"] = []
    return d


def record_presence_update(
    user_id: str,
    username: str,
    global_name: str,
    avatar: str,
    status: str,
    client_status: Dict[str, str],
    activities: List[Dict[str, Any]],
    initial_sync: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Compares incoming presence with stored state in DB.
    If anything changed (status, platform client_status, or activities), logs the transition
    and updates active sessions.
    """
    conn = get_conn()
    row = conn.execute("SELECT * FROM tracked_users WHERE user_id = ?", (user_id,)).fetchone()
    if not row:
        conn.close()
        return None

    user = dict(row)
    ts = now_iso()

    desktop = client_status.get("desktop", "offline")
    mobile = client_status.get("mobile", "offline")
    web = client_status.get("web", "offline")
    embedded = client_status.get("embedded", "offline")

    old_status = user["current_status"] or "offline"
    old_platforms = {
        "desktop": user["desktop_status"] or "offline",
        "mobile": user["mobile_status"] or "offline",
        "web": user["web_status"] or "offline",
        "embedded": user["embedded_status"] or "offline"
    }
    new_platforms = {
        "desktop": desktop,
        "mobile": mobile,
        "web": web,
        "embedded": embedded
    }

    try:
        old_activities = json.loads(user["current_activities"] or "[]")
    except Exception:
        old_activities = []

    # Normalize activities for comparison (ignore timestamps inside activity)
    def norm_acts(acts):
        return [
            {
                "name": a.get("name", ""),
                "type": a.get("type", 0),
                "details": a.get("details", ""),
                "state": a.get("state", "")
            }
            for a in acts
        ]

    status_changed = (old_status != status)
    platforms_changed = (old_platforms != new_platforms)
    activities_changed = (norm_acts(old_activities) != norm_acts(activities))

    last_seen = ts if status != "offline" else user["last_seen_online"]

    # Always update user metadata & current state
    conn.execute("""
        UPDATE tracked_users SET
            username = COALESCE(?, username),
            global_name = COALESCE(?, global_name),
            avatar = COALESCE(?, avatar),
            current_status = ?,
            desktop_status = ?,
            mobile_status = ?,
            web_status = ?,
            embedded_status = ?,
            current_activities = ?,
            last_seen_online = ?,
            last_updated = ?
        WHERE user_id = ?
    """, (
        username,
        global_name,
        avatar,
        status,
        desktop,
        mobile,
        web,
        embedded,
        json.dumps(activities, ensure_ascii=False),
        last_seen,
        ts,
        user_id
    ))

    if not status_changed and not platforms_changed and not activities_changed:
        conn.commit()
        conn.close()
        return None

    # Build human-readable Turkish summary of what happened
    display_name = global_name or username or user["global_name"] or user["username"] or user_id
    summary_parts = []
    event_type = "update"

    status_tr = {
        "online": "Çevrimiçi (Online)",
        "idle": "Boşta (Idle)",
        "dnd": "Rahatsız Etmeyin (DND)",
        "offline": "Çevrimdışı (Offline)"
    }
    platform_tr = {
        "desktop": "Bilgisayar (PC)",
        "mobile": "Mobil (Telefon)",
        "web": "Web Tarayıcı",
        "embedded": "Konsol / Cihaz"
    }

    if old_status == "offline" and status != "offline":
        event_type = "login"
        active_plats = [platform_tr[k] for k, v in new_platforms.items() if v != "offline"]
        plat_str = f" [{', '.join(active_plats)}]" if active_plats else ""
        summary_parts.append(f"Giriş yaptı: {status_tr.get(status, status)}{plat_str}")
    elif old_status != "offline" and status == "offline":
        event_type = "logout"
        old_plats = [platform_tr[k] for k, v in old_platforms.items() if v != "offline"]
        plat_str = f" (Son aktif: {', '.join(old_plats)})" if old_plats else ""
        summary_parts.append(f"Çıkış yaptı (Çevrimdışı oldu){plat_str}")
    elif status_changed:
        event_type = "status_change"
        summary_parts.append(f"Durum değişti: {status_tr.get(old_status, old_status)} ➔ {status_tr.get(status, status)}")

    if platforms_changed and event_type not in ("login", "logout"):
        event_type = "platform_change" if event_type == "update" else event_type
        plat_changes = []
        for p_key, p_name in platform_tr.items():
            ov = old_platforms.get(p_key, "offline")
            nv = new_platforms.get(p_key, "offline")
            if ov == "offline" and nv != "offline":
                plat_changes.append(f"{p_name} üzerinden bağlandı ({status_tr.get(nv, nv)})")
            elif ov != "offline" and nv == "offline":
                plat_changes.append(f"{p_name} bağlantısını kapattı")
            elif ov != nv:
                plat_changes.append(f"{p_name} durumu: {status_tr.get(ov, ov)} ➔ {status_tr.get(nv, nv)}")
        if plat_changes:
            summary_parts.append(" | ".join(plat_changes))

    if activities_changed:
        if event_type == "update":
            event_type = "activity_change"
        old_names = {a.get("name"): a for a in old_activities if a.get("name")}
        new_names = {a.get("name"): a for a in activities if a.get("name")}

        started = [a for name, a in new_names.items() if name not in old_names]
        stopped = [a for name, a in old_names.items() if name not in new_names]
        updated = [a for name, a in new_names.items() if name in old_names and (
            a.get("details") != old_names[name].get("details") or a.get("state") != old_names[name].get("state")
        )]

        # Apply Activity Update Cooldown so per-second RPC changes don't bloat the database
        try:
            cooldown_Row = conn.execute("SELECT value FROM settings WHERE key = 'activity_cooldown'").fetchone()
            cooldown_sec = int(cooldown_Row["value"]) if cooldown_Row and cooldown_Row["value"] != "" else 180
        except Exception:
            cooldown_sec = 180

        now_epoch = time.time()
        filtered_updated = []
        for a in updated:
            act_name = a.get("name", "Aktivite")
            # If cooldown_sec == -1, user chose "Never log sub-updates (only start/stop)"
            if cooldown_sec < 0:
                continue
            # For Spotify, if the song title (details) changed, log it unless changed within 15s
            is_spotify_new_song = (act_name == "Spotify" and a.get("details") != old_names.get(act_name, {}).get("details"))
            effective_cd = 15 if is_spotify_new_song else cooldown_sec

            key = (user_id, act_name)
            last_t = _activity_update_cooldowns.get(key, 0.0)
            if (now_epoch - last_t) >= effective_cd:
                _activity_update_cooldowns[key] = now_epoch
                filtered_updated.append(a)

        for a in started:
            act_label = a.get("name", "Aktivite")
            _activity_update_cooldowns[(user_id, act_label)] = now_epoch
            detail_str = f" ({a.get('details') or a.get('state')})" if (a.get("details") or a.get("state")) else ""
            summary_parts.append(f"Aktivite başladı: {act_label}{detail_str}")
        for a in stopped:
            act_label = a.get("name", "Aktivite")
            _activity_update_cooldowns.pop((user_id, act_label), None)
            summary_parts.append(f"Aktivite bitti: {act_label}")
        for a in filtered_updated:
            act_label = a.get("name", "Aktivite")
            detail_str = " - ".join([x for x in [a.get("details"), a.get("state")] if x])
            summary_parts.append(f"Aktivite güncellendi: {act_label} ({detail_str})")

    # If the only thing that happened was a throttled/ignored activity sub-update, commit the live card state and skip inserting a log row!
    if not summary_parts:
        conn.commit()
        conn.close()
        return None

    summary = " • ".join(summary_parts)

    # Manage status_sessions
    if status_changed or platforms_changed:
        # Close open status session if any
        open_sess = conn.execute(
            "SELECT id, started_at FROM status_sessions WHERE user_id = ? AND ended_at IS NULL ORDER BY id DESC LIMIT 1",
            (user_id,)
        ).fetchone()
        if open_sess:
            try:
                dt_start = datetime.fromisoformat(open_sess["started_at"])
                dt_end = datetime.fromisoformat(ts)
                dur = max(0, int((dt_end - dt_start).total_seconds()))
            except Exception:
                dur = 0
            conn.execute(
                "UPDATE status_sessions SET ended_at = ?, duration_seconds = ? WHERE id = ?",
                (ts, dur, open_sess["id"])
            )

        # Open new session if not offline
        if status != "offline":
            active_p = [k for k, v in new_platforms.items() if v != "offline"]
            conn.execute(
                "INSERT INTO status_sessions (user_id, status, platforms, started_at, ended_at, duration_seconds) VALUES (?, ?, ?, ?, NULL, 0)",
                (user_id, status, json.dumps(active_p), ts)
            )

    # Manage activity_sessions
    if activities_changed:
        open_act_rows = conn.execute(
            "SELECT id, activity_name, started_at FROM activity_sessions WHERE user_id = ? AND ended_at IS NULL",
            (user_id,)
        ).fetchall()
        open_act_map = {r["activity_name"]: r for r in open_act_rows}
        current_act_map = {a.get("name", ""): a for a in activities if a.get("name")}

        # Close stopped activities
        for act_name, r in open_act_map.items():
            if act_name not in current_act_map:
                try:
                    dt_start = datetime.fromisoformat(r["started_at"])
                    dt_end = datetime.fromisoformat(ts)
                    dur = max(0, int((dt_end - dt_start).total_seconds()))
                except Exception:
                    dur = 0
                conn.execute(
                    "UPDATE activity_sessions SET ended_at = ?, duration_seconds = ? WHERE id = ?",
                    (ts, dur, r["id"])
                )

        # Open newly started activities
        for act_name, a in current_act_map.items():
            if act_name not in open_act_map:
                conn.execute("""
                    INSERT INTO activity_sessions (user_id, activity_name, activity_type, details, state, started_at, ended_at, duration_seconds)
                    VALUES (?, ?, ?, ?, ?, ?, NULL, 0)
                """, (
                    user_id,
                    act_name,
                    str(a.get("type_name", a.get("type", "playing"))),
                    a.get("details", ""),
                    a.get("state", ""),
                    ts
                ))

    # Insert log entry
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO presence_logs (
            user_id, username, event_type,
            old_status, new_status,
            old_platforms, new_platforms,
            old_activities, new_activities,
            summary, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        user_id,
        display_name,
        event_type,
        old_status,
        status,
        json.dumps(old_platforms),
        json.dumps(new_platforms),
        json.dumps(old_activities, ensure_ascii=False),
        json.dumps(activities, ensure_ascii=False),
        summary,
        ts
    ))
    log_id = cur.lastrowid
    conn.commit()
    conn.close()

    return {
        "id": log_id,
        "user_id": user_id,
        "username": display_name,
        "avatar": avatar or user.get("avatar", ""),
        "event_type": event_type,
        "old_status": old_status,
        "new_status": status,
        "old_platforms": old_platforms,
        "new_platforms": new_platforms,
        "old_activities": old_activities,
        "new_activities": activities,
        "summary": summary,
        "timestamp": ts
    }


def get_total_logs_count() -> int:
    conn = get_conn()
    row = conn.execute("SELECT COUNT(*) as cnt FROM presence_logs").fetchone()
    conn.close()
    return (row["cnt"] or 0) if row else 0


def get_logs(user_id: Optional[str] = None, event_type: Optional[str] = None, limit: int = 500) -> List[Dict[str, Any]]:
    conn = get_conn()
    query = """
        SELECT l.*, u.avatar, u.global_name
        FROM presence_logs l
        LEFT JOIN tracked_users u ON l.user_id = u.user_id
        WHERE 1=1
    """
    params = []
    if user_id:
        query += " AND l.user_id = ?"
        params.append(user_id)
    if event_type and event_type != "all":
        query += " AND l.event_type = ?"
        params.append(event_type)

    query += " ORDER BY l.id DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()

    result = []
    for r in rows:
        d = dict(r)
        for field in ("old_platforms", "new_platforms", "old_activities", "new_activities"):
            try:
                d[field] = json.loads(d[field] or ("{}" if "platforms" in field else "[]"))
            except Exception:
                d[field] = {} if "platforms" in field else []
        result.append(d)
    return result


def get_user_analytics(user_id: str) -> Dict[str, Any]:
    conn = get_conn()
    user = get_tracked_user(user_id)

    # Status sessions (including ongoing session duration calculated live)
    sessions = [dict(r) for r in conn.execute(
        "SELECT * FROM status_sessions WHERE user_id = ? ORDER BY id DESC LIMIT 100",
        (user_id,)
    ).fetchall()]

    now_dt = datetime.now().astimezone()
    total_status_sec = {"online": 0, "idle": 0, "dnd": 0}
    platform_sec = {"desktop": 0, "mobile": 0, "web": 0, "embedded": 0}
    hourly_activity = [0] * 24

    for s in sessions:
        dur = s["duration_seconds"]
        if not s["ended_at"]:
            try:
                dt_start = datetime.fromisoformat(s["started_at"])
                dur = max(0, int((now_dt - dt_start).total_seconds()))
                s["duration_seconds"] = dur
            except Exception:
                pass
        st = s["status"]
        if st in total_status_sec:
            total_status_sec[st] += dur

        try:
            plats = json.loads(s["platforms"] or "[]")
            for p in plats:
                if p in platform_sec:
                    platform_sec[p] += dur
            s["platforms"] = plats
        except Exception:
            s["platforms"] = []

    # Hourly distribution from logs
    log_times = conn.execute(
        "SELECT timestamp FROM presence_logs WHERE user_id = ?",
        (user_id,)
    ).fetchall()
    for lt in log_times:
        try:
            dt = datetime.fromisoformat(lt["timestamp"])
            hourly_activity[dt.hour] += 1
        except Exception:
            pass

    # Top activities
    act_rows = [dict(r) for r in conn.execute(
        "SELECT * FROM activity_sessions WHERE user_id = ? ORDER BY id DESC LIMIT 100",
        (user_id,)
    ).fetchall()]

    act_totals: Dict[str, int] = {}
    for a in act_rows:
        dur = a["duration_seconds"]
        if not a["ended_at"]:
            try:
                dt_start = datetime.fromisoformat(a["started_at"])
                dur = max(0, int((now_dt - dt_start).total_seconds()))
                a["duration_seconds"] = dur
            except Exception:
                pass
        name = a["activity_name"]
        if name:
            act_totals[name] = act_totals.get(name, 0) + dur

    top_activities = [
        {"name": k, "total_seconds": v}
        for k, v in sorted(act_totals.items(), key=lambda item: item[1], reverse=True)
    ][:10]

    # Counts of logins / logouts
    counts = conn.execute("""
        SELECT
            SUM(CASE WHEN event_type = 'login' THEN 1 ELSE 0 END) as logins,
            SUM(CASE WHEN event_type = 'logout' THEN 1 ELSE 0 END) as logouts,
            COUNT(*) as total_events
        FROM presence_logs WHERE user_id = ?
    """, (user_id,)).fetchone()

    conn.close()

    return {
        "user": user,
        "status_seconds": total_status_sec,
        "platform_seconds": platform_sec,
        "hourly_activity": hourly_activity,
        "top_activities": top_activities,
        "recent_sessions": sessions[:30],
        "recent_activity_sessions": act_rows[:30],
        "stats": {
            "logins": (counts["logins"] or 0) if counts else 0,
            "logouts": (counts["logouts"] or 0) if counts else 0,
            "total_events": (counts["total_events"] or 0) if counts else 0
        }
    }
