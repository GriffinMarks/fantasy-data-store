#!/usr/bin/env python3
import os, json, pathlib, datetime as dt, time
import requests
from typing import Any, Dict, List, Set

# -------- CONFIG --------
LEAGUE_ID = os.getenv("SLEEPER_LEAGUE_ID", "1262790170931892224")
SEASON    = int(os.getenv("SEASON", "2025"))
WEEK      = int(os.getenv("WEEK", "6"))  # default Week 6
TREND_H   = int(os.getenv("TRENDING_LOOKBACK_H", "24"))
TREND_N   = int(os.getenv("TRENDING_LIMIT", "100"))

BASE = "https://api.sleeper.app/v1"
ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT  = ROOT / "data"
CACHE= OUT / "cache"
OUT.mkdir(parents=True, exist_ok=True)
CACHE.mkdir(parents=True, exist_ok=True)

def now_iso():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def write_json(relpath: str, payload: Dict[str, Any]):
    p = OUT / relpath
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"WROTE {p}")

def http_get(url: str, params: Dict[str, Any] = None, default=None):
    try:
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[WARN] GET {url} failed: {e}")
        return default

# -------- Sleeper fetchers --------
def get_league():    return http_get(f"{BASE}/league/{LEAGUE_ID}", default={})
def get_users():     return http_get(f"{BASE}/league/{LEAGUE_ID}/users", default=[])
def get_rosters():   return http_get(f"{BASE}/league/{LEAGUE_ID}/rosters", default=[])
def get_matchups(w): return http_get(f"{BASE}/league/{LEAGUE_ID}/matchups/{w}", default=[])
def get_tx(w):       return http_get(f"{BASE}/league/{LEAGUE_ID}/transactions/{w}", default=[])
def get_players_catalog():
    cache = CACHE / "players_nfl.json"
    if cache.exists() and (dt.datetime.utcnow() - dt.datetime.utcfromtimestamp(cache.stat().st_mtime) < dt.timedelta(hours=12)):
        return json.loads(cache.read_text("utf-8"))
    data = http_get(f"{BASE}/players/nfl", default={})
    cache.write_text(json.dumps(data), encoding="utf-8")
    return data

def get_trending(h, n):
    # Primary path
    adds  = http_get(f"{BASE}/players/trending/nfl", params={"type":"add","lookback_hours":h,"limit":n}, default=None)
    drops = http_get(f"{BASE}/players/trending/nfl", params={"type":"drop","lookback_hours":h,"limit":n}, default=None)
    # Fallback path (some mirrors expose /players/nfl/trending)
    if adds is None:
        adds = http_get(f"{BASE}/players/nfl/trending", params={"type":"add","lookback_hours":h,"limit":n}, default=[])
    if drops is None:
        drops = http_get(f"{BASE}/players/nfl/trending", params={"type":"drop","lookback_hours":h,"limit":n}, default=[])
    return {"generated_at": now_iso(), "lookback_hours": h, "limit": n, "adds": adds or [], "drops": drops or []}

# -------- Builders --------
def decorate(pid: str, catalog: Dict[str, Any]):
    p = catalog.get(str(pid)) or {}
    return {
        "player_id": str(pid),
        "full_name": p.get("full_name") or p.get("name"),
        "first_name": p.get("first_name"),
        "last_name": p.get("last_name"),
        "team": p.get("team"),
        "position": p.get("position"),
        "status": p.get("status"),
        "injury_status": p.get("injury_status"),
        "injury_body_part": p.get("injury_body_part"),
        "injury_start_date": p.get("injury_start_date"),
        "news_updated": p.get("news_updated"),
    }

def build_rosters(rosters, users, catalog):
    user_by_id = {u.get("user_id"): u for u in users}
    rostered: Set[str] = set()
    out = []

    for r in rosters:
        plist = r.get("players") or []
        slist = r.get("starters") or []
        rostered.update(plist)

        u = user_by_id.get(r.get("owner_id"), {})
        owner = u.get("display_name") or u.get("username")
        team_name = (u.get("metadata") or {}).get("team_name")

        starters = [decorate(pid, catalog) for pid in slist if pid]
        bench    = [decorate(pid, catalog) for pid in plist if pid and pid not in slist]

        out.append({
            "roster_id": r.get("roster_id"),
            "owner_id": r.get("owner_id"),
            "owner": owner,
            "team_name": team_name,
            "wins": (r.get("settings") or {}).get("wins"),
            "losses": (r.get("settings") or {}).get("losses"),
            "ties": (r.get("settings") or {}).get("ties"),
            "points_for": (r.get("settings") or {}).get("fpts"),
            "points_against": (r.get("settings") or {}).get("fpts_against"),
            "starters": starters,
            "bench": bench,
            "taxi": r.get("taxi") or [],
            "ir": r.get("reserve") or [],
        })

    payload = {"season": SEASON, "generated_at": now_iso(), "league_id": LEAGUE_ID, "rosters": out}
    return payload, rostered

def build_standings(rosters_payload):
    rows = []
    for r in rosters_payload["rosters"]:
        w = r.get("wins") or 0
        l = r.get("losses") or 0
        t = r.get("ties") or 0
        pf= r.get("points_for") or 0
        rows.append({
            "roster_id": r["roster_id"],
            "owner": r.get("owner"),
            "team_name": r.get("team_name"),
            "wins": w, "losses": l, "ties": t,
            "win_pct": (w + 0.5*t) / max(1, (w + l + t)),
            "points_for": pf
        })
    rows.sort(key=lambda x: (-x["wins"], -x["win_pct"], -x["points_for"]))
    return {"season": SEASON, "generated_at": now_iso(), "league_id": LEAGUE_ID, "standings": rows}

def build_matchups(week, matchups, rosters, users):
    users_by_id   = {u.get("user_id"): u for u in users}
    roster_by_id  = {r.get("roster_id"): r for r in rosters}

    def owner_name(rid):
        rid_obj = roster_by_id.get(rid, {})
        uid = rid_obj.get("owner_id")
        u = users_by_id.get(uid, {})
        return (u.get("metadata") or {}).get("team_name") or u.get("display_name") or u.get("username")

    games = {}
    for m in matchups or []:
        mid = m.get("matchup_id")
        if mid is None: mid = 0
        games.setdefault(mid, []).append({
            "roster_id": m.get("roster_id"),
            "owner": owner_name(m.get("roster_id")),
            "starters": m.get("starters"),
            "points": m.get("points"),
            "projected_points": m.get("projected_points"),
        })

    return {
        "season": SEASON,
        "week": week,
        "generated_at": now_iso(),
        "league_id": LEAGUE_ID,
        "games": [{"matchup_id": k, "teams": v} for k, v in sorted(games.items(), key=lambda kv: kv[0])]
    }

def build_transactions(week, moves):
    return {
        "season": SEASON,
        "week": week,
        "generated_at": now_iso(),
        "league_id": LEAGUE_ID,
        "moves": moves or []
    }

def build_available(catalog, rostered_ids: Set[str]):
    available = []
    for pid, p in catalog.items():
        pos = p.get("position")
        if pos in {"QB","RB","WR","TE","K","DEF"} and pid not in rostered_ids:
            available.append({
                "player_id": pid,
                "full_name": p.get("full_name") or p.get("name"),
                "team": p.get("team"),
                "position": pos,
                "status": p.get("status")
            })
    available.sort(key=lambda x: (x["position"] or "ZZ", x["full_name"] or ""))
    return {"season": SEASON, "generated_at": now_iso(), "league_id": LEAGUE_ID, "count": len(available), "players": available}

def build_injuries(catalog, rostered_ids: Set[str]):
    injured = []
    for pid in rostered_ids:
        p = catalog.get(str(pid)) or {}
        if p.get("injury_status"):
            injured.append({
                "player_id": str(pid),
                "full_name": p.get("full_name") or p.get("name"),
                "team": p.get("team"),
                "position": p.get("position"),
                "injury_status": p.get("injury_status"),
                "injury_body_part": p.get("injury_body_part"),
                "injury_start_date": p.get("injury_start_date"),
                "news_updated": p.get("news_updated"),
            })
    injured.sort(key=lambda x: (x["position"] or "ZZ", x["full_name"] or ""))
    return {"season": SEASON, "generated_at": now_iso(), "league_id": LEAGUE_ID, "players": injured}

# -------- Main --------
def main():
    users   = get_users()
    rosters = get_rosters()
    catalog = get_players_catalog()

    rosters_payload, rostered_ids = build_rosters(rosters, users, catalog)
    write_json(f"rosters/{SEASON}.json", rosters_payload)

    standings_payload = build_standings(rosters_payload)
    write_json(f"standings/{SEASON}.json", standings_payload)

    matchups = get_matchups(WEEK)
    write_json(f"matchups/{SEASON}/week_{WEEK:02}.json", build_matchups(WEEK, matchups, rosters, users))

    tx = get_tx(WEEK)
    write_json(f"transactions/{SEASON}/week_{WEEK:02}.json", build_transactions(WEEK, tx))

    write_json(f"trending/{dt.datetime.utcnow():%Y-%m-%d}.json", get_trending(TREND_H, TREND_N))

    write_json(f"available/{SEASON}/week_{WEEK:02}.json", build_available(catalog, rostered_ids))
    write_json(f"injuries/{SEASON}/week_{WEEK:02}.json", build_injuries(catalog, rostered_ids))

    meta = {
        "season": SEASON,
        "latest_week": WEEK,
        "updated_at": now_iso(),
        "league_id": LEAGUE_ID,
        "datasets": ["rosters","standings","matchups","transactions","trending","available","injuries"]
    }
    write_json("meta.json", meta)

if __name__ == "__main__":
    main()
