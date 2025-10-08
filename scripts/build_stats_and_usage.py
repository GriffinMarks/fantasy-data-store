#!/usr/bin/env python3
import os, json, pathlib, datetime as dt, time
import requests
from typing import Dict, Any, List, Tuple
from collections import defaultdict

LEAGUE_ID = os.getenv("SLEEPER_LEAGUE_ID", "1262790170931892224")
def env_int(name: str, default: int) -> int:
    val = os.getenv(name, "")
    try:
        return int(val)
    except Exception:
        return default

SEASON = env_int("SEASON", 2025)
WEEK   = env_int("WEEK", 6)

BASE      = "https://api.sleeper.app/v1"

ROOT  = pathlib.Path(__file__).resolve().parent.parent
OUT   = ROOT / "data"
CACHE = OUT / "cache"
OUT.mkdir(parents=True, exist_ok=True)
CACHE.mkdir(parents=True, exist_ok=True)

def now_iso(): return dt.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"

def get(url, params=None, default=None):
    try:
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[warn] GET {url} -> {e}")
        return default

def write_json(relpath:str, payload:Dict[str,Any]):
    p = OUT / relpath
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print("WROTE", p)

# ---------- players catalog (for names/pos/team) ----------
def get_players_catalog() -> Dict[str, Any]:
    cache = CACHE / "players_nfl.json"
    if cache.exists() and (dt.datetime.utcnow() - dt.datetime.utcfromtimestamp(cache.stat().st_mtime) < dt.timedelta(hours=12)):
        return json.loads(cache.read_text("utf-8"))
    data = get(f"{BASE}/players/nfl", default={}) or {}
    cache.write_text(json.dumps(data), encoding="utf-8")
    return data

# ---------- league scoring (to compute fantasy points) ----------
def get_league_scoring() -> Dict[str, float]:
    lg = get(f"{BASE}/league/{LEAGUE_ID}", default={}) or {}
    s  = (lg.get("scoring_settings") or {})
    defaults = dict(
        pass_yd=0.04, pass_td=4, pass_int=-1,
        rush_yd=0.1, rush_td=6,
        rec=1, rec_yd=0.1, rec_td=6,
        fumbles_lost=-2, two_pt=2
    )
    for k,v in defaults.items(): s.setdefault(k, v)
    return s

def _num(v): return float(v) if isinstance(v,(int,float)) else 0.0

def fantasy_points(stats:Dict[str,Any], sc:Dict[str,float]) -> float:
    pts = (
        _num(stats.get("pass_yd") or stats.get("pass_yds")) * sc["pass_yd"]
      + _num(stats.get("pass_td")) * sc["pass_td"]
      + _num(stats.get("pass_int")) * sc["pass_int"]
      + _num(stats.get("rush_yd") or stats.get("rush_yds")) * sc["rush_yd"]
      + _num(stats.get("rush_td")) * sc["rush_td"]
      + _num(stats.get("rec") or stats.get("receptions")) * sc["rec"]
      + _num(stats.get("rec_yd") or stats.get("rec_yds")) * sc["rec_yd"]
      + _num(stats.get("rec_td")) * sc["rec_td"]
      + _num(stats.get("fum_lost") or stats.get("fumbles_lost")) * sc["fumbles_lost"]
      + _num(stats.get("two_pt") or stats.get("two_ptm") or stats.get("two_pt_conv")) * sc["two_pt"]
    )
    return round(pts, 2)

# ---------- weekly stats fetch (normalize dict → list) ----------
def fetch_week_stats(season:int, week:int) -> List[Dict[str,Any]]:
    """
    Sleeper weekly stats endpoint returns a DICT keyed by player_id.
    Normalize to a list with 'player_id' carried into each row.
    """
    raw = get(f"{BASE}/stats/nfl/{season}/{week}", params={"season_type":"regular"}, default={}) or {}
    rows: List[Dict[str,Any]] = []
    if isinstance(raw, dict):
        for pid, stats in raw.items():
            if not isinstance(stats, dict): continue
            row = {k:v for k,v in stats.items() if isinstance(v,(int,float,str))}
            row["player_id"] = str(pid)
            rows.append(row)
    elif isinstance(raw, list):
        rows = raw
    return rows

def build_week_stats(season:int, week:int, catalog:Dict[str,Any]) -> List[Dict[str,Any]]:
    sc = get_league_scoring()
    rows = fetch_week_stats(season, week)
    out: List[Dict[str,Any]] = []
    for r in rows:
        pid   = r.get("player_id")
        meta  = catalog.get(str(pid), {}) if pid else {}
        name  = meta.get("full_name") or meta.get("name")
        team  = r.get("team") or r.get("player_team") or meta.get("team")
        pos   = meta.get("position")
        opp   = r.get("opponent") or r.get("opp")
        # build normalized row; keep numeric stats too
        row = {"player_id": str(pid), "name": name, "team": team, "pos": pos, "opp": opp}
        for k,v in r.items():
            if isinstance(v,(int,float)) and k not in row:
                row[k] = v
        # usage helpers
        row["targets"]  = _num(r.get("tgt") or r.get("targets"))
        row["rush_att"] = _num(r.get("rush_att") or r.get("rushing_att") or r.get("att_rush"))
        row["fantasy_pts"] = fantasy_points(row, sc)
        out.append(row)
    return out

def build_szn_to_date(weeks:List[int], weekly_files:List[List[Dict[str,Any]]]) -> Dict[str,Any]:
    agg = defaultdict(lambda: {"games":0,"fantasy_pts":0.0,"targets":0.0,"rush_att":0.0,"pos":None,"name":None})
    for rows in weekly_files:
        for r in rows:
            pid = r["player_id"]
            a = agg[pid]
            a["games"] += 1
            a["fantasy_pts"] += _num(r.get("fantasy_pts"))
            a["targets"] += _num(r.get("targets"))
            a["rush_att"] += _num(r.get("rush_att"))
            a["pos"] = a["pos"] or r.get("pos")
            a["name"] = a["name"] or r.get("name")
    table = []
    for pid, a in agg.items():
        g = max(1, a["games"])
        table.append({
            "player_id": pid,
            "name": a["name"],
            "pos": a["pos"],
            "games": a["games"],
            "ppg": round(a["fantasy_pts"]/g,2),
            "tgt_pg": round(a["targets"]/g,2),
            "rush_att_pg": round(a["rush_att"]/g,2)
        })
    table.sort(key=lambda x: (-x["ppg"], x["name"] or ""))
    return {"season": SEASON, "generated_at": now_iso(), "players": table}

def build_usage_shares(week_rows:List[Dict[str,Any]]) -> Dict[str,Any]:
    totals_tgt = defaultdict(float)
    totals_car = defaultdict(float)
    for r in week_rows:
        team = r.get("team")
        if not team: continue
        totals_tgt[team] += _num(r.get("targets"))
        totals_car[team] += _num(r.get("rush_att"))
    shares = []
    for r in week_rows:
        team = r.get("team")
        if not team: continue
        tgt_tot = totals_tgt[team] or 0.0
        car_tot = totals_car[team] or 0.0
        shares.append({
            "player_id": r["player_id"],
            "name": r.get("name"),
            "team": team,
            "pos": r.get("pos"),
            "targets": _num(r.get("targets")),
            "rush_att": _num(r.get("rush_att")),
            "target_share": round((_num(r.get("targets"))/tgt_tot)*100.0,1) if tgt_tot>0 else 0.0,
            "carry_share": round((_num(r.get("rush_att"))/car_tot)*100.0,1) if car_tot>0 else 0.0
        })
    return {"season": SEASON, "week": WEEK, "generated_at": now_iso(), "players": shares}

def build_sos_defense_vs_pos(history:List[List[Dict[str,Any]]]) -> Dict[str,Any]:
    allowed = defaultdict(lambda: {"pts":0.0,"games":0})
    for rows in history:
        for r in rows:
            opp = r.get("opp"); pos = r.get("pos")
            if not opp or not pos: continue
            allowed[(opp,pos)]["pts"] += _num(r.get("fantasy_pts"))
            allowed[(opp,pos)]["games"] += 1
    table = []
    for (def_team,pos), a in allowed.items():
        g = max(1, a["games"])
        table.append({"def_team":def_team,"pos":pos,"pts_allowed_pg": round(a["pts"]/g,2), "games": a["games"]})
    table.sort(key=lambda x: (x["pos"], -x["pts_allowed_pg"]))
    return {"season": SEASON, "through_week": WEEK, "generated_at": now_iso(), "defense_vs_pos": table}

def main():
    catalog = get_players_catalog()

    # this week
    this_week = build_week_stats(SEASON, WEEK, catalog)
    write_json(f"player_stats/{SEASON}/week_{WEEK:02}.json",
               {"season":SEASON,"week":WEEK,"generated_at":now_iso(),"players":this_week})

    # prior weeks (for season-to-date & SOS)
    weekly_rows = [this_week]
    for w in range(1, WEEK):
        rows = build_week_stats(SEASON, w, catalog)
        weekly_rows.append(rows)
        write_json(f"player_stats/{SEASON}/week_{w:02}.json",
                   {"season":SEASON,"week":w,"generated_at":now_iso(),"players":rows})

    # season to date
    szn = build_szn_to_date(list(range(1, WEEK+1)), weekly_rows)
    write_json(f"player_stats/{SEASON}/season_to_date.json", szn)

    # usage shares (this week)
    usage = build_usage_shares(this_week)
    write_json(f"usage/{SEASON}/week_{WEEK:02}.json", usage)

    # strength of schedule (def vs position)
    sos = build_sos_defense_vs_pos(weekly_rows)
    write_json(f"sos/{SEASON}/through_week_{WEEK:02}.json", sos)

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
