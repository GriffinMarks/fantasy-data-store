#!/usr/bin/env python3
import os, json, pathlib, datetime as dt, time, math
import requests
from typing import Dict, Any, List, Tuple, Set, DefaultDict
from collections import defaultdict

LEAGUE_ID = os.getenv("SLEEPER_LEAGUE_ID", "1262790170931892224")
SEASON    = int(os.getenv("SEASON", "2025"))
WEEK      = int(os.getenv("WEEK", "6"))
BASE      = "https://api.sleeper.app/v1"

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT  = ROOT / "data"
OUT.mkdir(parents=True, exist_ok=True)

def now_iso():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

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
    with open(p, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"WROTE {p}")

# ---------- Scoring ----------
def get_league_scoring() -> Dict[str, float]:
    lg = get(f"{BASE}/league/{LEAGUE_ID}", default={}) or {}
    s  = (lg.get("scoring_settings") or {})
    # sensible defaults if missing (PPR-ish)
    defaults = dict(
        pass_yd=0.04, pass_td=4, pass_int=-1,
        rush_yd=0.1, rush_td=6,
        rec=1, rec_yd=0.1, rec_td=6,
        fumbles_lost=-2, two_pt=2
    )
    for k,v in defaults.items(): s.setdefault(k, v)
    return s

def stat(p:Dict[str,Any], *keys, default=0.0) -> float:
    for k in keys:
        if k in p and isinstance(p[k], (int,float)): return float(p[k])
    return float(default)

def fantasy_points(row:Dict[str,Any], sc:Dict[str,float]) -> float:
    # Accept common Sleeper stat key variants
    pass_yds = stat(row, "pass_yd", "pass_yds")
    pass_td  = stat(row, "pass_td")
    pass_int = stat(row, "pass_int")
    rush_yds = stat(row, "rush_yd", "rush_yds")
    rush_td  = stat(row, "rush_td")
    rec      = stat(row, "rec", "receptions")
    rec_yds  = stat(row, "rec_yd", "rec_yds")
    rec_td   = stat(row, "rec_td")
    fum_lost = stat(row, "fum_lost", "fumbles_lost")
    two_pt   = stat(row, "two_pt", "two_ptm", "two_pt_conv")

    pts = (
        pass_yds * sc["pass_yd"]
        + pass_td * sc["pass_td"]
        + pass_int * sc["pass_int"]
        + rush_yds * sc["rush_yd"]
        + rush_td * sc["rush_td"]
        + rec * sc["rec"]
        + rec_yds * sc["rec_yd"]
        + rec_td * sc["rec_td"]
        + fum_lost * sc["fumbles_lost"]
        + two_pt * sc["two_pt"]
    )
    return round(pts, 2)

# ---------- Fetch weekly player stats ----------
def fetch_week_stats(season:int, week:int, pos:str) -> List[Dict[str,Any]]:
    # Sleeper weekly stats: /stats/nfl/{season}/{week}?position=RB&season_type=regular
    data = get(f"{BASE}/stats/nfl/{season}/{week}", params={"position":pos, "season_type":"regular"}, default=[])
    return data or []

def build_week_stats(season:int, week:int) -> List[Dict[str,Any]]:
    sc = get_league_scoring()
    positions = ["QB","RB","WR","TE"]
    out: List[Dict[str,Any]] = []
    for pos in positions:
        rows = fetch_week_stats(season, week, pos)
        for r in rows:
            # Normalize some usual fields if present
            team = r.get("team") or r.get("player_team")
            opp  = r.get("opponent") or r.get("opp")
            pid  = r.get("player_id") or r.get("pid") or r.get("id")
            name = r.get("player") or r.get("full_name") or r.get("name")
            row  = {"player_id": str(pid), "name": name, "team": team, "pos": pos, "opp": opp}
            row.update({k:v for k,v in r.items() if isinstance(v,(int,float))})  # keep raw numerics
            row["fantasy_pts"] = fantasy_points(row, sc)
            # usage helpers if present
            row["targets"] = stat(r, "tgt","targets")
            row["rush_att"] = stat(r, "rush_att","rushing_att","att_rush")
            out.append(row)
        time.sleep(0.15)
    return out

def build_szn_to_date(weeks:List[int], weekly_files:List[List[Dict[str,Any]]]) -> Dict[str,Any]:
    agg: DefaultDict[str, Dict[str,Any]] = defaultdict(lambda: {"games":0,"fantasy_pts":0.0,"targets":0.0,"rush_att":0.0})
    meta: Dict[str, Tuple[str,str]] = {}
    for week, rows in zip(weeks, weekly_files):
        for r in rows:
            pid = r["player_id"]
            meta.setdefault(pid, (r.get("name"), r.get("pos")))
            agg[pid]["games"] += 1
            agg[pid]["fantasy_pts"] += r.get("fantasy_pts",0.0)
            agg[pid]["targets"] += r.get("targets",0.0)
            agg[pid]["rush_att"] += r.get("rush_att",0.0)
    table = []
    for pid, a in agg.items():
        name,pos = meta.get(pid, ("",""))
        g = max(1, a["games"])
        table.append({
            "player_id": pid, "name": name, "pos": pos,
            "games": a["games"],
            "ppg": round(a["fantasy_pts"]/g,2),
            "tgt_pg": round(a["targets"]/g,2),
            "rush_att_pg": round(a["rush_att"]/g,2)
        })
    table.sort(key=lambda x: (-x["ppg"], x["name"] or ""))
    return {"season": SEASON, "generated_at": now_iso(), "players": table}

def build_usage_shares(week_rows:List[Dict[str,Any]]) -> Dict[str,Any]:
    # team totals → shares by player
    totals: DefaultDict[Tuple[str,str], Dict[str,float]] = defaultdict(lambda: {"targets":0.0,"rush_att":0.0})
    for r in week_rows:
        team = r.get("team"); pos = r.get("pos")
        totals[(team, "targets")]["targets"] += r.get("targets",0.0)
        totals[(team, "rush_att")]["rush_att"] += r.get("rush_att",0.0)
    shares = []
    for r in week_rows:
        team = r.get("team")
        tgt_tot = totals[(team,"targets")]["targets"] or 0.0
        car_tot = totals[(team,"rush_att")]["rush_att"] or 0.0
        shares.append({
            "player_id": r["player_id"],
            "name": r.get("name"),
            "team": team,
            "pos": r.get("pos"),
            "targets": r.get("targets",0.0),
            "rush_att": r.get("rush_att",0.0),
            "target_share": round((r.get("targets",0.0)/tgt_tot)*100.0,1) if tgt_tot>0 else 0.0,
            "carry_share": round((r.get("rush_att",0.0)/car_tot)*100.0,1) if car_tot>0 else 0.0
        })
    return {"season": SEASON, "week": WEEK, "generated_at": now_iso(), "players": shares}

def build_sos_defense_vs_pos(history:List[List[Dict[str,Any]]]) -> Dict[str,Any]:
    # Approx: points allowed BY defense (opp) to each position so far this year
    allowed: DefaultDict[Tuple[str,str], Dict[str,float]] = defaultdict(lambda: {"pts":0.0,"games":0})
    for rows in history:
        for r in rows:
            opp = r.get("opp")
            pos = r.get("pos")
            if not opp or not pos: continue
            allowed[(opp,pos)]["pts"] += r.get("fantasy_pts",0.0)
            allowed[(opp,pos)]["games"] += 1
    table = []
    for (def_team,pos), a in allowed.items():
        g = max(1,a["games"])
        table.append({"def_team":def_team,"pos":pos,"pts_allowed_pg": round(a["pts"]/g,2), "games": a["games"]})
    table.sort(key=lambda x: (x["pos"], -x["pts_allowed_pg"]))
    return {"season": SEASON, "through_week": WEEK, "generated_at": now_iso(), "defense_vs_pos": table}

def main():
    weeks = list(range(1, WEEK+1))
    weekly_rows: List[List[Dict[str,Any]]] = []

    # Build this week first (and write per-week file)
    this_week = build_week_stats(SEASON, WEEK)
    weekly_rows.append(this_week)
    write_json(f"player_stats/{SEASON}/week_{WEEK:02}.json", {"season":SEASON,"week":WEEK,"generated_at":now_iso(),"players":this_week})

    # Also fetch earlier weeks (for season-to-date + SOS)
    for w in range(1, WEEK):
        rows = build_week_stats(SEASON, w)
        weekly_rows.append(rows)
        # (optional) write each prior week too:
        write_json(f"player_stats/{SEASON}/week_{w:02}.json", {"season":SEASON,"week":w,"generated_at":now_iso(),"players":rows})

    # Season to date
    szn = build_szn_to_date(weeks=list(range(1, WEEK+1)), weekly_files=weekly_rows[::-1])  # include all
    write_json(f"player_stats/{SEASON}/season_to_date.json", szn)

    # Usage (from this week)
    usage = build_usage_shares(this_week)
    write_json(f"usage/{SEASON}/week_{WEEK:02}.json", usage)

    # Strength of schedule (def vs pos to date)
    sos = build_sos_defense_vs_pos(weekly_rows)
    write_json(f"sos/{SEASON}/through_week_{WEEK:02}.json", sos)

if __name__ == "__main__":
    main()
