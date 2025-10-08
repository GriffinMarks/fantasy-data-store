#!/usr/bin/env python3
import json, pathlib, datetime as dt, os

SEASON = int(os.getenv("SEASON","2025"))
ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT  = ROOT / "data"

def load_json(rel):
    p = OUT / rel
    if not p.exists(): return {}
    return json.loads(p.read_text(encoding="utf-8"))

def zscore(vals, v):
    if not vals: return 0.0
    m = sum(vals)/len(vals)
    sd = (sum((x-m)**2 for x in vals)/max(1,len(vals)-1))**0.5 or 1.0
    return (v-m)/sd

def main():
    # needs stats built first
    s2d = load_json(f"player_stats/{SEASON}/season_to_date.json") or {}
    players = s2d.get("players", [])

    # position scarcity weights (tweakable)
    pos_pool = {}
    for p in players:
        pos_pool.setdefault(p["pos"], []).append(p["ppg"])

    values=[]
    for p in players:
        pos = p["pos"]; ppg = p["ppg"]; tgt = p.get("tgt_pg",0); car = p.get("rush_att_pg",0)
        z_ppg = zscore(pos_pool.get(pos,[]), ppg)
        score = 50 + 15*z_ppg + 2*tgt + 1.5*car   # simple transparent blend
        values.append({"player_id":p["player_id"],"name":p["name"],"pos":pos,"ppg":ppg,"value":round(score,1)})

    # rank within position & overall
    values.sort(key=lambda x: (-x["value"], x["pos"], x["name"]))
    for i,v in enumerate(values,1): v["overall_rank"]=i

    # by position
    by_pos={}
    for v in values:
        by_pos.setdefault(v["pos"], []).append(v)
    for pos in by_pos:
        for i,v in enumerate(by_pos[pos],1):
            v["pos_rank"]=i

    payload={"season":SEASON,"generated_at":dt.datetime.utcnow().isoformat()+"Z","values":values}
    p = OUT / f"value/{SEASON}/{dt.datetime.utcnow():%Y-%m-%d}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print("WROTE", p)

if __name__=="__main__":
    main()
