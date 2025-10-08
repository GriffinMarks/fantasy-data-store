#!/usr/bin/env python3
import os, json, pathlib, datetime as dt, requests

LEAGUE_ID = os.getenv("SLEEPER_LEAGUE_ID", "1262790170931892224")
SEASON    = int(os.getenv("SEASON", "2025"))
MAX_WEEK  = int(os.getenv("MAX_WEEK", "18"))  # scan up to

BASE = "https://api.sleeper.app/v1"
ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT  = ROOT / "data"
OUT.mkdir(parents=True, exist_ok=True)

def now_iso(): return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def get(url):
    r = requests.get(url, timeout=60); r.raise_for_status(); return r.json()

def write_json(rel, payload):
    p = OUT / rel; p.parent.mkdir(parents=True, exist_ok=True)
    with open(p,"w",encoding="utf-8") as f: json.dump(payload,f,indent=2,ensure_ascii=False)
    print("WROTE", p)

def main():
    all_moves = []
    for w in range(1, MAX_WEEK+1):
        try:
            moves = get(f"{BASE}/league/{LEAGUE_ID}/transactions/{w}") or []
            write_json(f"transactions/{SEASON}/week_{w:02}.json",
                       {"season":SEASON,"week":w,"generated_at":now_iso(),"league_id":LEAGUE_ID,"moves":moves})
            all_moves.extend([{"week":w, **m} for m in moves])
        except Exception as e:
            print("WARN", e)
    write_json(f"transactions/{SEASON}/season.json",
               {"season":SEASON,"generated_at":now_iso(),"league_id":LEAGUE_ID,"moves":all_moves})

if __name__ == "__main__":
    main()
