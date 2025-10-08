#!/usr/bin/env python3
import os, json, pathlib, datetime as dt, feedparser

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT  = ROOT / "data"
OUT.mkdir(parents=True, exist_ok=True)

# You can edit this list anytime:
FEEDS = (os.getenv("NEWS_FEEDS") or
 "https://www.espn.com/espn/rss/nfl/news,"
 "https://www.rotowire.com/rss/football.php").split(",")

def now_iso(): return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def fetch_feed(url:str, limit:int=50):
    d = feedparser.parse(url)
    items = []
    for e in d.entries[:limit]:
        items.append({
            "title": getattr(e,"title",""),
            "link": getattr(e,"link",""),
            "published": getattr(e,"published",""),
            "summary": getattr(e,"summary",""),
            "source": url
        })
    return items

def main():
    articles=[]
    for u in FEEDS:
        u=u.strip()
        if not u: continue
        try:
            articles.extend(fetch_feed(u))
        except Exception as e:
            print("WARN feed", u, e)
    payload={"generated_at": now_iso(),"count": len(articles),"articles": articles}
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"news/{dt.datetime.utcnow():%Y-%m-%d}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print("WROTE", path)

if __name__ == "__main__":
    main()
