# phase2b_summarize.py
from __future__ import annotations
import os, json
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
import datetime as dt

from dotenv import load_dotenv
from serpapi import GoogleSearch
from openai import OpenAI

load_dotenv()
SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY", "").strip()
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY", "").strip()

# ---------------- Models ----------------
@dataclass
class Review:
    review_id: str
    rating: float
    date: str
    relative_time: str
    text: str
    author: Optional[str]
    source: str = "google_maps_reviews"

# ------------- Fetch (SerpAPI) -------------
def _require_keys(fetch_needed: bool = True):
    if fetch_needed and not SERPAPI_API_KEY:
        raise RuntimeError("Missing SERPAPI_API_KEY")
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing OPENAI_API_KEY")

def _page(data_id: str, next_page_token: Optional[str] = None, *, lang="en", sort_by="newest") -> Dict[str, Any]:
    params = {"engine":"google_maps_reviews","data_id":data_id,"hl":lang,"api_key":SERPAPI_API_KEY,"sort_by":sort_by}
    if next_page_token:
        params["next_page_token"] = next_page_token
    return GoogleSearch(params).get_dict()

def _next_token(payload: Dict[str, Any]) -> Optional[str]:
    if payload.get("next_page_token"): return payload["next_page_token"]
    pag = payload.get("serpapi_pagination") or {}
    return pag.get("next_page_token") if isinstance(pag, dict) else None

def fetch_all_reviews(data_id: str, *, max_results=500, lang="en", sort_by="newest") -> Dict[str, Any]:
    all_reviews: List[Dict[str, Any]] = []
    token: Optional[str] = None
    while True:
        payload = _page(data_id, token, lang=lang, sort_by=sort_by)
        reviews = payload.get("reviews") or payload.get("reviews_results") or []
        all_reviews.extend(reviews)
        if len(all_reviews) >= max_results: break
        token = _next_token(payload)
        if not token: break
    return {"data_id": data_id, "reviews": all_reviews}

# ------------- Normalize & window -------------
def _to_iso(date_like: Any) -> Optional[str]:
    try:
        if isinstance(date_like, (int, float)):
            return dt.datetime.utcfromtimestamp(date_like).date().isoformat()
        if isinstance(date_like, str):
            s = date_like.replace(" UTC","").replace("Z","")
            try:
                return dt.datetime.fromisoformat(s).date().isoformat()
            except Exception:
                return dt.datetime.strptime(s[:10], "%Y-%m-%d").date().isoformat()
    except Exception:
        return None
    return None

def normalize_reviews(raw: Dict[str, Any]) -> List[Review]:
    items = raw.get("reviews") or []
    out: List[Review] = []
    for r in items:
        rid = str(r.get("review_id") or r.get("id") or "")
        rating = float(r.get("rating") or 0)
        date_iso = (
            _to_iso(r.get("iso_date")) or
            _to_iso(r.get("iso_date_of_last_edit")) or
            _to_iso(r.get("date")) or
            _to_iso(r.get("time")) or
            _to_iso(r.get("published_at")) or
            ""
        )
        rel = r.get("relative_time_description") or r.get("relative_time") or ""
        text = (r.get("snippet") or r.get("text") or r.get("content") or "").strip()
        author = r.get("author_name") or r.get("author")
        if not author:
            prof = r.get("user") or r.get("profile") or {}
            if isinstance(prof, dict):
                author = prof.get("name")
        out.append(Review(rid, rating, date_iso, rel, text, author))
    return out

def filter_window(reviews: List[Review], window: str) -> List[Review]:
    if window == "all": return reviews
    days = 90 if window == "last90" else 180
    cutoff = dt.date.today() - dt.timedelta(days=days)
    kept: List[Review] = []
    for r in reviews:
        if not r.date: 
            continue  # exclude undated from windowed views
        try:
            if dt.date.fromisoformat(r.date) >= cutoff:
                kept.append(r)
        except Exception:
            pass
    return kept

# ------------- Light analytics -------------
def sentiment_bucket(rating: float) -> str:
    if rating >= 4.0: return "positive"
    if rating <= 2.0: return "negative"
    return "neutral"

THEME_KEYWORDS = {
    "Staff & Service": ["staff","service","waiter","waitress","server","friendly","helpful","polite","attentive","manager","team","bar staff","foh"],
    "Food Quality / Execution": ["food","meal","chicken","lasagne","steak","grill","undercooked","overcooked","cold","microwave","tasty","portion","menu"],
    "Speed / Wait Time": ["quick","slow","wait","waiting","timely","fast","delay"],
    "Value & Deals": ["value","price","cheap","deal","2-for","offer","expensive"],
    "Environment": ["atmosphere","vibe","family","kids","clean","dirty","noise","sport","tv","screen","cozy","cosy"],
    "Events": ["quiz","karaoke","event","live","host"],
}

def _kw_hits(text: str, words: List[str]) -> bool:
    t = text.lower()
    return any(w in t for w in words)

def theme_breakdown(reviews: List[Review]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str,int]] = {}
    for theme, words in THEME_KEYWORDS.items():
        pos=neu=neg=0
        for r in reviews:
            if _kw_hits(r.text, words):
                b = sentiment_bucket(r.rating)
                if b == "positive": pos+=1
                elif b == "negative": neg+=1
                else: neu+=1
        out[theme] = {"positive":pos,"neutral":neu,"negative":neg}
    return out

def basic_metrics(reviews: List[Review]) -> Dict[str, Any]:
    if not reviews:
        return {"count":0,"avg_rating":None,"pos":0,"neu":0,"neg":0}
    ratings = [r.rating for r in reviews if isinstance(r.rating, (int,float))]
    buckets = [sentiment_bucket(r.rating) for r in reviews]
    return {
        "count": len(reviews),
        "avg_rating": round(sum(ratings)/len(ratings), 2) if ratings else None,
        "pos": buckets.count("positive"),
        "neu": buckets.count("neutral"),
        "neg": buckets.count("negative"),
    }

def sample_quotes(reviews: List[Review], n=6) -> List[Dict[str, Any]]:
    pos = [r for r in reviews if sentiment_bucket(r.rating)=="positive"]
    neg = [r for r in reviews if sentiment_bucket(r.rating)=="negative"]
    mix = (pos[: n//2]) + (neg[: n - n//2])
    return [{"text": r.text[:300], "author": r.author or "Guest", "rating": r.rating, "date": r.date} for r in mix]

def slice_last90(reviews: List[Review]) -> List[Review]:
    cutoff = dt.date.today() - dt.timedelta(days=90)
    out = []
    for r in reviews:
        if not r.date:
            continue
        try:
            if dt.date.fromisoformat(r.date) >= cutoff:
                out.append(r)
        except Exception:
            pass
    return out

def avg_rating(reviews: List[Review]) -> Optional[float]:
    vals = [r.rating for r in reviews if isinstance(r.rating, (int, float))]
    return round(sum(vals)/len(vals), 2) if vals else None

# ------------- Style & LLM -------------
DEFAULT_STYLE_TEXT = """# Pub Pulse Summary — [PUB_NAME]

## Executive Snapshot
- Timeframe: [WINDOW]. Total reviews in window, average rating, and sentiment split (pos/neu/neg).
- One-paragraph overview of guest sentiment and volume.

## Trends & Performance
- Compare last 90 days vs. all-time (avg rating, review volume).
- Call out direction of travel (improving, stable, declining).

## What Guests Love
- Bullet points of consistent positives with short supporting quotes.
- Highlight standout staff by name where possible.

## What Hurts
- Bullet points of recurring negatives with short supporting quotes.
- Focus on food execution, speed, and service consistency.

## Theme Breakdown
- Staff & Service — % pos / % neu / % neg, brief note.
- Food Quality / Execution — % pos / % neu / % neg, brief note.
- Speed / Wait Time — % pos / % neu / % neg, brief note.
- Value & Deals — % pos / % neu / % neg, brief note.
- Environment — % pos / % neu / % neg, brief note.
- Events — % pos / % neu / % neg, brief note.

## Moments That Matter (Do more of this)
- High-impact positives that drive repeat visits (events, deals, staff moments).

## Issue Log (ranked by impact)
- Each item: **Issue — Severity (1–5) | Suggested Owner | Action** (1–2 lines).
- Keep focused on operational fixes that move the needle.

## Staff Kudos Leaderboard
- Staff with the most positive mentions + example quotes.

## Events & Formats That Win
- What’s driving engagement (e.g., quiz nights, live sport), with examples.

## Improvement Priority List (next 30–60 days)
- Top 3 fixes with likely impact and quick actions.

## One Thing To Fix
- The single most impactful improvement to implement now.
"""

def load_style(path: Optional[str]) -> str:
    """
    Load the style guide for the Pub Pulse summary.
    - If a path is given and exists, use it.
    - If no path, try 'pubpulse_style.md' in CWD.
    - Otherwise, fall back to DEFAULT_STYLE_TEXT.
    Prints which option was chosen.
    """
    if path:
        p = Path(path)
        if p.exists():
            print(f"[info] Using style file: {p.resolve()}")
            return p.read_text(encoding="utf-8")
        else:
            print(f"[warn] Style file not found at {path}, falling back to DEFAULT_STYLE_TEXT")
            return DEFAULT_STYLE_TEXT

    local = Path("pubpulse_style.md")
    if local.exists():
        print(f"[info] Using local style file: {local.resolve()}")
        return local.read_text(encoding="utf-8")

    print("[warn] No style file found, using built-in default style")
    return DEFAULT_STYLE_TEXT

def make_llm_summary(pub_title: str, date_window: str, facts: Dict[str, Any], style_text: str, model="gpt-4o-mini") -> str:
    client = OpenAI(api_key=OPENAI_API_KEY)
    messages = [
        {
            "role":"system",
            "content": (
                "You are a precise analyst. Follow the style guide exactly. "
                "Use only provided facts/quotes. "
                "If window_is_all is true or narrative_hints.suppress_volume_trend is true, "
                "do NOT claim review volume is 'steady' or 'rising'; only compare last90 vs all-time if last90 exists."
            )
        },
        {"role":"user","content":"Style guide:\n" + style_text},
        {"role":"user","content":"Summarize this JSON into a Pub Pulse markdown:\n" + json.dumps({
            "pub_title": pub_title,
            "window": date_window,
            "facts": facts
        }, ensure_ascii=False)}
    ]
    resp = client.chat.completions.create(model=model, messages=messages, temperature=0.2)
    return resp.choices[0].message.content


# ------------- CLI -------------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Phase 2b: summarize normalized reviews into Pub Pulse (Markdown + JSON).")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--data-id", help="Google Maps data_id to fetch now.")
    src.add_argument("--from-json", help="Path to raw reviews JSON previously saved (envelope or list).")
    ap.add_argument("--pub-title", default="(Pub Name)", help="Shown in the summary header.")
    ap.add_argument("--window", choices=["all","last90","last180"], default="last90")
    ap.add_argument("--sort", choices=["newest","rating","most_relevant"], default="newest")
    ap.add_argument("--max", type=int, default=500)
    ap.add_argument("--style-file", help="Path to your style file (*.md/*.txt). If omitted, tries 'pubpulse_style.md'.")
    ap.add_argument("--out-md", default="pub_pulse.md")
    ap.add_argument("--out-json", default="pub_pulse_facts.json")
    args = ap.parse_args()

    # Determine if we need SerpAPI key (only when fetching)
    _require_keys(fetch_needed=bool(args.data_id))

    # 1) Load / fetch
    if args.from_json:
        raw_text = Path(args.from_json).read_text(encoding="utf-8")
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid JSON file: {args.from_json}") from e
        raw = parsed if isinstance(parsed, dict) else {"reviews": parsed}
    else:
        raw = fetch_all_reviews(args.data_id, max_results=args.max, sort_by=args.sort)

    # 2) Normalize + window
    reviews = normalize_reviews(raw)
    reviews_win = filter_window(reviews, args.window)
    last90 = slice_last90(reviews)  # for trend comparison

       # 3) Build facts for the LLM
    last90 = slice_last90(reviews)
    metrics_all = basic_metrics(reviews)
    metrics_win = basic_metrics(reviews_win)
    metrics_last90 = basic_metrics(last90)

    facts = {
        "window": args.window,
        "window_is_all": (args.window == "all"),
        "total_reviews_all_time": metrics_all["count"],
        "reviews_in_window": metrics_win["count"],
        "avg_rating_in_window": metrics_win["avg_rating"],
        "last90": {
            "count": metrics_last90["count"],
            "avg_rating": metrics_last90["avg_rating"],
        },
        "all_time": {
            "count": metrics_all["count"],
            "avg_rating": metrics_all["avg_rating"],
        },
        "sentiment_counts_window": {
            "positive": metrics_win["pos"],
            "neutral": metrics_win["neu"],
            "negative": metrics_win["neg"],
        },
        "themes_window": theme_breakdown(reviews_win),
        "quotes": sample_quotes(reviews_win, n=6),
        # narrative hints for the LLM
        "narrative_hints": {
            "suppress_volume_trend": (args.window == "all"),
            "prefer_trend_statement": (metrics_last90["count"] > 0 and args.window != "last90"),
        },
    }

    # Log data source
    if args.from_json:
        print(f"[info] Using reviews from file: {Path(args.from_json).resolve()}")
    else:
        print(f"[info] Fetched reviews via SerpAPI for data_id: {args.data_id}")

    # 4) LLM summary (Markdown)
    style_text = load_style(args.style_file)
    md = make_llm_summary(args.pub_title, args.window, facts, style_text)

    # 5) Output
    Path(args.out_md).write_text(md, encoding="utf-8")
    Path(args.out_json).write_text(json.dumps(facts, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[done] Wrote: {args.out_md} and {args.out_json}  (reviews in window: {metrics_win['count']})")
