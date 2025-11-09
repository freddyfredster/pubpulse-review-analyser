from __future__ import annotations
import os, time, json, datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from serpapi import GoogleSearch

load_dotenv()
SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY", "").strip()

# ---------------- Models ----------------
@dataclass
class Review:
    review_id: str
    rating: float
    date: str            # ISO YYYY-MM-DD when possible
    relative_time: str   # e.g. "2 weeks ago"
    text: str
    author: Optional[str]
    source: str = "google_maps_reviews"

# ------------- Fetch core ---------------
def _require_key():
    if not SERPAPI_API_KEY:
        raise RuntimeError("Missing SERPAPI_API_KEY")

def _page(
    data_id: str,
    next_page_token: Optional[str] = None,
    *,
    lang: str = "en",
    sort_by: str = "newest"               # "newest" | "rating" | "most_relevant"
) -> Dict[str, Any]:
    params = {
        "engine": "google_maps_reviews",
        "data_id": data_id,
        "hl": lang,
        "api_key": SERPAPI_API_KEY,
        "sort_by": sort_by,
    }
    if next_page_token:
        params["next_page_token"] = next_page_token
    return GoogleSearch(params).get_dict()

def _next_token(payload: Dict[str, Any]) -> Optional[str]:
    if token := payload.get("next_page_token"):
        return token
    pag = payload.get("serpapi_pagination") or {}
    return pag.get("next_page_token") if isinstance(pag, dict) else None

def fetch_all_reviews(
    data_id: str,
    *,
    max_results: int = 500,
    lang: str = "en",
    sort_by: str = "newest"
) -> Dict[str, Any]:
    """
    Paginate reviews using SerpAPI (official client), honoring sort server-side.
    Returns a raw envelope with reviews and minimal metadata.
    """
    _require_key()
    all_reviews: List[Dict[str, Any]] = []
    token: Optional[str] = None

    while True:
        payload = _page(data_id, token, lang=lang, sort_by=sort_by)
        reviews = payload.get("reviews") or payload.get("reviews_results") or []
        all_reviews.extend(reviews)

        if len(all_reviews) >= max_results:
            break

        token = _next_token(payload)
        if not token:
            break

        # allow next_page_token to become valid
        time.sleep(2.0)

    return {
        "source": "serpapi/google_maps_reviews",
        "data_id": data_id,
        "count": min(len(all_reviews), max_results),
        "reviews": all_reviews[:max_results],
        "meta": {
            "fetched_at": dt.datetime.utcnow().isoformat() + "Z",
            "sort_by": sort_by,
        },
    }

# --------- Normalize utilities ----------
def _to_iso(d: Any) -> Optional[str]:
    try:
        if isinstance(d, (int, float)):
            return dt.datetime.utcfromtimestamp(d).date().isoformat()
        if isinstance(d, str):
            s = d.replace(" UTC", "").replace("Z", "")
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

        # robust date handling
        date_iso = (
            _to_iso(r.get("iso_date"))
            or _to_iso(r.get("iso_date_of_last_edit"))
            or _to_iso(r.get("date"))
            or _to_iso(r.get("time"))
            or _to_iso(r.get("published_at"))
            or ""
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

def filter_window(reviews: List[Review], window: str = "last90") -> List[Review]:
    """
    Client-side time window: "all", "last90", or "last180".
    """
    if window == "all":
        return reviews
    today = dt.date.today()
    cutoff = today - dt.timedelta(days=90 if window == "last90" else 180)
    kept: List[Review] = []
    for r in reviews:
        if not r.date:
            continue
        try:
            if dt.date.fromisoformat(r.date) >= cutoff:
                kept.append(r)
        except Exception:
            pass
    return kept

# --------------- CLI --------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Phase 2: fetch & normalize Google reviews by data_id (SerpAPI client).")
    parser.add_argument("--data-id", required=True, help="Google Maps data_id (e.g., '0x...:0x...').")
    parser.add_argument("--max", type=int, default=400, help="Max reviews to fetch.")
    parser.add_argument("--lang", default="en", help="Language for results (hl).")
    parser.add_argument("--sort", default="newest", choices=["newest","rating","most_relevant"], help="Sort order.")
    parser.add_argument("--window", choices=["all","last90","last180"], default="all",
                        help="Client-side date window filter.")
    parser.add_argument("--preview", type=int, default=0,
                        help="If >0, print this many normalized reviews for quick inspection.")
    args = parser.parse_args()

    raw = fetch_all_reviews(
        args.data_id,
        max_results=args.max,
        lang=args.lang,
        sort_by=args.sort
    )
    norm = normalize_reviews(raw)
    if args.window != "all":
        norm = filter_window(norm, args.window)

    summary = {
        "data_id": raw["data_id"],
        "fetched_count": raw["count"],
        "normalized_count": len(norm),
        "window": args.window,
        "sort_by": raw["meta"]["sort_by"]
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))

    if args.preview > 0:
        sample = [{
            "review_id": r.review_id,
            "rating": r.rating,
            "date": r.date,
            "relative_time": r.relative_time,
            "author": r.author,
            "text": r.text[:200]  # trim for console
        } for r in norm[:args.preview]]
        print("\nPREVIEW:")
        print(json.dumps(sample, indent=2, ensure_ascii=False))
