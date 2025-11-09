from __future__ import annotations
import os
import re
import json
from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from serpapi import GoogleSearch

SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY", "").strip()

# -----------------------------
# Data models
# -----------------------------
@dataclass
class PlacePick:
    title: str
    data_id: str
    place_id: Optional[str]
    data_cid: Optional[str]
    address: Optional[str]
    rating: Optional[float]
    reviews: Optional[int]
    position: Optional[int]

# -----------------------------
# Cache helper (JSON file)
# -----------------------------
class Cache:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            try:
                self._data = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                self._data = {}
        else:
            self._data = {}

    def _key(self, pub_name: str, location: str) -> str:
        return f"{_normalize(pub_name)}|{_normalize(location)}"

    def get(self, pub_name: str, location: str) -> Optional[Dict[str, Any]]:
        return self._data.get(self._key(pub_name, location))

    def put(self, pub_name: str, location: str, obj: Dict[str, Any]) -> None:
        self._data[self._key(pub_name, location)] = obj
        self.path.write_text(json.dumps(self._data, indent=2, ensure_ascii=False), encoding="utf-8")

# -----------------------------
# Utils
# -----------------------------
def _require_env_key() -> None:
    if not SERPAPI_API_KEY:
        raise RuntimeError("Missing SERPAPI_API_KEY. Put it in your environment or .env file.")

def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip().lower()

def _to_pick(d: Dict[str, Any]) -> PlacePick:
    return PlacePick(
        title=d.get("title") or "",
        data_id=str(d.get("data_id") or ""),
        place_id=d.get("place_id"),
        data_cid=d.get("data_cid"),
        address=d.get("address"),
        rating=d.get("rating"),
        reviews=d.get("reviews"),
        position=d.get("position"),
    )

# -----------------------------
# Core logic
# -----------------------------
def resolve_top_data_id(pub_name: str,
                        location: str,
                        *,
                        lang: str = "en",
                        ll: Optional[str] = None,
                        google_domain: str = "google.co.uk") -> Dict[str, Any]:
    """
    Resolve the top (position=1) result via SerpAPI client and return
    a payload with success + pick + candidates + raw metadata.
    """
    _require_env_key()

    name_n = _normalize(pub_name)
    loc_n  = _normalize(location)
    if not name_n or not loc_n:
        raise ValueError("Both pub_name and location are required and must be non-empty.")

    query = f"{pub_name} {location}"

    params: Dict[str, Any] = {
        "engine": "google_maps",
        "type": "search",
        "q": query,
        "hl": lang,
        "google_domain": google_domain,
        "api_key": SERPAPI_API_KEY,
    }
    if ll:
        params["ll"] = ll  # e.g. "@52.598,-2.166,14z"

    payload = GoogleSearch(params).get_dict()

    local_results: List[Dict[str, Any]] = payload.get("local_results") or []
    place_results: Dict[str, Any] = payload.get("place_results") or {}

    rows: List[Dict[str, Any]] = []
    if isinstance(local_results, list) and local_results:
        rows = local_results
    elif isinstance(place_results, dict) and place_results:
        rows = [place_results]

    candidates: List[PlacePick] = [_to_pick(d) for d in rows if isinstance(d, dict)]

    if not candidates:
        return {
            "success": False,
            "reason": "No local_results/place_results returned by SerpAPI.",
            "raw_search_parameters": payload.get("search_parameters"),
            "raw_metadata": payload.get("search_metadata"),
            "candidates": [],
        }

    top = sorted(candidates, key=lambda x: (x.position if x.position is not None else 9999))[0]

    title_ok = all(tok in _normalize(top.title) for tok in name_n.split())
    address_ok = (not top.address) or any(tok in _normalize(top.address) for tok in loc_n.split())

    if not top.data_id:
        return {
            "success": False,
            "reason": "Top result missing data_id.",
            "pick": top.__dict__,
            "raw_search_parameters": payload.get("search_parameters"),
            "raw_metadata": payload.get("search_metadata"),
            "candidates": [c.__dict__ for c in candidates[:5]],
        }

    if not (title_ok and address_ok):
        return {
            "success": False,
            "reason": "Top result failed light sanity check (title/location).",
            "pick": top.__dict__,
            "raw_search_parameters": payload.get("search_parameters"),
            "raw_metadata": payload.get("search_metadata"),
            "candidates": [c.__dict__ for c in candidates[:5]],
        }

    return {
        "success": True,
        "pick": top.__dict__,
        "raw_search_parameters": payload.get("search_parameters"),
        "raw_metadata": payload.get("search_metadata"),
        "candidates": [c.__dict__ for c in candidates[:5]],
    }

def compact_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert the verbose payload to a compact object for downstream steps.
    """
    if not payload.get("success"):
        return {"success": False, "reason": payload.get("reason", "Unknown")}
    p = payload["pick"]
    return {
        "success": True,
        "data_id": p["data_id"],
        "place_id": p.get("place_id"),
        "title": p.get("title"),
        "address": p.get("address"),
    }

# -----------------------------
# CLI
# -----------------------------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Resolve SerpAPI Google Maps data_id for a pub (with cache).")
    ap.add_argument("--name", required=True, help="Pub name (e.g., 'The Two Greens')")
    ap.add_argument("--location", required=True, help="Town/area/city/borough (e.g., 'Tettenhall')")
    ap.add_argument("--lang", default="en", help="Language (default: en)")
    ap.add_argument("--ll", default=None, help="Geo-bias like '@52.598,-2.166,14z' (optional)")
    ap.add_argument("--google-domain", default="google.co.uk", help="Default: google.co.uk")
    ap.add_argument("--cache-path", default=str(Path(".cache") / "pubreview_resolutions.json"),
                    help="Path to JSON cache file.")
    ap.add_argument("--confirm", action="store_true", help="Print a short confirmation (title + address).")
    ap.add_argument("--debug", action="store_true", help="Print full verbose payload instead of compact output.")
    args = ap.parse_args()

    cache = Cache(Path(args.cache_path))

    # 1) Try cache first
    cached = cache.get(args.name, args.location)
    if cached:
        result = cached  # already a compact object
    else:
        # 2) Resolve via SerpAPI
        payload = resolve_top_data_id(
            args.name,
            args.location,
            lang=args.lang,
            ll=args.ll,
            google_domain=args.google_domain,
        )
        # 3) Compact object
        result = compact_from_payload(payload)
        # 4) Save to cache if success
        if result.get("success"):
            cache.put(args.name, args.location, result)

    # Optional human confirmation
    if args.confirm and result.get("success"):
        print(f"[confirm] {result['title']} — {result['address']}")

    # Output
    if args.debug:
        # If debug, recompute verbose payload from live (not from cache)
        if cached:
            # If cached, we don’t have verbose payload; print compact + note
            print(json.dumps({"note": "cached_compact_only", "compact": cached}, indent=2, ensure_ascii=False))
        else:
            # We still have the verbose payload in this code path (could be stored if desired)
            payload = resolve_top_data_id(args.name, args.location,
                                          lang=args.lang, ll=args.ll, google_domain=args.google_domain)
            print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))
