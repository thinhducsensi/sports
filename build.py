import hashlib
import html as html_lib
import json
import math
import os
import re
import sys
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from datetime import datetime, time, timedelta
from pathlib import Path
from time import monotonic, sleep
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

SPORT_API = "https://sport-stream-resolver.viet-ng228.workers.dev/sport.json"
TIME_ZONE = ZoneInfo("Asia/Ho_Chi_Minh")
RECENT_WINDOW_MS = 135 * 60 * 1000
VERIFY_LIVE_WINDOW_MS = 12 * 60 * 60 * 1000
# The feed's explicit live status has no duration cap. Unlabelled older
# events must not acquire a live badge solely because a media URL responds.
MAX_WORKERS = 24
RESOLVER_TIMEOUT = 5.0
USER_AGENT = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Mobile Safari/537.36"

TERMINAL_STATUSES = {
    "ft", "finished", "finish", "ended", "end", "completed", "complete",
    "cancelled", "canceled", "postponed", "abandoned", "ket thuc", "huy",
    "pend", "bi hoan",
}
LIVE_STATUSES = {
    "live", "playing", "inplay", "in play", "1h", "2h", "ht", "halftime",
    "ongoing", "running",
}
KICKOFF_FIELDS = (
    "kickoff", "time_start", "start_time", "startTime", "start_timestamp",
    "startTimestamp", "timestamp",
)
SPORT_ICON = {
    "football": "⚽", "soccer": "⚽", "futsal": "⚽", "basketball": "🏀",
    "volleyball": "🏐", "tennis": "🎾", "badminton": "🏸", "table tennis": "🏓",
    "billiards": "🎱", "billiard": "🎱", "snooker": "🎱", "pool": "🎱",
    "baseball": "⚾", "hockey": "🏒", "handball": "🤾", "rugby": "🏉",
    "cricket": "🏏", "golf": "⛳", "boxing": "🥊", "mma": "🥊",
    "motorsport": "🏁", "racing": "🏁", "esports": "🎮",
}
GENERIC_STREAM_WORDS = {
    "sport", "sports", "sport stream", "stream", "source", "server", "sv", "backup",
    "main", "primary", "mirror", "default", "auto", "link", "channel", "cdn",
}
FORMAT_ORDER = {"HLS": 0, "FLV": 1, "TS": 2, "DASH": 3, "MP4": 4}

CHUOICHIEN_API_BASES = (
    "https://api-v2.chuoichientv.com",
    "https://api-v2.chuoichientv.net",
)

DIRECT_FEEDS = {
    "chuoichien": {
        "url": "https://api-v2.chuoichientv.net/v2/matches?page=1&limit=100&type=blv",
        "kind": "chuoichien",
        "headers": {"User-Agent": "Mozilla/5.0"},
    },
    "gavang33": {
        "url": "https://gavangtv-api.adviceme.io/api/v1/matches",
        "kind": "gavang33",
        "headers": {"User-Agent": USER_AGENT, "Referer": "https://gavang33.co/"},
    },
}

PROVIDER_ALIASES = {
    "cola": "colatv",
    "colatv": "colatv",
    "chuoi chien": "chuoichien",
    "chuoichien": "chuoichien",
    "ga vang 33": "gavang33",
    "gavang33": "gavang33",
}
KNOWN_PROVIDER_IDS = {
    "chuoichien", "chuoi chien", "colatv", "cola", "gavang33", "ga vang 33",
    "giovang", "gio vang", "phalang", "phalangtv", "pha lang", "pha lang tv",
    "xoilacxth", "xoilac", "xoi lac", "xoi lac xth", "sport", "sports",
    "sportstream", "sport stream",
}


def scalar_text(value):
    if value is None or isinstance(value, (dict, list, tuple, set, bool)):
        return ""
    return str(value).strip()


def first_text(*values):
    for value in values:
        text = scalar_text(value)
        if text:
            return text
    return ""


def normalize_text(value):
    text = scalar_text(value).lower().replace("đ", "d").replace("_", " ").replace("-", " ")
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def safe_title_text(value):
    text = scalar_text(value).replace("\r", " ").replace("\n", " ")
    for mark in (",", "，", "︐", "︑", "﹐", "،", "、"):
        text = text.replace(mark, " / ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def with_cache_buster(url):
    parts = urlsplit(str(url))
    query = parse_qsl(parts.query, keep_blank_values=True)
    query.append(("_fresh", str(int(datetime.now().timestamp() * 1000))))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def fetch_json(url, timeout=8, retries=1, cache_bust=False, report_failure=False):
    target = with_cache_buster(url) if cache_bust else str(url)
    headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
        "Cache-Control": "no-cache, no-store, max-age=0",
        "Pragma": "no-cache",
    }
    for attempt in range(max(1, retries)):
        try:
            request = Request(target, headers=headers)
            with urlopen(request, timeout=timeout) as response:
                if not 200 <= getattr(response, "status", 200) < 300:
                    return None
                return json.loads(response.read().decode("utf-8-sig"))
        except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            if report_failure and attempt + 1 >= retries:
                fields = dict(parse_qsl(urlsplit(url).query))
                provider = fields.get("provider", "unknown")
                event_id = fields.get("id", "")
                kind = f"HTTP {exc.code}" if isinstance(exc, HTTPError) else type(exc).__name__
                print(f"[resolver-http] {provider}/{event_id}: {kind}", file=sys.stderr)
            if attempt + 1 < retries:
                sleep(1.0 + attempt)
    return None


def fetch_json_custom(url, headers=None, timeout=6, cache_bust=True):
    req_headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": USER_AGENT,
        "Cache-Control": "no-cache, no-store, max-age=0",
        "Pragma": "no-cache",
    }
    if isinstance(headers, dict):
        req_headers.update(headers)
    try:
        request = Request(with_cache_buster(url) if cache_bust else str(url), headers=req_headers)
        with urlopen(request, timeout=timeout) as response:
            if not 200 <= getattr(response, "status", 200) < 300:
                return None
            return json.loads(response.read().decode("utf-8-sig"))
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError, UnicodeDecodeError):
        # Some APIs sign the exact query string and reject an extra parameter.
        # The second request still carries no-cache headers and reads the
        # server's current response; it never reuses a saved media URL.
        return fetch_json_custom(url, headers, timeout, False) if cache_bust else None


def fetch_text(url, headers=None, timeout=5):
    req_headers = {
        "Accept": "text/html,application/xhtml+xml,application/json,text/plain,*/*",
        "User-Agent": USER_AGENT,
        "Cache-Control": "no-cache, no-store, max-age=0",
        "Pragma": "no-cache",
    }
    if isinstance(headers, dict):
        req_headers.update(headers)
    try:
        request = Request(str(url), headers=req_headers)
        with urlopen(request, timeout=timeout) as response:
            if not 200 <= getattr(response, "status", 200) < 300:
                return ""
            return response.read().decode("utf-8", errors="replace")
    except (HTTPError, URLError, TimeoutError, OSError, UnicodeDecodeError):
        return ""



def _nested_obj(obj, key):
    value = obj.get(key) if isinstance(obj, dict) else None
    return value if isinstance(value, dict) else {}


def _array_value(obj, *keys):
    if not isinstance(obj, dict):
        return []
    for key in keys:
        value = obj.get(key)
        if isinstance(value, list):
            return value
    return []


def _chuoi_rows(data):
    rows = _iter_candidate_matches(data)
    return [row for row in rows if isinstance(row, dict)]


def _chuoi_commentators(row):
    people = []
    seen = set()
    for blv in _array_value(row, "blvs", "commentators"):
        if not isinstance(blv, dict):
            continue
        name = first_text(blv.get("name"), blv.get("username"), blv.get("nickname"), blv.get("nickName"))
        norm = normalize_text(name)
        if name and norm not in seen:
            seen.add(norm)
            people.append(name)
    return " / ".join(people)


def _chuoi_match_from_row(row, sport):
    teams = _nested_obj(row, "teams")
    home = _nested_obj(teams, "home") or _nested_obj(row, "home")
    away = _nested_obj(teams, "away") or _nested_obj(row, "away")
    home_name = first_text(home.get("name"), row.get("homeName"), row.get("team1"))
    away_name = first_text(away.get("name"), row.get("awayName"), row.get("team2"))
    title = first_text(row.get("title"), row.get("name"))
    if home_name and away_name:
        title = f"{home_name} vs {away_name}"
    if not title:
        return None
    external_id = first_text(row.get("externalId"), row.get("external_id"), row.get("id"), row.get("_id"))
    if not external_id:
        return None
    kickoff = None
    for key in ("matchTime", "match_time", "startTime", "start_time", "kickoff", "time_start", "timestamp"):
        kickoff = parse_timestamp_ms(row.get(key))
        if kickoff:
            break
    league = _nested_obj(row, "league")
    status = first_text(row.get("status"), row.get("status_code"))
    live = row.get("isLive") is True or row.get("live") is True or normalize_text(status) in LIVE_STATUSES
    if not kickoff and not live:
        return None
    result = {
        "id": f"chuoichien:{external_id}",
        "provider": "chuoichien",
        "provider_name": "Chuối Chiên",
        "sport": sport,
        "sport_name": sport,
        "sport_api_type": first_text(row.get("sport"), row.get("sportName"), row.get("sport_name"), row.get("sportType"), row.get("category")),
        "name": title,
        "live": live,
        "status": status,
        "competition": first_text(league.get("name"), league.get("title"), row.get("competition")),
        "home_logo": first_text(home.get("logo"), home.get("logoUrl")),
        "away_logo": first_text(away.get("logo"), away.get("logoUrl")),
        "commentator": _chuoi_commentators(row),
        "_chuoi_external_id": external_id,
        "_chuoi_detail": first_text(row.get("detail_api"), row.get("api_url"), row.get("json_url"), row.get("resolver")),
    }
    if kickoff:
        result["kickoff"] = kickoff
    if home_name and away_name:
        # Extract streams from this exact event row. A provider-wide index by
        # team names can attach a previous match's caster or stream.
        exact = parse_direct_chuoichien([row], DIRECT_FEEDS["chuoichien"])
        result["sources"] = exact.get(pair_key(home_name, away_name), [])
    json_sources, json_refs = json_stream_fields(row, {"User-Agent": USER_AGENT, "Referer": "https://chuoichientv.org/"})
    result["sources"] = dedupe_by_stream_url(result.get("sources", []) + json_sources)
    result["resolvers"] = list(dict.fromkeys(link for link, _, _ in json_refs))
    return result


def fetch_chuoichien_supplement_matches(feed=None):
    out = []
    headers = {"User-Agent": USER_AGENT, "Referer": "https://chuoichientv.org/"}
    known_sports = {}
    discovered_sports = set()
    def category(value):
        if isinstance(value, dict):
            return first_text(value.get("slug"), value.get("key"), value.get("id"), value.get("name"))
        return first_text(value)
    def discover_catalog(data):
        if not isinstance(data, dict):
            return
        for parent in (data, _nested_obj(data, "data"), _nested_obj(data, "filters"), _nested_obj(data, "meta")):
            for key in ("sports", "sportTypes", "sport_types", "categories"):
                values = parent.get(key)
                if isinstance(values, list):
                    for entry in values:
                        value = category(entry)
                        if value and normalize_text(value) not in ("all", "tat ca"):
                            discovered_sports.add(value)
    if isinstance(feed, dict):
        for prior in feed.get("matches") or []:
            if isinstance(prior, dict) and provider_key(prior) == "chuoichien":
                for key in ("source_id", "provider_id", "id"):
                    ident = first_text(prior.get(key)).removeprefix("chuoichien:")
                    if ident:
                        known_sports[ident] = sport_category(prior)
    def task(sport):
        rows, seen = [], set()
        previous_page = None
        # The API is paged; a single page of 100 silently loses events.
        for page in range(1, 51):
            suffix = f"/v2/matches?page={page}&limit=100"
            if sport:
                suffix += f"&sport={quote(sport)}"
            if sport == "football":
                suffix += "&type=blv"
            data = None
            for base in CHUOICHIEN_API_BASES:
                data = fetch_json_custom(base + suffix, headers, timeout=5.5)
                if isinstance(data, (dict, list)):
                    break
            if not isinstance(data, (dict, list)):
                raise RuntimeError(f"Chuối Chiên: không đọc được trang {page} của môn {sport or 'tất cả'}")
            if not sport:
                discover_catalog(data)
            page_rows = _chuoi_rows(data)
            signature = tuple(first_text(row.get("externalId"), row.get("id"), row.get("_id")) for row in page_rows[:20])
            for row in page_rows:
                declared = next((value for field in ("sport", "sportName", "sport_name", "sportType", "category")
                                 if (value := category(row.get(field)))), "")
                if not sport and declared:
                    discovered_sports.add(declared)
                # The unfiltered endpoint can expose sports missing from both
                # the old seed list and the aggregated sport.json snapshot.
                ident = first_text(row.get("externalId"), row.get("external_id"), row.get("id"), row.get("_id"))
                match = _chuoi_match_from_row(row, declared or sport or known_sports.get(ident) or "other")
                if match and match["id"] not in seen:
                    seen.add(match["id"])
                    rows.append(match)
            # A short page or repeated page finishes pagination. Do not stop
            # merely because a full page has new API rows lacking valid IDs.
            if len(page_rows) < 100 or signature == previous_page:
                break
            previous_page = signature
        return sport, rows
    _, unfiltered = task("")
    out.extend(unfiltered)
    sports = sorted(discovered_sports)
    # Request only categories returned by the provider or the current worker
    # feed; this list automatically expands when the APIs add a sport.
    if isinstance(feed, dict):
        for row in feed.get("matches") or []:
            if isinstance(row, dict) and provider_key(row) == "chuoichien":
                sport = sport_category(row)
                if sport and sport not in sports:
                    sports.append(sport)
    with ThreadPoolExecutor(max_workers=min(8, len(sports) + 1)) as executor:
        futures = [executor.submit(task, sport) for sport in sports]
        for future in as_completed(futures):
            try:
                _, rows = future.result()
                out.extend(rows)
            except Exception as exc:
                raise RuntimeError(f"Chuối Chiên: danh mục môn thể thao đọc lỗi: {exc}") from exc
    unique = {}
    for match in out:
        existing = unique.get(match["id"])
        if existing is None:
            unique[match["id"]] = match
            continue
        if sport_category(existing) == "other" and sport_category(match) != "other":
            existing["sport"] = match["sport"]
            existing["sport_name"] = match["sport_name"]
        existing["sources"] = dedupe_by_stream_url(existing.get("sources", []) + match.get("sources", []))
        if not existing.get("commentator") and match.get("commentator"):
            existing["commentator"] = match["commentator"]
        existing["resolvers"] = list(dict.fromkeys(existing.get("resolvers", []) + match.get("resolvers", [])))
        if not existing.get("sport_api_type") and match.get("sport_api_type"):
            existing["sport_api_type"] = match["sport_api_type"]
    return list(unique.values())


def _provider_ids_in_feed(data):
    ids = set()
    if not isinstance(data, dict):
        return ids
    providers = data.get("providers")
    if isinstance(providers, list):
        for item in providers:
            if isinstance(item, dict):
                key = provider_canonical(first_text(item.get("key"), item.get("id"), item.get("name")))
            else:
                key = provider_canonical(item)
            if key:
                ids.add(key)
    for match in data.get("matches") or []:
        if isinstance(match, dict):
            key = provider_key(match)
            if key:
                ids.add(key)
    return ids


def _join_people(values):
    out, seen = [], set()
    for value in values:
        text = first_text(value)
        norm = normalize_text(text)
        if text and norm and norm not in seen:
            seen.add(norm)
            out.append(text)
    return " / ".join(out)


def fetch_colatv_supplement_matches():
    data = None
    endpoints = [f"https://api{host}.colatv88xd.cc/api/matches" for host in (1, 2, 3, 4, 5, 6)]
    endpoints.append("https://api.cltvlv.com/api/matches")
    for endpoint in endpoints:
        data = fetch_json_custom(endpoint, {"User-Agent": USER_AGENT}, timeout=4.5)
        if isinstance(data, dict) and isinstance(data.get("data"), dict) and data["data"]:
            break
    if not isinstance(data, dict) or not isinstance(data.get("data"), dict):
        raise RuntimeError("CoLaTV: không đọc được JSON data từ các API đã khai báo")
    out = []
    current = now_ms()
    for raw in data["data"].values():
        if not isinstance(raw, dict):
            continue
        sport_id = first_text(raw.get("sportId"))
        sport = first_text(raw.get("sportName"), raw.get("sport_name"), raw.get("sport"))
        if not sport:
            sport = {"1": "football", "2": "basketball"}.get(sport_id, f"sport {sport_id}" if sport_id else "other")
        kickoff = parse_timestamp_ms(raw.get("matchTime"))
        live_status = raw.get("matchStatus") == 2 or raw.get("isLive") is True or raw.get("live") is True or normalize_text(raw.get("status")) in LIVE_STATUSES
        if not kickoff and not live_status:
            continue
        home = first_text(raw.get("homeTeamName"))
        away = first_text(raw.get("awayTeamName"))
        title = f"{home} vs {away}" if home and away else first_text(raw.get("title"), raw.get("name"), raw.get("matchName"))
        if not title:
            continue
        anchors = raw.get("anchorAppointmentVoList")
        if not isinstance(anchors, list):
            anchors = []
        sources, names = [], []
        for ai, anchor in enumerate(anchors):
            if not isinstance(anchor, dict):
                continue
            caster = first_text(anchor.get("nickName"), anchor.get("nickname"), anchor.get("name"))
            if caster:
                names.append(caster)
            for si, (field, fmt) in enumerate((("playStreamAddress2", "HLS"), ("playStreamAddress", "FLV"))):
                url = first_text(anchor.get(field))
                if url:
                    obj = source_obj(url, caster, fmt, headers={"User-Agent": USER_AGENT, "Referer": "https://cola.tv/"}, name=caster, index=ai * 2 + si)
                    if obj:
                        sources.append(obj)
            # The API supplies additional CDN links in each anchor's servers.
            # Keep that anchor's commentator attached to those links only.
            for si, server in enumerate(anchor.get("servers") or []):
                url = first_text(server) if isinstance(server, str) else first_text(server.get("url"), server.get("link")) if isinstance(server, dict) else ""
                obj = source_obj(url, caster, headers={"User-Agent": USER_AGENT, "Referer": "https://cola.tv/"}, name=caster, index=1000 + ai * 100 + si)
                if obj:
                    sources.append(obj)
        # videoUrl is a match-wide API stream; no anchor owns its commentary.
        video = first_text(raw.get("videoUrl"), raw.get("video_url"))
        obj = source_obj(video, headers={"User-Agent": USER_AGENT, "Referer": "https://cola.tv/"}, index=10000)
        if obj:
            sources.append(obj)
        json_sources, json_refs = json_stream_fields(raw, {"User-Agent": USER_AGENT, "Referer": "https://cola.tv/"})
        item = {
            "id": "colatv:" + first_text(raw.get("matchId"), hashlib.sha1(f"{home}|{away}|{kickoff}".encode()).hexdigest()[:10]),
            "provider": "colatv", "provider_name": "CoLaTV", "sport": sport, "sport_name": sport,
            "sport_api_type": first_text(raw.get("sportId"), raw.get("sportName"), raw.get("sport_name")),
            "name": title,
            "live": live_status,
            "status": "live" if raw.get("matchStatus") == 2 else "scheduled" if raw.get("matchStatus") == 1 else first_text(raw.get("status")),
            "competition": first_text(raw.get("competitionName")), "home_logo": first_text(raw.get("homeTeamLogo")),
            "away_logo": first_text(raw.get("awayTeamLogo")), "commentator": _join_people(names),
            "sources": dedupe_by_stream_url(sources + json_sources),
            "resolvers": list(dict.fromkeys(link for link, _, _ in json_refs)),
        }
        if kickoff:
            item["kickoff"] = kickoff
        out.append(item)
    return out


def _giovang_sport_type(row):
    if not isinstance(row, dict):
        return ""
    league = row.get("league") if isinstance(row.get("league"), dict) else {}
    # This API declares its category in `type`; use that value first.
    keys = ("type", "sport", "sport_type", "sportType", "category", "category_name", "categoryName", "game_type", "gameType")
    raw = ""
    for key in keys:
        raw = first_text(row.get(key))
        if raw:
            break
    if not raw:
        for key in keys:
            raw = first_text(league.get(key))
            if raw:
                break
    n = normalize_text(raw).replace(" ", "")
    # Preserve the provider's explicit category even when the league title
    # looks inconsistent. The playlist should not silently rewrite JSON.
    mapping = {
        "football":"football", "soccer":"football", "bongda":"football", "tennis":"tennis",
        "basketball":"basketball", "bongro":"basketball", "volleyball":"volleyball", "bongchuyen":"volleyball",
        "badminton":"badminton", "caulong":"badminton", "tabletennis":"table tennis", "pingpong":"table tennis",
        "bongban":"table tennis", "baseball":"baseball", "bongchay":"baseball", "boxing":"boxing", "mma":"mma",
        "combat":"mma", "vothuat":"mma", "muaythai":"boxing", "kickboxing":"boxing", "ufc":"mma",
        "esport":"esports", "esports":"esports", "lol":"esports", "gaming":"esports",
    }
    if n in mapping:
        return mapping[n]
    # 'type' is itself the API's sport category. A new type must survive even
    # if no icon or translation has been added to this script yet.
    return normalize_text(raw) if raw else "other"


def _giovang_rows(data):
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("response", "data", "matches", "fixtures"):
        value = data.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
        if isinstance(value, dict):
            for sub in ("response", "data", "matches", "fixtures", "list"):
                nested = value.get(sub)
                if isinstance(nested, list):
                    return [x for x in nested if isinstance(x, dict)]
            indexed = [x for x in value.values() if isinstance(x, dict) and any(k in x for k in ("id", "time_start", "teams"))]
            if indexed:
                return indexed
    return []


def _giovang_page_url(row, event_id, kickoff):
    """The event-page route used by the older working Giờ Vàng builder."""
    explicit = first_text(row.get("match_url"), row.get("match_link"), row.get("page_url"))
    if _json_http_url(explicit) and urlsplit(explicit).hostname == "giovang.org":
        return explicit
    teams = row.get("teams") if isinstance(row.get("teams"), dict) else {}
    home = teams.get("home") if isinstance(teams.get("home"), dict) else {}
    away = teams.get("away") if isinstance(teams.get("away"), dict) else {}
    home_slug = first_text(home.get("slug")) or re.sub(r"[^a-z0-9]+", "-", normalize_text(home.get("name"))).strip("-")
    away_slug = first_text(away.get("slug")) or re.sub(r"[^a-z0-9]+", "-", normalize_text(away.get("name"))).strip("-")
    day_month = first_text(row.get("day_month")).replace("/", "-")
    if not day_month and kickoff:
        day_month = datetime.fromtimestamp(kickoff / 1000, TIME_ZONE).strftime("%d-%m")
    if not all((home_slug, away_slug, day_month, event_id)):
        return ""
    return f"https://giovang.org/truc-tiep-{home_slug}-vs-{away_slug}-{day_month}-{event_id}"


def fetch_giovang_supplement_matches(feed=None):
    base = "https://live-api.keonhacaitp.one"
    headers = {"User-Agent": USER_AGENT, "Referer": "https://giovang.org/"}
    urls = [base + "/storage/livestream/all.json", base + "/storage/livestream/live.json"]
    worker_matches = {}
    for match in feed.get("matches", []) if isinstance(feed, dict) else []:
        if isinstance(match, dict) and provider_key(match) == "giovang":
            for key in ("provider_id", "source_id", "id"):
                ident = first_text(match.get(key)).removeprefix("giovang:")
                if ident:
                    worker_matches[ident] = match
    listing_rows = {}
    failed_urls = []
    with ThreadPoolExecutor(max_workers=2) as ex:
        futures = {ex.submit(fetch_json_custom, u, headers, 5.0): u for u in urls}
        for f in as_completed(futures):
            try:
                result = f.result()
                if not isinstance(result, (dict, list)):
                    failed_urls.append(futures[f])
                else:
                    listing_rows[futures[f]] = _giovang_rows(result)
            except Exception:
                failed_urls.append(futures[f])
    if failed_urls:
        raise RuntimeError("Giờ Vàng: không đọc được JSON " + ", ".join(failed_urls))
    # Process all.json first even when live.json responds faster. A live
    # listing is evidence for an event, not a replacement for terminal status.
    rows = [(row, False) for row in listing_rows.get(urls[0], [])]
    rows.extend((row, True) for row in listing_rows.get(urls[1], []))
    out, by_id = [], {}
    source_counts = {"all": 0, "live": 0}
    for row, is_live_listing in rows:
        event_id = first_text(row.get("id"), row.get("fi"), row.get("fixture_id"), row.get("match_id"))
        if not event_id:
            continue
        previous = by_id.get(event_id)
        worker = worker_matches.get(event_id, {})
        sport = _giovang_sport_type(row)
        if sport == "other":
            sport = first_text(previous.get("sport") if previous else "", worker.get("sport"), "other")
        status = first_text(row.get("status"), row.get("status_code"))
        status_code = first_text(row.get("status_code"), row.get("status"))
        # A current live.json entry supersedes a stale status from all.json.
        # An explicit terminal status in the current entry still takes priority.
        if is_live_listing and not (set(filter(None, (normalize_text(status), normalize_text(status_code)))) & TERMINAL_STATUSES):
            status = status or "live"
            status_code = status_code or "live"
        teams = row.get("teams") if isinstance(row.get("teams"), dict) else {}
        home_obj = teams.get("home") if isinstance(teams.get("home"), dict) else {}
        away_obj = teams.get("away") if isinstance(teams.get("away"), dict) else {}
        home, away = first_text(home_obj.get("name")), first_text(away_obj.get("name"))
        title = f"{home} vs {away}" if home and away and normalize_text(home) != normalize_text(away) else first_text(row.get("title"), row.get("name"), row.get("event_name"), home, away, previous.get("name") if previous else "", worker.get("name"))
        if not title:
            continue
        kickoff = next((value for key in ("time_start", "start_time", "startTime", "matchTime", "kickoff") if (value := parse_timestamp_ms(row.get(key)))), None)
        kickoff = kickoff or (previous.get("kickoff") if previous else None) or get_kickoff(worker)
        if not kickoff and not is_live_listing and normalize_text(status) not in LIVE_STATUSES:
            continue
        blv = row.get("blv") if isinstance(row.get("blv"), list) else [row.get("blv")] if row.get("blv") else []
        names=[]
        for person in blv:
            if isinstance(person, dict):
                names.append(first_text(person.get("name"), person.get("nickname"), person.get("nickName"), person.get("username"), person.get("title"), person.get("blv")))
            else:
                names.append(first_text(person))
        league = row.get("league") if isinstance(row.get("league"), dict) else {}
        listing_url = urls[1] if is_live_listing else urls[0]
        sources, refs = json_stream_fields(row, headers, listing_url)
        source_counts["live" if is_live_listing else "all"] += len(sources)
        detail_urls = []
        for key in ("resolver", "api_url", "json_url", "detail_api", "sources_url"):
            value = row.get(key)
            for raw in value if isinstance(value, list) else [value]:
                link = _json_http_url(raw, listing_url)
                if link and link not in detail_urls:
                    detail_urls.append(link)
        for link, _, _ in refs:
            if link not in detail_urls:
                detail_urls.append(link)
        explicit_live = row.get("is_live") is True or row.get("isLive") is True or row.get("live") is True or normalize_text(status) in LIVE_STATUSES
        item = {
            "id": f"giovang:{event_id}", "provider": "giovang", "provider_name": "Giờ Vàng", "sport": sport,
            "sport_name": sport, "sport_api_type": first_text(row.get("type"), row.get("sport"), row.get("sportType")), "name": title,
            "live": explicit_live,
            "_listed_live": is_live_listing,
            "status": status, "status_code": status_code, "competition": first_text(league.get("title"), league.get("name")),
            "home_logo": first_text(home_obj.get("logo")), "away_logo": first_text(away_obj.get("logo")),
            "commentator": _join_people(names),
            "resolvers": detail_urls, "sources": sources,
        }
        if kickoff:
            item["kickoff"] = kickoff
        # /api/fixtures/{id} was used in the earlier v29 provider adapter.
        # The event ID comes from the current JSON; media URLs come only from
        # the JSON response (or the event page), never from a saved playlist.
        if not is_terminal(item) and (is_live_listing or explicit_live or kickoff and kickoff <= end_of_next_vietnam_day(now_ms())):
            item["_giovang_detail"] = base + "/api/fixtures/" + quote(event_id, safe="")
            item["_giovang_page"] = _giovang_page_url(row, event_id, kickoff)
            item["resolver_headers"] = headers
        if previous:
            for key in ("sport", "sport_name", "name", "kickoff", "competition", "home_logo", "away_logo", "commentator", "_giovang_detail", "_giovang_page", "resolver_headers"):
                if not item.get(key) and previous.get(key):
                    item[key] = previous[key]
                elif item.get(key):
                    previous[key] = item[key]
            previous["resolvers"] = list(dict.fromkeys(previous.get("resolvers", []) + detail_urls))
            previous["sources"] = dedupe_by_stream_url(sources + previous.get("sources", [])) if sources else previous.get("sources", [])
            if is_live_listing:
                previous["_listed_live"] = True
                if not is_terminal(item):
                    previous["status"] = status or "live"
                    previous["status_code"] = status_code or "live"
                    previous["live"] = True
                else:
                    previous["status"] = status
                    previous["status_code"] = status_code
                    previous["live"] = False
            continue
        by_id[event_id] = item
        out.append(item)
    print(f"[giovang] all.json={len(listing_rows.get(urls[0], []))} live.json={len(listing_rows.get(urls[1], []))} trận; URL luồng trong JSON: all={source_counts['all']} live={source_counts['live']}")
    return out


def _extract_html_payload(text):
    if not text:
        return ""
    raw = text.strip()
    if not raw.startswith("{"):
        return raw
    try:
        root = json.loads(raw)
    except Exception:
        return ""
    data = root.get("data") if isinstance(root, dict) and isinstance(root.get("data"), dict) else {}
    parts=[]
    arr = data.get("htmls") if isinstance(data.get("htmls"), list) else root.get("htmls") if isinstance(root, dict) and isinstance(root.get("htmls"), list) else []
    parts.extend(str(x) for x in arr if x)
    if not parts:
        one = first_text(data.get("html"), root.get("html") if isinstance(root, dict) else "")
        if one:
            parts.append(one)
    return "\n".join(parts)


def _xoilac_match_rows(text, base, sport):
    """Read JSON listing rows as well as the HTML fragments returned by the site."""
    try:
        payload = json.loads(text)
    except (ValueError, TypeError):
        return []
    rows = _iter_candidate_matches(payload)
    if not rows and isinstance(payload, dict):
        nested = payload.get("data")
        if isinstance(nested, dict):
            rows = _iter_candidate_matches(nested)
    result = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        home_obj = first_text(_nested_obj(row, "home_team").get("name"), _nested_obj(row, "home").get("name"), row.get("home_name"), row.get("homeName"))
        away_obj = first_text(_nested_obj(row, "away_team").get("name"), _nested_obj(row, "away").get("name"), row.get("away_name"), row.get("awayName"))
        title = first_text(row.get("name"), row.get("title"), row.get("match_name"), row.get("event_name"))
        if home_obj and away_obj:
            title = f"{home_obj} vs {away_obj}"
        if not title:
            continue
        path = first_text(row.get("detail_url"), row.get("url"), row.get("link"))
        row_sources = direct_sources(row)
        if urlsplit(path).path.lower().endswith((".m3u8", ".flv", ".ts")):
            direct = normalize_source({"url":path, "headers":row.get("headers")}, api_index=len(row_sources))
            if direct:
                row_sources = dedupe_by_stream_url(row_sources + [direct])
            path = first_text(row.get("detail_url"))
        slug = first_text(row.get("slug"), row.get("seo_slug"))
        if not path and slug:
            path = "/truc-tiep/" + slug
        if not path and not row_sources:
            continue
        kickoff = next((t for key in ("kickoff", "time_start", "start_time", "startTime", "matchTime", "timestamp") if (t := parse_timestamp_ms(row.get(key)))), None)
        status = first_text(row.get("status"), row.get("status_code"), row.get("state"))
        category = first_text(row.get("sport"), row.get("sport_name"), row.get("sportType"), sport)
        ident = first_text(row.get("id"), row.get("match_id"), slug, path)
        league = _nested_obj(row, "league") or _nested_obj(row, "competition")
        match = {"id":"xoilacxth:"+ident, "provider":"xoilacxth", "provider_name":"XoilacXTH",
                 "sport":category, "sport_name":category, "name":title,
                 "status":status, "live":row.get("live") is True or row.get("isLive") is True or normalize_text(status) in LIVE_STATUSES,
                 "competition":first_text(row.get("competition"), row.get("league"), league.get("name"), league.get("title")),
                 "commentator":first_text(row.get("commentator"), row.get("blv"), row.get("caster")),
                 "home_logo":first_text(_nested_obj(row,"home_team").get("logo"),_nested_obj(row,"home").get("logo")),
                 "away_logo":first_text(_nested_obj(row,"away_team").get("logo"),_nested_obj(row,"away").get("logo")),
                 "_xoilac_detail":urljoin(base.rstrip("/")+"/", path) if path else ""}
        if kickoff:
            match["kickoff"] = kickoff
        match["sources"] = row_sources
        result.append(match)
    return result


def _listing_next_url(payload, current_url):
    """Follow pagination only when the endpoint advertises a next page."""
    try:
        root = json.loads(payload)
    except (TypeError, ValueError):
        return ""
    if not isinstance(root, dict):
        return ""
    candidates = [root, root.get("pagination"), root.get("meta"), root.get("data")]
    for obj in candidates:
        if not isinstance(obj, dict):
            continue
        value = first_text(obj.get("next_page_url"), obj.get("nextPageUrl"), obj.get("next_url"), obj.get("nextUrl"))
        if not value:
            next_obj = obj.get("links")
            value = first_text(next_obj.get("next")) if isinstance(next_obj, dict) else ""
        if value:
            result = urljoin(current_url, value)
            if urlsplit(result).netloc == urlsplit(current_url).netloc:
                return result
    return ""


def _extract_attr(tag, name):
    m = re.search(r"\b" + re.escape(name) + r"\s*=\s*([\"'])(.*?)\1", tag, flags=re.I|re.S)
    return html_lib.unescape(m.group(2)) if m else ""


def _xoilac_match_segments(html_text):
    if not html_text:
        return []
    starts=[m.start() for m in re.finditer(r"<div\b[^>]*class=[\"'][^\"']*(?:main-grid-match|grid-matches__item)[^\"']*[\"'][^>]*>", html_text, flags=re.I)]
    if not starts:
        return []
    starts.append(len(html_text))
    return [html_text[starts[i]:starts[i+1]] for i in range(len(starts)-1)]


def _xoilac_team_from_segment(segment, side):
    class_pat = r"(?:gmd-" + side + r"_team|" + side + r"[^\"']*team)"
    m = re.search(r"<[^>]+class=[\"'][^\"']*" + class_pat + r"[^\"']*[\"'][^>]*>(.{0,1600}?)</(?:div|section|li)>", segment, flags=re.I|re.S)
    if not m:
        return ""
    block=m.group(1)
    # Prefer a named team element; otherwise use cleaned text.
    n = re.search(r"<[^>]+class=[\"'][^\"']*(?:team-name|team-name-group)[^\"']*[\"'][^>]*>(.*?)</[^>]+>", block, flags=re.I|re.S)
    return _strip_tags(n.group(1) if n else block)


def _xoilac_parse_time(segment):
    text=_strip_tags(segment)
    now=datetime.now(TIME_ZONE)
    m=re.search(r"\b(\d{1,2}):(\d{2})\b(?:\s*(?:ngay|ngày)?\s*)?(\d{1,2})/(\d{1,2})(?:/(\d{4}))?", normalize_text(text))
    if m:
        year=int(m.group(5) or now.year)
        try:
            return int(datetime(year,int(m.group(4)),int(m.group(3)),int(m.group(1)),int(m.group(2)),tzinfo=TIME_ZONE).timestamp()*1000)
        except Exception:
            pass
    return None


def _xoilac_origins(feed):
    """Trust only source domains explicitly carried in this provider's resolver URLs."""
    origins = set()
    for row in feed.get("matches", []) if isinstance(feed, dict) else []:
        if not isinstance(row, dict) or provider_key(row) != "xoilacxth":
            continue
        for candidate in (row.get("_xoilac_detail"), resolver_url(row)):
            direct = first_text(candidate)
            if "resolve" in urlsplit(direct).path:
                direct = first_text(dict(parse_qsl(urlsplit(direct).query)).get("url"))
            parsed = urlsplit(direct)
            if parsed.scheme == "https" and parsed.hostname:
                origins.add(f"https://{parsed.netloc}")
    return sorted(origins)


def fetch_xoilac_supplement_matches(feed=None):
    bases=_xoilac_origins(feed)
    out=[]
    categories=[]
    home_loaded = False
    for base in bases:
        home=fetch_text(base+"/", {"User-Agent":USER_AGENT, "Referer":base+"/"}, 4.5)
        home_loaded = home_loaded or bool(home)
        # Follow the URLs actually published in the provider's navigation;
        # do not invent a /sport/{category}/filter/... API route.
        for path in re.findall(r"/sport/[a-zA-Z0-9_-]+(?:/filter/[a-zA-Z0-9_-]+)?", html_lib.unescape(home).replace("\\/", "/"), re.I):
            category = path.split("/")[2].lower()
            listing_url = urljoin(base + "/", path)
            if (category, listing_url, base) not in categories:
                categories.append((category, listing_url, base))
    if bases and not home_loaded:
        raise RuntimeError("Xoilac: không đọc được trang danh mục để phát hiện môn thể thao")
    # If this provider exposes no category links, the primary index and each
    # match's own resolver remain the authoritative paths to its streams.
    def load_category(job):
        category, listing_url, base = job
        results=[]
        url=listing_url
        visited=set()
        for _ in range(12):
            if url in visited:
                break
            visited.add(url)
            payload=fetch_text(url, {"User-Agent": USER_AGENT, "Referer": base + "/"}, 4.5)
            if not payload:
                raise RuntimeError(f"Xoilac: không đọc được trang danh mục {url}")
            results.append((payload, base))
            url=_listing_next_url(payload, url)
            if not url:
                break
        return category, results
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(categories)))) as executor:
        fetched = list(executor.map(load_category, categories))
    for category, payloads in fetched:
      for payload, used_base in payloads:
        out.extend(_xoilac_match_rows(payload, used_base, category))
        html=_extract_html_payload(payload)
        if not html:
            continue
        for seg in _xoilac_match_segments(html):
            anchor=re.search(r"<a\b([^>]*)href=[\"']([^\"']*/truc-tiep/[^\"']+)[\"']([^>]*)>", seg, flags=re.I|re.S)
            if not anchor:
                continue
            tag=anchor.group(0)
            href=html_lib.unescape(anchor.group(2))
            home=_xoilac_team_from_segment(seg,"home")
            away=_xoilac_team_from_segment(seg,"away")
            if not home or not away:
                title=_extract_attr(tag,"title")
                h,a=split_match_teams(title)
                home=home or h; away=away or a
            title = f"{home} vs {away}" if home and away else _extract_attr(tag,"title")
            if not title:
                continue
            names=[]
            for cm in re.finditer(r"<(?:a|div|span)\b[^>]*class=[\"'][^\"']*(?:blv-item|commentator|grid-match__commentator)[^\"']*[\"'][^>]*>(.*?)</(?:a|div|span)>", seg, flags=re.I|re.S):
                name=_strip_tags(cm.group(1))
                if name and normalize_text(name) != "vs": names.append(name)
            status_match=re.search(r"\bdata-status=[\"']([^\"']+)[\"']", seg, flags=re.I)
            status=status_match.group(1) if status_match else ""
            live=normalize_text(status) in LIVE_STATUSES
            kickoff=_xoilac_parse_time(seg)
            fid_match=re.search(r"\bdata-fid=[\"']([^\"']+)[\"']", seg, flags=re.I)
            fid=fid_match.group(1) if fid_match else href.rstrip('/').split('/')[-1]
            league_match=re.search(r"<[^>]+class=[\"'][^\"']*(?:match-league|league-name)[^\"']*[\"'][^>]*>(.*?)</[^>]+>", seg, flags=re.I|re.S)
            league=_strip_tags(league_match.group(1)) if league_match else ""
            match={"id":"xoilacxth:"+fid,"provider":"xoilacxth","provider_name":"XoilacXTH","sport":category,"sport_name":category,
                   "name":title,"live":live,"status":status,"competition":league,"commentator":_join_people(names),
                   "_xoilac_detail":urljoin(used_base.rstrip('/')+'/', href)}
            if kickoff: match["kickoff"]=kickoff
            out.append(match)
    unique={}
    for match in out:
        key=(match["id"], sport_category(match))
        if key not in unique:
            unique[key]=match
        elif match.get("sources"):
            unique[key]["sources"]=dedupe_by_stream_url(unique[key].get("sources", [])+match["sources"])
    return list(unique.values())


def fetch_all_provider_supplements(data):
    jobs=[]
    failures={}
    adapters={
        "chuoichien": fetch_chuoichien_supplement_matches,
        "colatv": fetch_colatv_supplement_matches,
        "giovang": lambda: fetch_giovang_supplement_matches(data),
        "xoilacxth": fetch_xoilac_supplement_matches,
        "gavang33": lambda: fetch_direct_supplement_matches("gavang33", data),
    }
    started = monotonic()
    with ThreadPoolExecutor(max_workers=len(adapters)) as ex:
        futures={}
        for provider, func in adapters.items():
            # The worker's provider list can lag behind a working provider
            # API. Probe each configured endpoint; only returned events enter
            # the playlist. Xoilac still discovers its origin from the feed.
            future = ex.submit(func, data) if provider in ("chuoichien", "xoilacxth") else ex.submit(func)
            futures[future] = (provider, monotonic())
        for f in as_completed(futures):
            provider, submitted = futures[f]
            try:
                rows=f.result()
                source_kind = "link danh mục/JSON" if provider == "xoilacxth" else "API riêng"
                print(f"[adapter] {provider}: {len(rows)} trận từ {source_kind}; {monotonic() - submitted:.1f}s")
                if rows: jobs.extend(rows)
            except Exception as exc:
                print(f"[adapter] {provider}: lỗi lấy dữ liệu sau {monotonic() - submitted:.1f}s: {exc}", file=sys.stderr)
                failures[provider] = str(exc)
    print(f"[timing] API riêng: {monotonic() - started:.1f}s")
    if failures:
        # These provider endpoints augment the worker feed; the Android app
        # itself reads sport.json and each fixture's resolver. One blocked
        # provider must not prevent other providers from updating.
        data["_failed_provider_adapters"] = failures
    return jobs


def _same_match(a, b):
    if provider_key(a) != provider_key(b):
        return False
    ids_a = {first_text(a.get(key)).removeprefix(provider_key(a) + ":") for key in ("id", "provider_id", "source_id", "_chuoi_external_id") if first_text(a.get(key))}
    ids_b = {first_text(b.get(key)).removeprefix(provider_key(b) + ":") for key in ("id", "provider_id", "source_id", "_chuoi_external_id") if first_text(b.get(key))}
    if ids_a & ids_b:
        return True
    if sport_category(a) != sport_category(b):
        return False
    if normalize_text(match_name(a)) != normalize_text(match_name(b)):
        return False
    ka, kb = get_kickoff(a), get_kickoff(b)
    if ka and kb:
        return abs(ka - kb) <= 10 * 60 * 1000
    return bool(first_text(a.get("id")) and first_text(a.get("id")) == first_text(b.get("id")))


def merge_supplement_matches(base_matches, supplements):
    result = list(base_matches)
    for extra in supplements:
        found = None
        for current in result:
            if isinstance(current, dict) and _same_match(current, extra):
                found = current
                break
        if found is None:
            # A newly discovered event can carry a direct source and a JSON
            # resolver simultaneously. Keep current API sources separate from
            # potentially stale embedded sources in the aggregate index.
            if isinstance(extra.get("sources"), list):
                extra["_provider_sources"] = dedupe_by_stream_url(
                    direct_sources(extra) + extra.get("_provider_sources", [])
                )
            extra["_provider_fresh"] = True
            extra["_provider_resolvers"] = declared_match_json_urls(extra)
            result.append(extra)
            continue
        found["_provider_fresh"] = True
        for key in ("_chuoi_external_id", "_chuoi_detail", "_giovang_detail", "_giovang_page", "resolver_headers", "_xoilac_detail", "sport_api_type",
                    "name", "kickoff", "competition", "home_logo", "away_logo", "commentator"):
            if extra.get(key):
                found[key] = extra[key]
        if sport_category(extra) != "other":
            for key in ("sport", "sport_name"):
                if extra.get(key):
                    found[key] = extra[key]
        extra_json_urls = declared_match_json_urls(extra)
        if extra_json_urls:
            found["_provider_resolvers"] = list(dict.fromkeys(extra_json_urls + found.get("_provider_resolvers", [])))
            found["resolvers"] = list(dict.fromkeys(found.get("resolvers", []) + extra_json_urls))
        # A fresh provider feed can close an event still marked live in an older
        # aggregated index. Terminal status must win over the stale live flag.
        if is_terminal(extra):
            found["status"] = first_text(extra.get("status"), extra.get("status_code"))
            found["status_code"] = first_text(extra.get("status_code"), extra.get("status"))
            found["live"] = False
            found["_listed_live"] = False
        elif first_text(extra.get("status"), extra.get("status_code")):
            # Live and scheduled states in the provider API supersede an older
            # status from the aggregated index; keep long events only if live.
            found["status"] = first_text(extra.get("status"))
            found["status_code"] = first_text(extra.get("status_code"))
            found["live"] = extra.get("live") is True
            found["_listed_live"] = extra.get("_listed_live") is True
        if isinstance(extra.get("sources"), list):
            # Keep the newest provider response separate from embedded sources
            # in sport.json, which may contain an obsolete CDN URL.
            found["_provider_sources"] = dedupe_by_stream_url(
                direct_sources(extra) + found.get("_provider_sources", [])
            )
        if extra.get("live") is True and not is_terminal(extra):
            found["live"] = True
        if extra.get("_listed_live") is True and not is_terminal(extra):
            found["_listed_live"] = True
    return result


def provider_canonical(value):
    norm = normalize_text(value)
    return PROVIDER_ALIASES.get(norm, norm.replace(" ", ""))


def split_match_teams(value):
    text = scalar_text(value)
    if not text:
        return "", ""
    parts = re.split(r"(?i)\s+(?:vs\.?|v|versus)\s+", text, maxsplit=1)
    if len(parts) != 2:
        return "", ""
    return normalize_text(parts[0]), normalize_text(parts[1])


def pair_key(home, away):
    return f"{normalize_text(home)}|{normalize_text(away)}"


def source_obj(url, caster="", fmt="", quality="", headers=None, name="", index=0):
    if not scalar_text(url):
        return None
    obj = {
        "url": scalar_text(url),
        "headers": normalize_headers(headers or {}),
        "_api_index": index,
    }
    if caster:
        obj["commentator"] = scalar_text(caster)
        obj["_direct_caster"] = scalar_text(caster)
    if fmt:
        obj["type"] = fmt
    if quality:
        obj["quality"] = quality
    if name:
        obj["name"] = name
    return obj


def _iter_candidate_matches(data):
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    for key in ("matches", "result", "data", "response", "list", "items", "rows"):
        value = data.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            for subkey in ("matches", "list", "items", "rows", "response"):
                nested = value.get(subkey)
                if isinstance(nested, list):
                    return nested
    if isinstance(data.get("data"), dict):
        return list(data["data"].values())
    return []


def parse_direct_chuoichien(data, cfg):
    out = {}
    playback_headers = {
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://live.chuoichien.tv",
        "Referer": "https://live.chuoichien.tv/",
    }
    rows = _iter_candidate_matches(data)
    for row in rows:
        if not isinstance(row, dict):
            continue
        match = row.get("match") if isinstance(row.get("match"), dict) else row
        teams = _nested_obj(row, "teams") or _nested_obj(match, "teams")
        home_obj = row.get("home") if isinstance(row.get("home"), dict) else match.get("home") if isinstance(match.get("home"), dict) else _nested_obj(teams, "home")
        away_obj = row.get("away") if isinstance(row.get("away"), dict) else match.get("away") if isinstance(match.get("away"), dict) else _nested_obj(teams, "away")
        home = first_text(home_obj.get("name"), home_obj.get("nickname"), row.get("team1"), row.get("homeName"), match.get("team1"), match.get("homeName"))
        away = first_text(away_obj.get("name"), away_obj.get("nickname"), row.get("team2"), row.get("awayName"), match.get("team2"), match.get("awayName"))
        if not home or not away:
            continue
        sources = []
        blvs = row.get("blvs") or row.get("branchStreamUrls") or match.get("blvs") or match.get("branchStreamUrls")
        if isinstance(blvs, list):
            for bi, blv in enumerate(blvs):
                if not isinstance(blv, dict):
                    continue
                caster = first_text(blv.get("name"), blv.get("nickname"), blv.get("blv"))
                streams = blv.get("streams") or blv.get("branchStreamUrls") or blv.get("urls")
                if isinstance(streams, list):
                    for si, st in enumerate(streams):
                        if isinstance(st, str):
                            url, label = st, ""
                        elif isinstance(st, dict):
                            url = first_text(st.get("url"), st.get("link"), st.get("streamUrl"))
                            label = first_text(st.get("label"), st.get("name"))
                        else:
                            continue
                        fmt = "HLS" if ".m3u8" in url.lower() else "FLV" if ".flv" in url.lower() else ""
                        quality = quality_label({"name": label})
                        obj = source_obj(url, caster, fmt, quality, playback_headers, label or caster, bi*100+si)
                        if obj:
                            sources.append(obj)
        top_streams = row.get("streams") or match.get("streams")
        single_blv = first_text(row.get("blv"), match.get("blv"))
        if isinstance(top_streams, list):
            for si, st in enumerate(top_streams):
                if not isinstance(st, dict):
                    continue
                url = first_text(st.get("url"), st.get("link"), st.get("streamUrl"))
                label = first_text(st.get("label"), st.get("name"))
                fmt = "HLS" if ".m3u8" in url.lower() else "FLV" if ".flv" in url.lower() else ""
                obj = source_obj(url, single_blv, fmt, quality_label({"name": label}), playback_headers, label, 10000+si)
                if obj:
                    sources.append(obj)
        if sources:
            out[pair_key(home, away)] = dedupe_sources(sources)
    return out


def parse_direct_gavang33(data, cfg):
    out = {}
    root = data.get("data") if isinstance(data, dict) else None
    if not isinstance(root, dict):
        return out
    playback_headers = {"User-Agent": USER_AGENT, "Referer": "https://gavang33.co/"}
    for raw in root.values():
        if not isinstance(raw, dict):
            continue
        home_obj = raw.get("homeTeam") if isinstance(raw.get("homeTeam"), dict) else {}
        away_obj = raw.get("awayTeam") if isinstance(raw.get("awayTeam"), dict) else {}
        home, away = first_text(home_obj.get("name")), first_text(away_obj.get("name"))
        if not home or not away:
            continue
        sources = []
        anchors = raw.get("anchorAppointmentVoList")
        if isinstance(anchors, list):
            for ai, anchor in enumerate(anchors):
                if not isinstance(anchor, dict):
                    continue
                caster = first_text(anchor.get("nickName"), anchor.get("nickname"), anchor.get("name"))
                for field in ("streamUrls", "servers"):
                    urls = anchor.get(field)
                    if not isinstance(urls, list):
                        continue
                    for ui, item in enumerate(urls):
                        url = first_text(item) if isinstance(item, str) else first_text(item.get("url"), item.get("link")) if isinstance(item, dict) else ""
                        if not url:
                            continue
                        fmt = "FLV" if ".flv" in url.lower() else "HLS" if ".m3u8" in url.lower() else ""
                        sources.append(source_obj(url, caster, fmt, headers=playback_headers, name=caster, index=ai*100+ui))
        if sources:
            out[pair_key(home, away)] = dedupe_sources(sources)
    return out


DIRECT_PARSERS = {
    "gavang33": parse_direct_gavang33,
}


def fetch_direct_supplement_matches(provider, feed=None):
    """Supplement every sport declared by this provider's current JSON."""
    cfg = DIRECT_FEEDS.get(provider)
    if not cfg:
        return []
    data = fetch_json_custom(cfg["url"], cfg.get("headers"), timeout=5.5)
    if data is None:
        raise RuntimeError(f"{provider}: không đọc được JSON {cfg['url']}")
    parser = DIRECT_PARSERS.get(cfg["kind"])
    rows = _iter_candidate_matches(data)
    sport_by_id = {}
    for prior in feed.get("matches", []) if isinstance(feed, dict) else []:
        if isinstance(prior, dict) and provider_key(prior) == provider:
            for ident in (prior.get("provider_id"), prior.get("source_id"), prior.get("id")):
                if first_text(ident):
                    sport_by_id[first_text(ident).removeprefix(provider + ":")] = sport_category(prior)
    out = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        match = _nested_obj(raw, "match") or raw
        home = first_text(_nested_obj(match, "homeTeam").get("name"), _nested_obj(match, "home").get("name"), raw.get("hostName"), raw.get("homeName"))
        away = first_text(_nested_obj(match, "awayTeam").get("name"), _nested_obj(match, "away").get("name"), raw.get("guestName"), raw.get("awayName"))
        title = f"{home} vs {away}" if home and away else first_text(match.get("title"), match.get("name"))
        if not title:
            continue
        # Parse only the current row, not an index keyed by names of two teams.
        one_row = {"data": {"row": raw}}
        exact = parser(one_row, cfg) if parser else {}
        sources = exact.get(pair_key(home, away), []) if home and away else []
        if not sources:
            sources = direct_sources(match)
        # Each provider can add sports and stream fields without changing the
        # old hand-written anchor parser. Read declared JSON fields as well.
        json_sources, json_refs = json_stream_fields(raw, cfg.get("headers"), cfg["url"])
        sources = dedupe_by_stream_url(sources + json_sources)
        kickoff = next((t for key in ("matchTime", "time_start", "startTime", "start_time", "kickoff", "timestamp") if (t := parse_timestamp_ms(match.get(key)))), None)
        status = first_text(match.get("status"), match.get("state"), match.get("matchStatus"))
        sport = first_text(match.get("sportName"), match.get("sport"), match.get("sportType"), match.get("category"))
        sport_id = first_text(match.get("sportId"))
        if not sport:
            sport = {"1": "football", "2": "basketball"}.get(sport_id, f"sport {sport_id}" if sport_id else "")
        ident = first_text(match.get("slug"), match.get("id"), match.get("matchId"), match.get("match_id"))
        if not ident:
            ident = hashlib.sha1(f"{title}|{kickoff}".encode()).hexdigest()[:12]
        sport = sport or sport_by_id.get(ident) or sport_by_id.get(first_text(match.get("matchId"))) or "other"
        competition = match.get("competition")
        competition_obj = competition if isinstance(competition, dict) else {}
        anchors = match.get("anchorAppointmentVoList") if isinstance(match.get("anchorAppointmentVoList"), list) else []
        anchor_people = _join_people(first_text(anchor.get("nickName"), anchor.get("nickname"), anchor.get("name")) for anchor in anchors if isinstance(anchor, dict))
        item = {"id":provider+":"+ident, "provider":provider, "provider_name":provider,
                "sport":sport, "sport_name":sport, "sport_api_type": first_text(match.get("sportId"), match.get("sportName"), match.get("sport"), match.get("sportType"), match.get("category")), "name":title,
                "status":status, "live":match.get("live") is True or match.get("isLive") is True or normalize_text(status) in LIVE_STATUSES,
                "competition":first_text(match.get("competitionName"), competition_obj.get("name"), competition_obj.get("title"), competition),
                "home_logo":first_text(_nested_obj(match, "homeTeam").get("logo"), _nested_obj(match, "home").get("logo")),
                "away_logo":first_text(_nested_obj(match, "awayTeam").get("logo"), _nested_obj(match, "away").get("logo")),
                "commentator":first_text(match.get("commentator"), anchor_people),
                "sources":sources, "resolvers":list(dict.fromkeys(link for link, _, _ in json_refs))}
        if kickoff:
            item["kickoff"] = kickoff
        out.append(item)
    return out


def _strip_tags(text):
    text = re.sub(r"<script\\b[^>]*>.*?</script>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<style\\b[^>]*>.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\\s+", " ", html_lib.unescape(text)).strip()


def _extract_stream_url_from_text(text):
    if not text:
        return ""
    patterns = [
        "[\\\"'](https?://[^\\\"']+?\\.m3u8(?:\\?[^\\\"']*)?)[\\\"']",
        "[\\\"'](https?://[^\\\"']+?\\.flv(?:\\?[^\\\"']*)?)[\\\"']",
        "(https?://[^\\s\\\"'<>]+?\\.m3u8(?:\\?[^\\s\\\"'<>]*)?)",
        "(https?://[^\\s\\\"'<>]+?\\.flv(?:\\?[^\\s\\\"'<>]*)?)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.I)
        if m:
            return html_lib.unescape(m.group(1)).replace("\\/", "/")
    return ""


def _xoilac_player_links(page_html, page_url):
    out = []
    if not page_html:
        return out
    pattern = re.compile("<a\\b([^>]*\\bplayer-link\\b[^>]*)>(.*?)</a>", re.I | re.S)
    for i, m in enumerate(pattern.finditer(page_html)):
        attrs = m.group(1)
        data = re.search("\\bdata-link=[\\\"']([^\\\"']+)[\\\"']", attrs, flags=re.I)
        if not data:
            continue
        label = _strip_tags(m.group(2))
        target = urljoin(page_url, html_lib.unescape(data.group(1)))
        if target:
            out.append((i, label, target))
    return out


def fetch_xoilac_sources(candidates):
    relevant = [item for item in candidates if provider_key(item["match"]) == "xoilacxth"]
    if not relevant:
        return {}
    origins = _xoilac_origins({"matches": [item["match"] for item in relevant]})
    pages = {}
    for item in relevant:
        match = item["match"]
        direct = first_text(match.get("_xoilac_detail"))
        if not direct:
            direct = first_text(dict(parse_qsl(urlsplit(resolver_url(match)).query)).get("url"))
        if direct and f"{urlsplit(direct).scheme}://{urlsplit(direct).netloc}" in origins:
            pages[item["api_index"]] = direct
    if not pages:
        return {}

    page_htmls = {}
    with ThreadPoolExecutor(max_workers=min(8, len(pages))) as ex:
        fmap = {}
        for idx, url in pages.items():
            headers = {"User-Agent": USER_AGENT, "Referer": url}
            fmap[ex.submit(fetch_text, url, headers, 4)] = (idx, url)
        for fut in as_completed(fmap):
            idx, url = fmap[fut]
            try:
                page_htmls[idx] = (url, fut.result())
            except Exception:
                page_htmls[idx] = (url, "")

    jobs = []
    for idx, (page_url, body) in page_htmls.items():
        for order, label, target in _xoilac_player_links(body, page_url):
            jobs.append((idx, order, label, target, page_url))

    result = {}
    if jobs:
        with ThreadPoolExecutor(max_workers=min(12, len(jobs))) as ex:
            fmap = {
                ex.submit(fetch_text, target, {"User-Agent": USER_AGENT, "Referer": page_url}, 4):
                (idx, order, label, target, page_url)
                for idx, order, label, target, page_url in jobs
            }
            for fut in as_completed(fmap):
                idx, order, label, target, page_url = fmap[fut]
                try:
                    body = fut.result()
                except Exception:
                    body = ""
                stream = _extract_stream_url_from_text(body) or _extract_stream_url_from_text(target)
                if not stream:
                    continue
                fmt = "HLS" if ".m3u8" in stream.lower() else "FLV" if ".flv" in stream.lower() else ""
                # Player-link text is a source identity, not automatically a commentator.
                obj = source_obj(
                    stream,
                    caster="",
                    fmt=fmt,
                    headers={"User-Agent": USER_AGENT, "Referer": page_url},
                    name=label,
                    index=order,
                )
                result.setdefault(idx, []).append(obj)
    return {idx: dedupe_sources(srcs) for idx, srcs in result.items() if srcs}


def fetch_giovang_page_sources(candidates):
    """Read the stream JSON embedded in each current provider event page."""
    pages = {
        item["api_index"]: item["match"]["_giovang_page"]
        for item in candidates
        if provider_key(item["match"]) == "giovang" and item["match"].get("_giovang_page")
    }
    found = {}
    if not pages:
        return found
    def load_page(url):
        headers = {"User-Agent": USER_AGENT, "Referer": "https://giovang.org/"}
        return fetch_text(with_cache_buster(url), headers, 4) or fetch_text(url, headers, 4)
    with ThreadPoolExecutor(max_workers=min(8, len(pages))) as executor:
        futures = {
            executor.submit(load_page, url): (idx, url)
            for idx, url in pages.items()
        }
        for future in as_completed(futures):
            idx, page_url = futures[future]
            try:
                page = future.result()
            except Exception:
                continue
            data = re.search(r"data-blv\s*=\s*([\"'])(.*?)\1", page, re.I | re.S)
            if not data:
                continue
            try:
                rows = json.loads(html_lib.unescape(data.group(2)).replace("\\/", "/"))
            except (ValueError, TypeError):
                continue
            headers = {"User-Agent": USER_AGENT, "Referer": page_url, "Origin": "https://giovang.org"}
            sources, _ = json_stream_fields(rows, headers, page_url)
            if sources:
                found[idx] = sources
    print(f"[giovang] trang chi tiết: {len(pages)} trận, {sum(map(len, found.values()))} URL luồng từ data-blv")
    return found


def fetch_chuoichien_detail_sources(candidates):
    """Resolve each current match through the provider's external-ID API."""
    relevant = []
    for item in candidates:
        match = item["match"]
        if provider_key(match) != "chuoichien":
            continue
        external_id = first_text(match.get("_chuoi_external_id"), match.get("provider_id"),
                                 match.get("source_id"), match.get("id")).removeprefix("chuoichien:")
        if external_id:
            relevant.append((item["api_index"], match, external_id))
    if not relevant:
        return {}
    headers = {"User-Agent": USER_AGENT, "Referer": "https://chuoichientv.org/", "Origin": "https://live.chuoichien.tv"}
    def fetch_one(index, match, external_id):
        suffix = "/v2/matches/external/" + quote(external_id, safe="")
        for base in CHUOICHIEN_API_BASES:
            url = base + suffix
            body = fetch_json_custom(url, headers, timeout=5.0)
            if isinstance(body, (dict, list)):
                current = {**match, "headers": {**headers, **normalize_headers(match.get("headers"))}}
                return index, follow_json_streams(body, current, url)
        return index, []
    found = {}
    with ThreadPoolExecutor(max_workers=min(12, len(relevant))) as executor:
        futures = [executor.submit(fetch_one, idx, match, external_id) for idx, match, external_id in relevant]
        for future in as_completed(futures):
            try:
                idx, sources = future.result()
                if sources:
                    found[idx] = sources
            except Exception as exc:
                print(f"[chuoichien] lỗi JSON chi tiết: {exc}", file=sys.stderr)
    print(f"[chuoichien] chi tiết: {len(relevant)} trận, {sum(map(len, found.values()))} URL luồng")
    return found


def source_label_commentator(source, match):
    """Use a source label as a caster only when the API labels it as such."""
    match_norm = normalize_text(match_name(match))
    competition_norm = normalize_text(competition_name(match))
    for key in ("name", "label", "title", "server", "provider"):
        raw = first_text(source.get(key))
        if not raw:
            continue
        # Strong signal: upstream explicitly labels this as BLV/caster/commentator.
        prefixed = bool(re.match(r"(?i)^\s*(?:blv|caster|commentator)\b", raw))
        human = strip_stream_technical_tokens(raw)
        if not human or technical_label(human) or is_provider_identity(human, match):
            continue
        norm = normalize_text(human)
        if norm in KNOWN_PROVIDER_IDS or norm in {match_norm, competition_norm}:
            continue
        if re.search(r"(?i)\b(?:vs\.?|versus)\b", human):
            continue
        # A whole source description is not a caster unless the label is explicit.
        if not prefixed:
            continue
        return human

    # A lone match-level name does not prove ownership of this stream.
    return ""

def now_ms():
    return int(datetime.now().timestamp() * 1000)


def parse_timestamp_ms(value):
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        text = scalar_text(value)
        try:
            number = float(text)
        except ValueError:
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=TIME_ZONE)
                return int(parsed.timestamp() * 1000)
            except ValueError:
                return None
    if not math.isfinite(number) or number <= 0:
        return None
    if number > 100_000_000_000_000:
        number /= 1000
    elif number < 100_000_000_000:
        number *= 1000
    return int(number)


def get_kickoff(match):
    for key in KICKOFF_FIELDS:
        value = parse_timestamp_ms(match.get(key))
        if value is not None:
            return value
    return None


def end_of_next_vietnam_day(timestamp_ms):
    current = datetime.fromtimestamp(timestamp_ms / 1000, TIME_ZONE)
    target = datetime.combine(current.date() + timedelta(days=1), time.max, tzinfo=TIME_ZONE)
    return int(target.timestamp() * 1000)


def status_values(match):
    values = set()
    for key in ("status_code", "status", "state", "phase"):
        value = normalize_text(match.get(key))
        if value:
            values.add(value)
    return values


def is_terminal(match):
    return bool(status_values(match) & TERMINAL_STATUSES)


def is_explicit_live(match):
    return match.get("live") is True or match.get("_listed_live") is True or bool(status_values(match) & LIVE_STATUSES)


def classify_match(match, current_ms, future_end_ms):
    if is_terminal(match):
        return None
    kickoff = get_kickoff(match)
    explicit_live = is_explicit_live(match)
    if kickoff is None:
        return {"block": 0, "kind": "live", "kickoff": 0} if explicit_live else None
    if explicit_live:
        return {"block": 0, "kind": "live", "kickoff": kickoff}
    if kickoff > current_ms:
        if kickoff <= future_end_ms:
            return {"block": 1, "kind": "upcoming", "kickoff": kickoff}
        return None
    age = current_ms - kickoff
    if 0 <= age <= RECENT_WINDOW_MS:
        return {"block": 0, "kind": "recent", "kickoff": kickoff}
    if (RECENT_WINDOW_MS < age <= VERIFY_LIVE_WINDOW_MS
            and (declared_match_json_urls(match) or match.get("_provider_sources")
                 or first_text(match.get("_xoilac_detail"), match.get("_giovang_page")))):
        # A delayed API status can hide a long live event. Only retain it when
        # a freshly resolved stream is confirmed below; a cached URL alone
        # must never extend the event's lifetime.
        return {"block": 0, "kind": "needs_live_check", "kickoff": kickoff}
    return None


def format_match_time(timestamp_ms):
    if not timestamp_ms:
        return "[LIVE]"
    return datetime.fromtimestamp(timestamp_ms / 1000, TIME_ZONE).strftime("[%H:%M] %d/%m")


def format_update_group(timestamp_ms):
    return datetime.fromtimestamp(timestamp_ms / 1000, TIME_ZONE).strftime("🕒 Cập nhật %H:%M %d/%m")


def match_name(match):
    return first_text(match.get("name"), match.get("match_name"), match.get("matchName"), match.get("title"))


def competition_name(match):
    return first_text(match.get("competition"), match.get("league"), match.get("tournament"), match.get("championship"), match.get("competition_name"), match.get("league_name"))


def sport_category(match):
    raw = normalize_text(first_text(match.get("sport"), match.get("sport_name"), match.get("sportType"), match.get("category")))
    aliases = {
        "soccer": "football", "bong da": "football", "association football": "football",
        "bong ro": "basketball", "bong chuyen": "volleyball", "quan vot": "tennis",
        "the thao dien tu": "esports", "esport": "esports", "e sport": "esports",
        "dua xe": "motorsport", "motor sport": "motorsport", "racing": "motorsport", "formula 1": "motorsport",
        "vo thuat": "mma",
    }
    if raw in aliases:
        return aliases[raw]
    return raw or "other"


def sport_icon(match):
    direct = first_text(match.get("sport_icon"), match.get("sportIcon"), match.get("icon"), match.get("emoji"))
    if direct and len(direct) <= 8:
        return direct
    category = sport_category(match)
    if category in SPORT_ICON:
        return SPORT_ICON[category]
    for key, icon in SPORT_ICON.items():
        if category.startswith(key + " ") or category.endswith(" " + key):
            return icon
    return "🏅"


def provider_key(match):
    return normalize_text(first_text(match.get("provider"), match.get("source"), match.get("provider_key"))) or "other"


def provider_name(match):
    return first_text(match.get("provider_name"), match.get("source_name"), match.get("provider"), match.get("source"), "Khác")


def provider_order(data, items):
    names = {}
    seen = []
    providers = data.get("providers") if isinstance(data, dict) else None
    if isinstance(providers, list):
        for item in providers:
            if isinstance(item, dict):
                key = normalize_text(first_text(item.get("key"), item.get("id"), item.get("provider"), item.get("name")))
                name = first_text(item.get("name"), item.get("label"), item.get("provider_name"), key)
            else:
                key = normalize_text(item)
                name = scalar_text(item)
            if key and key not in names:
                names[key] = name or key
                seen.append(key)
    for item in items:
        key = provider_key(item["match"])
        if key not in names:
            names[key] = provider_name(item["match"])
            seen.append(key)
    return [(key, names[key]) for key in seen]


def direct_match_commentator(match):
    return first_text(match.get("commentator"), match.get("blv"), match.get("caster"))


def split_people(value):
    raw = scalar_text(value)
    if not raw:
        return []
    parts = re.split(r"\s*(?:/|\\|\||•|;|&|\+|,| và | and )\s*", raw, flags=re.I)
    out = []
    seen = set()
    for part in parts:
        part = re.sub(r"(?i)^\s*(?:blv|caster|commentator)\s*[:\-]?\s*", "", part).strip(" ()[]{}-–—")
        norm = normalize_text(part)
        if part and norm and norm not in GENERIC_STREAM_WORDS and norm not in seen:
            seen.add(norm)
            out.append(part)
    return out


def normalize_headers(headers):
    result = {}
    if not isinstance(headers, dict):
        return result
    for key, value in headers.items():
        text = scalar_text(value)
        if text:
            result[str(key)] = text
    return result


def normalize_source(raw, fallback_headers=None, api_index=0):
    fallback_headers = fallback_headers or {}
    if isinstance(raw, str):
        url = raw.strip()
        return {"url": url, "headers": dict(fallback_headers), "_api_index": api_index} if url else None
    if not isinstance(raw, dict):
        return None
    url = first_text(raw.get("url"), raw.get("link"), raw.get("src"), raw.get("stream_url"), raw.get("streamUrl"), raw.get("play_url"), raw.get("playUrl"), raw.get("file"))
    if not url:
        return None
    result = dict(raw)
    result["url"] = url
    headers = dict(fallback_headers)
    headers.update(normalize_headers(raw.get("headers")))
    result["headers"] = headers
    result["_api_index"] = api_index
    return result


def source_key(source):
    headers = sorted((str(k).lower().strip(), str(v).strip()) for k, v in (source.get("headers") or {}).items())
    identity = [
        source.get("url", ""), source.get("type", ""), source.get("name", ""), source.get("provider", ""),
        source.get("commentator", ""), source.get("blv", ""), source.get("caster", ""),
        source.get("audio_name", ""), source.get("audioName", ""),
        "\n".join(f"{k}:{v}" for k, v in headers),
    ]
    return "\n".join(scalar_text(v) for v in identity)


def dedupe_sources(sources):
    result = []
    seen = set()
    for source in sources:
        if not source or not source.get("url"):
            continue
        key = source_key(source)
        if key in seen:
            continue
        seen.add(key)
        result.append(source)
    return result


def dedupe_by_stream_url(sources):
    """One entry per URL, preserving authoritative per-stream metadata."""
    result, positions = [], {}
    for source in sources:
        if not source or not source.get("url"):
            continue
        key = html_lib.unescape(source["url"]).strip()
        if key not in positions:
            positions[key] = len(result)
            result.append(source)
        else:
            original = result[positions[key]]
            if source.get("_direct_caster") and not original.get("_direct_caster"):
                original["_direct_caster"] = source["_direct_caster"]
                original["commentator"] = source["_direct_caster"]
            if not original.get("name") and source.get("name"):
                original["name"] = source["name"]
            if not original.get("type") and source.get("type"):
                original["type"] = source["type"]
            headers = normalize_headers(source.get("headers"))
            if headers:
                original["headers"] = {**headers, **normalize_headers(original.get("headers"))}
    return result


def probe_stream(source):
    """Check a stream response with its own HTTP headers; never guess URL age."""
    url = first_text(source.get("url"))
    path = urlsplit(url).path.lower()
    if not path.endswith((".m3u8", ".flv", ".ts")):
        return "unknown"
    headers = normalize_headers(source.get("headers"))
    headers.setdefault("User-Agent", USER_AGENT)
    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=2.5) as response:
            body = response.read(4096 if path.endswith(".m3u8") else 32)
            if path.endswith(".m3u8"):
                playlist = body.lstrip(b"\xef\xbb\xbf \t\r\n")
                return "ok" if playlist.startswith(b"#EXTM3U") and b"#EXT-X-ENDLIST" not in playlist else "unknown"
            if path.endswith(".flv"):
                return "ok" if body.startswith(b"FLV") else "unknown"
            return "ok" if body.startswith(b"\x47") else "unknown"
    except HTTPError as exc:
        # A GitHub runner can receive a geo-routed 404 for a stream that
        # remains playable by the user's IPTV client in another region.
        return "dead" if exc.code == 410 else "unknown"
    except (URLError, TimeoutError, OSError, ValueError):
        return "unknown"


def prioritize_verified_sources(items):
    """Verify uncertain long events before calling them live."""
    if os.environ.get("PLAYLIST_VERIFY_HLS", "1") == "0":
        return [item for item in items if item["state"].get("kind") != "needs_live_check"]
    unique = {}
    pending_items = [item for item in items if item["state"].get("kind") == "needs_live_check"]
    normal_items = [item for item in items if item["state"].get("block") == 0 and len(item["sources"]) > 1]
    single_live_items = [item for item in items if item["state"].get("kind") == "live" and len(item["sources"]) == 1]
    for group in (pending_items, normal_items, single_live_items):
        # Round robin: one provider with many streams cannot consume the entire
        # verification budget before the next match gets a chance.
        for position in range(max((len(item["sources"]) for item in group), default=0)):
            for item in group:
                if position >= len(item["sources"]):
                    continue
                source = item["sources"][position]
                key = (source["url"], tuple(sorted(normalize_headers(source.get("headers")).items())))
                if key not in unique and urlsplit(source["url"]).path.lower().endswith((".m3u8", ".flv", ".ts")):
                    unique[key] = source
    # Bound execution for the five-minute GitHub Actions job.
    selected = list(unique.items())[:120]
    checks = {}
    if selected:
        with ThreadPoolExecutor(max_workers=min(24, len(selected))) as executor:
            futures = {executor.submit(probe_stream, source): key for key, source in selected}
            for future in as_completed(futures):
                try:
                    checks[futures[future]] = future.result()
                except Exception:
                    checks[futures[future]] = "unknown"
    retained = []
    confirmed = 0
    for item in items:
        def state(source):
            key = (source["url"], tuple(sorted(normalize_headers(source.get("headers")).items())))
            return checks.get(key, "unknown")
        sources = item["sources"]
        # 404/410 is a conclusive missing URL for this build. Unknown/timeouts
        # remain available because geography and client headers can differ.
        sources = [source for source in sources if state(source) != "dead"]
        if not sources:
            continue
        if item["state"].get("kind") == "needs_live_check":
            if not any(state(source) == "ok" for source in sources):
                continue
            item["state"] = {**item["state"], "kind": "live", "_verified_live": True}
            confirmed += 1
        item["sources"] = sorted(sources, key=lambda source: 0 if state(source) == "ok" else 1 if state(source) == "unknown" else 2)
        retained.append(item)
    if pending_items:
        print(f"[live-check] quá 135 phút, API chưa xác nhận LIVE: giữ {confirmed}/{len(pending_items)} trận có luồng được kiểm tra đang phát")
    return retained


def resolver_url(match):
    value = first_text(match.get("resolver"), match.get("resolve_url"), match.get("resolver_url"))
    if value:
        return value
    sources = match.get("sources")
    if isinstance(sources, list):
        for raw in sources:
            if isinstance(raw, dict):
                value = first_text(raw.get("resolver"), raw.get("resolve_url"), raw.get("resolver_url"))
                if value:
                    return value
    return ""


def declared_match_json_urls(match):
    """Read API links declared for this match, including per-source resolvers."""
    links = []
    def add(value):
        for raw in value if isinstance(value, list) else [value]:
            url = _json_http_url(first_text(raw.get("url"), raw.get("href")) if isinstance(raw, dict) else raw)
            if url and url not in links:
                links.append(url)
    # For events present in the current provider JSON, never resolve media
    # through the aggregate worker: its fixture/status/URL can be cached.
    keys = (("_provider_resolvers", "_giovang_detail", "_chuoi_detail") if match.get("_provider_fresh") else
            ("resolvers", "_giovang_detail", "_chuoi_detail", "resolver", "resolve_url", "resolver_url", "json_url", "json_urls", "api_url", "api_urls", "sources_url", "streams_url"))
    for key in keys:
        add(match.get(key))
    for group in (() if match.get("_provider_fresh") else ("sources", "streams", "links", "urls")):
        for row in match.get(group, []) if isinstance(match.get(group), list) else []:
            if isinstance(row, dict):
                for key in JSON_REFERENCE_FIELDS:
                    add(row.get(key))
                candidates = (row.get("url"), row.get("link"))
                if normalize_text(first_text(row.get("type"), row.get("format"))) in {"resolver", "json", "api"}:
                    for value in candidates:
                        add(value)
                    continue
            else:
                candidates = (row,)
            for value in candidates:
                if urlsplit(first_text(value)).path.lower().endswith(".json"):
                    add(value)
    return links


def fetch_match_json_bodies(matches):
    requested = {}
    owners = {}
    for match in matches:
        if not isinstance(match, dict):
            continue
        headers = normalize_headers(match.get("resolver_headers")) or normalize_headers(match.get("headers"))
        for url in declared_match_json_urls(match):
            requested.setdefault(url, headers)
            owners.setdefault(url, []).append((provider_key(match), sport_category(match), first_text(match.get("id"))))
    urls = list(requested)
    bodies = {}
    if urls:
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(urls))) as executor:
            future_map = {executor.submit(fetch_fresh_resolver, url, requested[url]): url for url in urls}
            for future in as_completed(future_map):
                url = future_map[future]
                try:
                    bodies[url] = future.result()
                except Exception:
                    bodies[url] = None
    print(f"[resolver] đã hỏi {len(urls)} URL JSON được các trận khai báo; {sum(isinstance(v, (dict, list)) for v in bodies.values())} trả JSON")
    totals, examples = {}, {}
    for url in urls:
        success = isinstance(bodies.get(url), (dict, list))
        for provider, sport, event_id in owners[url]:
            key = (provider, sport)
            if key not in totals:
                totals[key] = [0, 0]
            totals[key][0] += 1
            totals[key][1] += success
            if not success and len(examples.setdefault(key, [])) < 3:
                examples[key].append(event_id)
    for (provider, sport), (asked, valid) in sorted(totals.items()):
        print(f"[resolver] {provider}/{sport}: JSON={valid}/{asked}" +
              (f"; không trả JSON: {', '.join(examples[(provider, sport)])}" if valid < asked else ""))
    return bodies


def refresh_match_from_json(match, bodies):
    """Only overwrite match status when a newly fetched JSON declares it."""
    recognized = LIVE_STATUSES | TERMINAL_STATUSES | {"upcoming", "scheduled", "ns", "not started", "chua bat dau"}
    for url, body in bodies.items():
        if not isinstance(body, dict):
            continue
        for obj in (body, body.get("match"), body.get("data"), body.get("response")):
            if not isinstance(obj, dict):
                continue
            status = first_text(obj.get("status_code"), obj.get("matchStatus"), obj.get("status"))
            if provider_key(match) == "colatv" and obj.get("matchStatus") == 2:
                status = "live"
            # The just-fetched provider listing is authoritative for its
            # explicit LIVE flag and sport. A slower detail endpoint can
            # retain yesterday's terminal state for the same fixture ID.
            listing_live = match.get("_provider_fresh") and is_explicit_live(match)
            if normalize_text(status) in recognized and not (listing_live and normalize_text(status) in TERMINAL_STATUSES):
                match["status"] = status
                match["status_code"] = status
            if isinstance(obj.get("live"), bool) and not (listing_live and obj["live"] is False):
                match["live"] = obj["live"]
            elif isinstance(obj.get("is_live"), bool) and not (listing_live and obj["is_live"] is False):
                match["live"] = obj["is_live"]
            elif isinstance(obj.get("isLive"), bool) and not (listing_live and obj["isLive"] is False):
                match["live"] = obj["isLive"]
            if first_text(obj.get("commentator"), obj.get("caster")):
                match["commentator"] = first_text(obj.get("commentator"), obj.get("caster"))
            league = _nested_obj(obj, "league") or _nested_obj(obj, "competition")
            competition = first_text(obj.get("competition"), obj.get("league"),
                                     league.get("name"), league.get("title"), obj.get("competitionName"))
            if competition:
                match["competition"] = competition
            sport = first_text(obj.get("sport"), obj.get("sport_name"), obj.get("sportName"))
            if sport and not (match.get("_provider_fresh") and first_text(match.get("sport_api_type"))):
                match["sport"] = sport
                match["sport_name"] = sport
            if any(isinstance(obj.get(key), dict) for key in ("home", "homeTeam", "home_team", "teams")):
                name = first_text(obj.get("name"), obj.get("match_name"), obj.get("matchName"), obj.get("title"))
                if name:
                    match["name"] = name


def direct_sources(match):
    headers = normalize_headers(match.get("headers"))
    sources = []
    index = 0
    for key in ("sources", "streams", "links", "urls"):
        array = match.get(key)
        if isinstance(array, list):
            for raw in array:
                if isinstance(raw, dict) and resolver_url({"sources": [raw]}) and not first_text(raw.get("url"), raw.get("link"), raw.get("src")):
                    continue
                source = normalize_source(raw, headers, index)
                index += 1
                if source and _json_media_url(source["url"]) and normalize_text(first_text(source.get("type"), source.get("format"))) not in {"resolver", "json", "api"}:
                    sources.append(source)
    return dedupe_sources(sources)


def resolver_sources(body, match):
    sources, _ = json_stream_fields(body, normalize_headers(match.get("headers")))
    return dedupe_by_stream_url(sources)


JSON_STREAM_ARRAYS = frozenset({
    "sources", "streams", "links", "urls", "streamUrls", "stream_urls",
    "branchStreamUrls", "servers", "lives", "playbacks", "qualities",
})
JSON_STREAM_FIELDS = frozenset({
    "videoUrl", "video_url", "playStreamAddress", "playStreamAddress2",
    "mobile_stream_url", "pc_stream_url", "link_stream_hd", "link_stream_sd",
    "stream_url", "streamUrl", "sourceUrl", "source_url", "play_url",
    "playUrl", "hdM3u8", "m3u8", "hdFlv", "flv",
})
JSON_REFERENCE_FIELDS = frozenset({
    "resolver", "resolvers", "resolve_url", "resolver_url", "api_url", "api_urls", "json_url", "json_urls",
    "sources_url", "streams_url", "stream_api", "detail_api", "detail_url",
})
def _json_http_url(value, base=""):
    raw = scalar_text(value)
    if not raw or len(raw) > 4096:
        return ""
    try:
        url = urljoin(base, raw) if base else raw
        parsed = urlsplit(url)
    except ValueError:
        return ""
    if parsed.scheme not in ("https", "http") or not parsed.hostname or parsed.username or parsed.password:
        return ""
    return url


def _json_media_url(url):
    path = urlsplit(url).path.lower()
    return bool(path.strip("/")) and not path.endswith((".json", ".html", ".htm", ".jpg", ".jpeg", ".png", ".svg", ".webp"))


def json_stream_fields(payload, fallback_headers=None, base_url="", inherited_caster=""):
    """Read the API's stream fields and explicit JSON references, not arbitrary URLs."""
    streams, references = [], []
    base_headers = normalize_headers(fallback_headers)
    count = 0

    def walk(node, key="", headers=None, caster="", depth=0):
        nonlocal count
        if depth > 12 or count > 100000:
            return
        if isinstance(node, list):
            for item in node:
                walk(item, key, headers, caster, depth + 1)
            return
        if not isinstance(node, dict):
            if isinstance(node, str) and key in JSON_STREAM_ARRAYS:
                add(node, {}, headers, caster, key)
            return
        count += 1
        current_headers = {**(headers or base_headers), **normalize_headers(node.get("headers"))}
        # Only names attached to an anchor or an individual source belong to
        # that stream. A match-wide commentator is not a per-stream identity.
        local_caster = first_text(node.get("commentator"), node.get("blv"), node.get("blv_name"), node.get("caster"))
        if key in ("blv", "blvs", "commentators", "anchorAppointmentVoList", "anchors", "sources", "streams"):
            local_caster = first_text(local_caster, node.get("nickName"), node.get("nickname"), node.get("name") if key in ("blv", "blvs", "commentators", "anchorAppointmentVoList", "anchors") else "")
        effective_caster = local_caster or caster
        source_context = key in JSON_STREAM_ARRAYS or key in ("source", "stream", "playback")
        owner_context = source_context or bool(node.get("blv_name")) or key in ("blv", "blvs", "commentators", "anchorAppointmentVoList", "anchors")
        for field, value in node.items():
            if field in JSON_REFERENCE_FIELDS:
                for raw in value if isinstance(value, list) else [value]:
                    link = _json_http_url(first_text(raw.get("url"), raw.get("href")) if isinstance(raw, dict) else raw, base_url)
                    if link:
                        references.append((link, effective_caster if owner_context else caster, current_headers))
            elif field in JSON_STREAM_FIELDS:
                add(value, node, current_headers, effective_caster if owner_context else caster, field)
            elif field in ("url", "link", "src", "file") and source_context:
                add(value, node, current_headers, effective_caster, field)
            elif field in ("url", "link", "src", "file") and _json_http_url(value, base_url) and urlsplit(_json_http_url(value, base_url)).path.lower().endswith((".m3u8", ".flv", ".mpd", ".ts", ".mp4")):
                add(value, node, current_headers, effective_caster if owner_context else caster, field)
            elif isinstance(value, (dict, list)):
                walk(value, field, current_headers, effective_caster if owner_context else caster, depth + 1)
            elif source_context and isinstance(value, str) and _json_http_url(value, base_url):
                path = urlsplit(_json_http_url(value, base_url)).path.lower()
                if path.endswith((".m3u8", ".flv", ".mpd", ".ts", ".mp4")) or re.match(r"(?i)^(?:server|stream|hls|flv|cdn|backup)[\s_\-\d]*$", field):
                    add(value, node, current_headers, effective_caster, field)

    def add(value, container, headers, caster, field):
        if isinstance(value, list):
            for part in value:
                add(part, container, headers, caster, field)
            return
        if isinstance(value, dict):
            walk(value, field, headers, caster, 1)
            return
        url = _json_http_url(value, base_url)
        if not url:
            return
        if urlsplit(url).path.lower().endswith(".json") or normalize_text(first_text(container.get("type"), container.get("format"))) in {"resolver", "json", "api"}:
            references.append((url, caster, headers or base_headers))
            return
        if not _json_media_url(url):
            return
        obj = normalize_source({
            "url": url, "headers": headers or base_headers,
            "name": first_text(container.get("name") if field in ("url", "link", "src") else "", container.get("label")),
            "type": first_text(container.get("type"), container.get("format")),
            "quality": first_text(container.get("quality")),
            "commentator": first_text(container.get("commentator"), container.get("blv"), container.get("caster"), caster),
        }, api_index=len(streams))
        if obj:
            streams.append(obj)

    walk(payload, headers=base_headers, caster=inherited_caster)
    return dedupe_by_stream_url(streams), references


def follow_json_streams(body, match, resolver, loaded=None, seen_urls=None):
    """Follow only JSON references actually returned by an API, with cycle bounds."""
    loaded = loaded if loaded is not None else {}
    playback_headers = normalize_headers(match.get("headers"))
    if provider_key(match) == "giovang":
        playback_headers = {**normalize_headers(match.get("resolver_headers")), **playback_headers}
    pending = [(resolver, body, "", playback_headers, 0)]
    visited, streams = set(), []
    while pending and len(visited) < 64:
        url, payload, caster, headers, depth = pending.pop(0)
        if url in visited or depth > 3 or not isinstance(payload, (dict, list)):
            continue
        visited.add(url)
        if seen_urls is not None and url not in seen_urls:
            seen_urls.append(url)
        found, refs = json_stream_fields(payload, headers, url, caster)
        streams.extend(found)
        for link, child_caster, child_headers in refs:
            if link in visited or any(entry[0] == link for entry in pending):
                continue
            if link not in loaded:
                loaded[link] = fetch_fresh_resolver(link, child_headers)
            if isinstance(loaded[link], (dict, list)):
                pending.append((link, loaded[link], child_caster, child_headers, depth + 1))
    if pending:
        print(f"[resolver] {resolver}: còn JSON liên kết chưa đọc do giới hạn 64 URL", file=sys.stderr)
    return dedupe_by_stream_url(streams)


def read_match_json_graphs(indexed_matches, root_bodies):
    """Fetch linked JSON concurrently and keep metadata belonging to each match."""
    streams_by_index, bodies_by_index = {}, {}
    if not indexed_matches:
        return streams_by_index, bodies_by_index

    def read_one(index, match):
        cache = dict(root_bodies)
        seen = []
        streams = []
        for url in declared_match_json_urls(match):
            streams.extend(follow_json_streams(cache.get(url), match, url, cache, seen))
        return index, dedupe_by_stream_url(streams), {url: cache[url] for url in seen if isinstance(cache.get(url), dict)}

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(indexed_matches))) as executor:
        futures = [executor.submit(read_one, index, match) for index, match in indexed_matches]
        for future in as_completed(futures):
            try:
                index, streams, bodies = future.result()
                streams_by_index[index] = streams
                bodies_by_index[index] = bodies
            except Exception as exc:
                print(f"[resolver] lỗi đọc JSON liên kết: {exc}", file=sys.stderr)
    print(f"[resolver] luồng từ JSON liên kết: {sum(len(v) for v in streams_by_index.values())}; phản hồi JSON: {sum(len(v) for v in bodies_by_index.values())}")
    return streams_by_index, bodies_by_index


def fetch_fresh_resolver(url, headers=None):
    # Prefer a fresh resolve; some providers reject extra query parameters.
    if headers:
        return (fetch_json_custom(url, headers, RESOLVER_TIMEOUT, True)
                or fetch_json_custom(url, headers, RESOLVER_TIMEOUT, False))
    return fetch_json(url, RESOLVER_TIMEOUT, 1, True) or fetch_json(url, RESOLVER_TIMEOUT, 1, False, report_failure=True)


def infer_format(source):
    raw_type = normalize_text(first_text(source.get("type"), source.get("format"), source.get("protocol"), source.get("extension"), source.get("ext")))
    if raw_type:
        if "hls" in raw_type or "mpegurl" in raw_type:
            return "HLS"
        if "flv" in raw_type:
            return "FLV"
        if "dash" in raw_type or "mpd" in raw_type:
            return "DASH"
        if raw_type in {"ts", "mpeg ts", "mpegts"}:
            return "TS"
        if "mp4" in raw_type:
            return "MP4"
    url = str(source.get("url") or "").lower().split("?", 1)[0]
    if ".m3u8" in url:
        return "HLS"
    if ".flv" in url:
        return "FLV"
    if ".mpd" in url:
        return "DASH"
    if re.search(r"\.ts(?:$|/)", url):
        return "TS"
    if ".mp4" in url:
        return "MP4"
    label = " ".join(first_text(source.get(k)) for k in ("name", "label", "title", "server"))
    if re.search(r"(?i)(?:^|\W)hls\d*(?:$|\W)", label):
        return "HLS"
    if re.search(r"(?i)(?:^|\W)flv\d*(?:$|\W)", label):
        return "FLV"
    if re.search(r"(?i)(?:^|\W)dash\d*(?:$|\W)", label):
        return "DASH"
    return ""


def quality_label(source):
    width = first_text(source.get("width"), source.get("video_width"), source.get("videoWidth"))
    height = first_text(source.get("height"), source.get("video_height"), source.get("videoHeight"))
    try:
        w, h = int(float(width)), int(float(height))
    except (ValueError, TypeError):
        w = h = 0
    evidence = []
    if w and h:
        evidence.append(f"{w}x{h}")
    for key in ("resolution", "video_resolution", "videoResolution", "dimensions", "dimension", "video_quality", "videoQuality", "quality", "name", "label", "title"):
        text = first_text(source.get(key))
        if text:
            evidence.append(text)
    joined = " | ".join(evidence)
    dim = re.search(r"(?<!\d)(\d{3,4})\s*[xX×]\s*(\d{3,4})(?!\d)", joined)
    if dim:
        w, h = int(dim.group(1)), int(dim.group(2))
        if w >= 3800 or h >= 2100:
            return "4K"
        if w >= 1900 or h >= 1000:
            return "FHD"
        if w >= 1200 or h >= 700:
            return "HD"
        return f"{w}x{h}"
    low = joined.lower()
    if re.search(r"(?<!\d)2160p?(?!\d)|\b4k\b|\buhd\b", low):
        return "4K"
    if re.search(r"(?<!\d)1080p?(?!\d)|\bfhd\b|\bfull\s*hd\b", low):
        return "FHD"
    if re.search(r"(?<!\d)720p?(?!\d)|\bhd\b", low):
        return "HD"
    if re.search(r"\bsd\b|(?<!\d)(?:480|360)p?(?!\d)", low):
        return "SD"
    return ""


def technical_label(value):
    text = normalize_text(value)
    if not text or text in GENERIC_STREAM_WORDS:
        return True
    if re.fullmatch(r"(?:server|sv|stream|source|backup|main|primary|mirror)\s*#?\d*", text):
        return True
    if re.fullmatch(r"(?:hls|flv|dash|ts|mp4)\d*", text):
        return True
    if re.fullmatch(r"(?:4k|uhd|qhd|fhd|hd|sd|2160p?|1440p?|1080p?|720p?|576p?|540p?|480p?|360p?|\d{3,4}x\d{3,4})", text):
        return True
    return False


def clean_human_label(value):
    raw = scalar_text(value)
    if not raw:
        return ""
    if technical_label(raw):
        return ""
    raw = re.sub(r"(?i)^\s*(?:blv|caster|commentator)\s*[:\-]?\s*", "", raw)
    parts = re.split(r"\s*(?:•|\||;|,|/|\\)\s*", raw)
    humans = []
    for part in parts:
        tokens = []
        for token in part.strip(" ()[]{}-–—").split():
            stripped = token.strip("()[]{}-–—")
            if technical_label(stripped):
                continue
            tokens.append(token)
        candidate = " ".join(tokens).strip(" ()[]{}-–—")
        if candidate and not technical_label(candidate):
            humans.append(candidate)
    if len(humans) == 1:
        return humans[0]
    return ""


def slug_text(value):
    text = normalize_text(value)
    return re.sub(r"[^a-z0-9]+", "", text)


def match_known_people(match):
    return split_people(direct_match_commentator(match))


def explicit_source_people(source):
    for key in ("commentator", "blv", "caster", "audio_name", "audioName", "voice", "presenter"):
        value = first_text(source.get(key))
        if value:
            return split_people(value)
    return []


def is_provider_identity(value, match):
    norm = normalize_text(value)
    if not norm:
        return False
    match_provider = normalize_text(first_text(match.get("provider"), match.get("provider_name"), match.get("source"), match.get("source_name")))
    return norm in KNOWN_PROVIDER_IDS or (match_provider and norm == match_provider)


def strip_stream_technical_tokens(value):
    raw = scalar_text(value)
    if not raw:
        return ""
    text = re.sub(r"(?i)^\s*(?:blv|caster|commentator)\s*[:\-]?\s*", "", raw).strip()
    text = re.sub(r"(?i)\b(?:hls|flv|dash|mpeg\s*ts|ts|mp4)\s*#?\d*\b", " ", text)
    text = re.sub(r"(?i)\b(?:4k|uhd|qhd|fhd|full\s*hd|hd|sd|2160p?|1440p?|1080p?|720p?|576p?|540p?|480p?|360p?)\b", " ", text)
    text = re.sub(r"(?i)\b\d{3,4}\s*[xX×]\s*\d{3,4}\b", " ", text)
    text = re.sub(r"(?i)\b(?:backup|mirror|main|primary|auto|server|source|stream|link|channel|cdn|sv)\s*#?\d*\b", " ", text)
    text = re.sub(r"[\[\](){}•|;,]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -–—/\\")
    return text.strip()


def people_match_from_evidence(people, evidence_values):
    if not people:
        return ""
    normalized_values = [normalize_text(v) for v in evidence_values if scalar_text(v)]
    evidence_norm = " ".join(normalized_values)
    evidence_slug = slug_text(" ".join(scalar_text(v) for v in evidence_values if scalar_text(v)))
    hits = []
    for person in people:
        norm = normalize_text(person)
        slug = slug_text(person)
        if not norm:
            continue
        # Short nicknames such as A/B must match a complete token. Without this,
        # "B" accidentally matches the B in "BLV".
        if len(norm) <= 2:
            matched = any(re.search(r"(?:^|\s)" + re.escape(norm) + r"(?:$|\s)", value) for value in normalized_values)
        else:
            matched = norm in evidence_norm or (slug and len(slug) >= 3 and slug in evidence_slug)
        if matched and person not in hits:
            hits.append(person)
    return hits[0] if len(hits) == 1 else ""


def source_commentator(source, match):
    # Authoritative per-stream metadata from direct provider APIs wins.
    direct_caster = first_text(source.get("_direct_caster"))
    if direct_caster:
        return direct_caster

    # A single source-level field is explicit metadata; keep its spelling.
    written_caster = first_text(source.get("commentator"), source.get("blv"), source.get("caster"))
    if written_caster and len(split_people(written_caster)) == 1:
        return written_caster

    explicit = explicit_source_people(source)
    if len(explicit) == 1:
        return explicit[0]
    if len(explicit) > 1:
        matched = people_match_from_evidence(
            explicit,
            [source.get("name"), source.get("label"), source.get("title"), source.get("server")],
        )
        if matched:
            return matched

    label_caster = source_label_commentator(source, match)
    if label_caster:
        return label_caster

    # If source identity explicitly points to one of the match commentators, use
    # that person for this source.
    known = match_known_people(match)
    evidence = [
        source.get("name"), source.get("label"), source.get("title"),
        source.get("server"), source.get("provider"),
    ]
    matched = people_match_from_evidence(known, evidence)
    if matched:
        return matched

    # Match-level blv lists people shown on a card, not ownership of a
    # specific URL. Never assign an unlabeled stream an invented caster.
    return ""

def order_sources(sources, match):
    caster_order = {}
    next_rank = 0
    decorated = []
    for source in sources:
        caster = source_commentator(source, match)
        caster_norm = normalize_text(caster)
        if caster_norm:
            if caster_norm not in caster_order:
                caster_order[caster_norm] = next_rank
                next_rank += 1
            caster_rank = caster_order[caster_norm]
            unknown = 0
        else:
            caster_rank = 10**6
            unknown = 1
        fmt = infer_format(source)
        fmt_rank = FORMAT_ORDER.get(fmt, 9)
        decorated.append(((unknown, caster_rank, fmt_rank, int(source.get("_api_index", 10**6))), source))
    decorated.sort(key=lambda item: item[0])
    return [source for _, source in decorated]


def source_meta(source, match):
    caster = source_commentator(source, match)
    quality = quality_label(source)
    fmt = infer_format(source)
    info = []
    if quality:
        info.append(quality)
    if fmt:
        info.append(fmt)
    return caster, " • ".join(info)


def match_identity(match):
    provider = provider_key(match)
    match_id = first_text(match.get("id"), match.get("match_id"), match.get("matchId"))
    kickoff = get_kickoff(match) or 0
    return "|".join([provider, match_id, str(kickoff), normalize_text(match_name(match)), normalize_text(competition_name(match))])


def sport_order_for_block(items):
    order = {}
    for item in items:
        category = sport_category(item["match"])
        if category not in order:
            order[category] = len(order)
    return order


def match_sort_key(item, sport_order):
    kickoff = item["state"].get("kickoff") or get_kickoff(item["match"]) or 0
    return (
        item["state"].get("block", 1),
        sport_order.get((item["state"].get("block", 1), sport_category(item["match"])), 10**6),
        0 if kickoff == 0 else 1,
        kickoff,
        item.get("api_index", 10**6),
    )


def build_sport_order(items):
    order = {}
    for block in (0, 1):
        for item in sorted(items, key=lambda x: x.get("api_index", 10**6)):
            if item["state"].get("block") != block:
                continue
            key = (block, sport_category(item["match"]))
            if key not in order:
                order[key] = len([k for k in order if k[0] == block])
    return order


def escape_attr(value):
    # Metadata URLs can contain commas (image crop parameters); title cleaning
    # must not rewrite them into another URL.
    return scalar_text(value).replace("\r", " ").replace("\n", " ").replace("&", "&amp;").replace('"', "&quot;")


def header_value(headers, name):
    wanted = name.lower()
    for key, value in (headers or {}).items():
        if str(key).lower() == wanted:
            return scalar_text(value)
    return ""


def stream_url_with_headers(source):
    # Keep the stream URL byte-for-byte as the provider returned it.
    # Playback headers are emitted separately as #EXTVLCOPT lines below.
    # Appending Kodi-style |Header=... parameters breaks a number of IPTV
    # players and is redundant when EXTVLCOPT already carries UA/referrer.
    return scalar_text(source.get("url"))


def stable_source_id(match, source):
    raw = f"{match_identity(match)}\n{source_key(source)}".encode("utf-8")
    return "sport-" + hashlib.sha1(raw).hexdigest()[:12]


def resolve_all(candidates, json_source_map=None):
    # Provider-declared detail JSON is already included in json_source_map.
    if json_source_map is None:
        roots = fetch_match_json_bodies([item["match"] for item in candidates])
        json_source_map, _ = read_match_json_graphs([(item["api_index"], item["match"]) for item in candidates], roots)
    # These three providers have independent detail endpoints. Read them in
    # parallel without dropping any match or secondary stream URL.
    started = monotonic()
    with ThreadPoolExecutor(max_workers=3) as executor:
        xoilac_future = executor.submit(fetch_xoilac_sources, candidates)
        giovang_future = executor.submit(fetch_giovang_page_sources, candidates)
        chuoi_future = executor.submit(fetch_chuoichien_detail_sources, candidates)
        xoilac_map = xoilac_future.result()
        giovang_page_map = giovang_future.result()
        chuoi_detail_map = chuoi_future.result()
    print(f"[timing] chi tiết trực tiếp 3 nguồn: {monotonic() - started:.1f}s")
    embedded_map = {}
    fresh_provider_map = {}
    provider_direct_map = {}
    for item in candidates:
        match = item["match"]
        embedded_map[item["api_index"]] = direct_sources(match)
        fresh_provider_map[item["api_index"]] = direct_sources({"sources": match.get("_provider_sources", [])})
        idx = item["api_index"]
        provider_direct_map[idx] = dedupe_by_stream_url(
            chuoi_detail_map.get(idx, []) + giovang_page_map.get(idx, []) + xoilac_map.get(idx, [])
        )

    resolved = []
    for item in candidates:
        match = item["match"]
        exact = provider_direct_map.get(item["api_index"], [])
        direct = fresh_provider_map.get(item["api_index"], [])
        urls = declared_match_json_urls(match)
        remote = json_source_map.get(item["api_index"], [])
        # Combine the current direct feed and freshly called resolver: the
        # resolver may expose additional commentators/backup servers. Never
        # reuse embedded sport.json sources when a resolver exists; those URLs
        # may predate a CDN change. The provider feed is first for its caster.
        embedded = embedded_map.get(item["api_index"], []) if not urls and not (direct or exact) else []
        if provider_key(match) == "giovang" and match.get("_provider_fresh"):
            sources = dedupe_by_stream_url(remote + direct + exact)
        elif exact and provider_key(match) in ("giovang", "chuoichien"):
            sources = dedupe_by_stream_url(exact + direct + remote)
        elif direct:
            sources = dedupe_by_stream_url(direct + remote + exact)
        else:
            sources = dedupe_by_stream_url(remote + exact + embedded)
        # A match-level commentator cannot identify an individual backup URL.
        match["_stream_count"] = len(sources)
        sources = order_sources(sources, match)
        if sources:
            resolved.append({**item, "sources": sources, "direct_provider": bool(exact)})
    return prioritize_verified_sources(resolved)


def build_title(match, state, source):
    live = "🔴 " if state.get("kind") == "live" else ""
    name = safe_title_text(match_name(match))
    caster, stream_info = source_meta(source, match)
    competition = safe_title_text(competition_name(match))
    caster_part = f" ({safe_title_text(caster)})" if caster else ""
    competition_part = f" • {competition}" if competition else ""
    stream_part = f" [{stream_info}]" if stream_info else ""
    return f"{live}{format_match_time(get_kickoff(match))} {sport_icon(match)} {name}{caster_part}{competition_part}{stream_part}".strip()


def build_playlist():
    build_started = monotonic()
    data = fetch_json(SPORT_API, timeout=10, retries=1, cache_bust=True)
    if not isinstance(data, dict):
        data = fetch_json(SPORT_API, timeout=10, retries=2, cache_bust=False)
    if not isinstance(data, dict):
        raise RuntimeError("Không lấy được sport.json")
    upstream_update = parse_timestamp_ms(data.get("updated"))
    if upstream_update and now_ms() - upstream_update > 60 * 60 * 1000:
        print(f"[freshness] sport.json đã cập nhật cách đây {(now_ms() - upstream_update) // 60000} phút")
    matches = data.get("matches") if isinstance(data.get("matches"), list) else []
    supplements = fetch_all_provider_supplements(data)
    matches = merge_supplement_matches(matches, supplements)
    # Resolve the current API JSON before time filtering: a long-running event
    # can have an old kickoff and a freshly declared live status.
    resolver_started = monotonic()
    resolver_bodies = fetch_match_json_bodies(matches)
    indexed_matches = [(index, match) for index, match in enumerate(matches) if isinstance(match, dict)]
    json_source_map, json_metadata = read_match_json_graphs(indexed_matches, resolver_bodies)
    print(f"[timing] resolver JSON và JSON liên kết: {monotonic() - resolver_started:.1f}s")
    for index, match in indexed_matches:
        refresh_match_from_json(match, json_metadata.get(index, {}))
    current = now_ms()
    future_end = end_of_next_vietnam_day(current)
    candidates = []
    filtered, filtered_examples = Counter(), {}
    for api_index, match in enumerate(matches):
        if not isinstance(match, dict):
            continue
        state = classify_match(match, current, future_end)
        if state:
            candidates.append({"match": match, "state": state, "api_index": api_index})
        else:
            kickoff = get_kickoff(match)
            if is_terminal(match):
                reason = "trạng thái kết thúc/hủy"
            elif kickoff is None:
                reason = "không có giờ bắt đầu và không LIVE"
            elif kickoff > future_end:
                reason = "bắt đầu sau ngày mai"
            else:
                reason = "quá hạn, API chưa xác nhận LIVE"
            key = (provider_key(match), sport_category(match), reason)
            filtered[key] += 1
            if len(filtered_examples.setdefault(key, [])) < 3:
                filtered_examples[key].append(
                    f"{first_text(match.get('id'))}({first_text(match.get('status_code'), match.get('status')) or '-'}, "
                    f"{format_match_time(kickoff) if kickoff else '-'}, live={is_explicit_live(match)})"
                )
    for (provider, sport, reason), amount in sorted(filtered.items()):
        print(f"[filter] {provider}/{sport}: {amount} trận {reason}; ví dụ {', '.join(filtered_examples[(provider, sport, reason)])}")

    resolved = resolve_all(candidates, json_source_map)
    if candidates and not resolved:
        raise RuntimeError("Không resolve được luồng nào; giữ playlist cũ")

    discovered = Counter((provider_key(m), sport_category(m)) for m in matches if isinstance(m, dict))
    eligible = Counter((provider_key(item["match"]), sport_category(item["match"])) for item in candidates)
    playable = Counter((provider_key(item["match"]), sport_category(item["match"])) for item in resolved)
    for key in sorted(discovered):
        print(f"[coverage] {key[0]}/{key[1]}: API={discovered[key]} eligible={eligible[key]} playable={playable[key]}")
    # Direct provider APIs only supplement sport.json. Verify that every
    # currently eligible worker fixture for an unavailable adapter still has
    # fresh playable sources before treating its failure as informational.
    for provider in sorted(data.get("_failed_provider_adapters", {})):
        sports = [sport for p, sport in eligible if p == provider]
        expected = sum(eligible[(provider, sport)] for sport in sports)
        actual = sum(playable[(provider, sport)] for sport in sports)
        level = "notice" if expected and actual == expected else "warning"
        print(f"::{level}::API riêng {provider} không truy cập được; "
              f"luồng dự phòng sport.json: {actual}/{expected} trận đủ điều kiện. "
              "Chưa xác nhận các môn/trận nằm ngoài sport.json.")
    resolved_ids = {item["api_index"] for item in resolved}
    no_link, unresolved = {}, {}
    for item in candidates:
        match = item["match"]
        key = (provider_key(match), sport_category(match))
        if item["api_index"] not in resolved_ids:
            event_id = first_text(match.get("id"), match_name(match))
            if (not declared_match_json_urls(match)
                    and not direct_sources({"sources": match.get("_provider_sources", [])})
                    and not first_text(match.get("_xoilac_detail"), match.get("_giovang_page"))):
                no_link.setdefault(key, []).append(event_id)
            else:
                unresolved.setdefault(key, []).append(event_id)
    for (provider, sport), event_ids in sorted(no_link.items()):
        print(f"[coverage] {provider}/{sport}: {len(event_ids)} trận hợp lệ nhưng API hiện không khai báo URL luồng/JSON; ví dụ {', '.join(event_ids[:4])}")
    for (provider, sport), event_ids in sorted(unresolved.items()):
        print(f"[coverage] {provider}/{sport}: {len(event_ids)} trận đã hỏi luồng chi tiết nhưng không có URL phát dùng được; ví dụ {', '.join(event_ids[:4])}")
    # Distinguish a failed HTTP/JSON resolver from an unfamiliar successful
    # JSON schema. Log field names and value shapes, never stream URLs/tokens.
    schema_examples = Counter()
    for item in candidates:
        match = item["match"]
        if item["api_index"] in resolved_ids:
            continue
        key = (provider_key(match), sport_category(match))
        if schema_examples[key] >= 2:
            continue
        roots = declared_match_json_urls(match)
        values = [resolver_bodies.get(url) for url in roots]
        successful = [body for body in values if isinstance(body, (dict, list))]
        if not roots:
            continue
        if not successful:
            reason = "JSON không trả về"
        else:
            body = successful[0]
            root = body if isinstance(body, dict) else {}
            keys = ", ".join(f"{name}:{type(value).__name__}" for name, value in list(root.items())[:12])
            nested = next((value for name in ("response", "data", "match")
                           if isinstance((value := root.get(name)), dict)), {})
            child_keys = ", ".join(list(nested)[:12]) if nested else ""
            reason = f"JSON có {len(body)} mục; root=[{keys}]" + (f"; data=[{child_keys}]" if child_keys else "")
        print(f"[schema] {first_text(match.get('id'))}: {reason}; luồng JSON={len(json_source_map.get(item['api_index'], []))}")
        schema_examples[key] += 1

    provider_sequence = provider_order(data, resolved)
    sport_order = build_sport_order(resolved)
    buckets = {key: [] for key, _ in provider_sequence}
    for item in resolved:
        buckets.setdefault(provider_key(item["match"]), []).append(item)
    for key in buckets:
        buckets[key].sort(key=lambda item: match_sort_key(item, sport_order))

    update_group = format_update_group(current)
    lines = ['#EXTM3U x-tvg-url=""']
    lines.append(f'#EXTINF:-1 tvg-id="playlist-update" tvg-name="{escape_attr(update_group)}" group-title="{escape_attr(update_group)}",{safe_title_text(update_group)}')
    lines.append("http://127.0.0.1/")
    total_streams = 0
    total_matches = 0

    for provider, group_name in provider_sequence:
        for item in buckets.get(provider, []):
            match = item["match"]
            sources = item["sources"]
            if not sources:
                continue
            total_matches += 1
            logo = first_text(match.get("home_logo"), match.get("away_logo"), match.get("logo"))
            for source in sources:
                title = build_title(match, item["state"], source)
                unique_id = stable_source_id(match, source)
                name = safe_title_text(match_name(match))
                lines.append(
                    f'#EXTINF:-1 tvg-id="{escape_attr(unique_id)}" tvg-name="{escape_attr(name)}" '
                    f'tvg-logo="{escape_attr(logo)}" group-title="{escape_attr(group_name)}",{title}'
                )
                lines.append(f"#EXTGRP:{safe_title_text(group_name)}")
                user_agent = header_value(source.get("headers"), "User-Agent")
                referer = header_value(source.get("headers"), "Referer")
                if user_agent:
                    lines.append(f"#EXTVLCOPT:http-user-agent={user_agent}")
                if referer:
                    lines.append(f"#EXTVLCOPT:http-referrer={referer}")
                lines.append(stream_url_with_headers(source))
                total_streams += 1

    temp = Path("playlist.m3u.tmp")
    temp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    temp.replace("playlist.m3u")
    print(f"{total_streams} luồng / {total_matches} trận / {len(provider_sequence)} nguồn")
    print(f"[timing] tổng thời gian build.py: {monotonic() - build_started:.1f}s")


if __name__ == "__main__":
    try:
        build_playlist()
    except Exception as exc:
        print(f"build.py: {exc}", file=sys.stderr)
        sys.exit(1)
