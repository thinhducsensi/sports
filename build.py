import hashlib
import json
import sys
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time, timedelta
from time import sleep
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

SPORT_API = "https://sport-stream-resolver.viet-ng228.workers.dev/sport.json"
TIME_ZONE = ZoneInfo("Asia/Ho_Chi_Minh")
MATCH_DURATION_MS = 135 * 60 * 1000
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
MAX_WORKERS = 32

ORDERED_PROVIDERS = [
    {"key": "chuoichien", "name": "Chuối Chiên"},
    {"key": "colatv", "name": "CoLaTV"},
    {"key": "gavang33", "name": "Gà Vàng 33"},
    {"key": "giovang", "name": "Giờ Vàng"},
    {"key": "phalang", "name": "PhaLangTV"},
    {"key": "xoilacxth", "name": "XoilacXTH"},
]

FOOTBALL_ALIASES = {"football", "soccer", "bong da"}

SPORT_ICON_BY_NAME = {
    "football": "⚽",
    "futsal": "⚽",
    "basketball": "🏀",
    "volleyball": "🏐",
    "tennis": "🎾",
    "badminton": "🏸",
    "table tennis": "🏓",
    "billiards": "🎱",
    "snooker": "🎱",
    "pool": "🎱",
    "baseball": "⚾",
    "hockey": "🏒",
    "handball": "🤾",
    "rugby": "🏉",
    "cricket": "🏏",
    "golf": "⛳",
    "boxing": "🥊",
    "mma": "🥊",
    "motorsport": "🏁",
    "racing": "🏁",
    "esports": "🎮",
}

FINISHED_STATUSES = {
    "ft",
    "finished",
    "finish",
    "ended",
    "end",
    "completed",
    "complete",
    "kết thúc",
    "ket thuc",
}

LIVE_STATUSES = {
    "live",
    "playing",
    "inplay",
    "in-play",
    "in play",
    "1h",
    "2h",
    "ht",
    "halftime",
    "ongoing",
}



def with_cache_buster(url):
    parts = urlsplit(str(url))
    query = parse_qsl(parts.query, keep_blank_values=True)
    query.append(("_fresh", str(int(datetime.now().timestamp() * 1000))))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def fetch_json(url, timeout=7, retries=2, cache_bust=False):
    target = with_cache_buster(url) if cache_bust else str(url)
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,*/*",
        "Cache-Control": "no-cache, no-store, max-age=0",
        "Pragma": "no-cache",
    }
    for attempt in range(max(1, retries)):
        request = Request(target, headers=headers)
        try:
            with urlopen(request, timeout=timeout) as response:
                status = getattr(response, "status", 200)
                if not 200 <= status < 300:
                    raise HTTPError(target, status, "HTTP error", response.headers, None)
                return json.loads(response.read().decode("utf-8-sig"))
        except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError, UnicodeDecodeError):
            if attempt + 1 < max(1, retries):
                sleep(1.5 * (attempt + 1))
    return None


def now_ms():
    return int(datetime.now().timestamp() * 1000)


def format_match_time(timestamp_ms):
    if not timestamp_ms:
        return "[--:--] --/--"
    value = datetime.fromtimestamp(timestamp_ms / 1000, TIME_ZONE)
    return value.strftime("[%H:%M] %d/%m")


def format_update_group(timestamp_ms):
    value = datetime.fromtimestamp(timestamp_ms / 1000, TIME_ZONE)
    return value.strftime("🕒 Cập nhật %H:%M %d/%m")


def end_of_next_vietnam_day(timestamp_ms):
    current = datetime.fromtimestamp(timestamp_ms / 1000, TIME_ZONE)
    target = datetime.combine(current.date() + timedelta(days=1), time.max, tzinfo=TIME_ZONE)
    return int(target.timestamp() * 1000)


def get_kickoff(match):
    value = match.get("kickoff")
    if value in (None, ""):
        value = match.get("time_start")
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    if number < 100_000_000_000:
        number *= 1000
    return int(number)


def normalize_text(value):
    text = str(value or "").strip().lower().replace("_", " ").replace("-", " ")
    text = unicodedata.normalize("NFD", text)
    text = "".join(char for char in text if unicodedata.category(char) != "Mn")
    return " ".join(text.split())


def status_values(match):
    values = set()
    for key in ("status_code", "status", "state", "phase"):
        value = match.get(key)
        if value not in (None, ""):
            values.add(str(value).strip().lower())
    return values


def is_finished(match):
    return bool(status_values(match) & FINISHED_STATUSES)


def is_explicit_live(match):
    return match.get("live") is True or bool(status_values(match) & LIVE_STATUSES)


def classify_match(match, current_ms, future_end_ms):
    if is_finished(match):
        return None
    kickoff = get_kickoff(match)
    if kickoff is None:
        return None
    if kickoff > current_ms:
        if kickoff <= future_end_ms:
            return {"rank": 2, "kickoff": kickoff}
        return None
    age = current_ms - kickoff
    if age < 0 or age > MATCH_DURATION_MS:
        return None
    return {"rank": 0 if is_explicit_live(match) else 1, "kickoff": kickoff}


def sport_category(match):
    raw_sport = normalize_text(match.get("sport"))
    raw_name = normalize_text(match.get("sport_name"))
    values = [value for value in (raw_sport, raw_name) if value]
    for value in values:
        if value in FOOTBALL_ALIASES or any(alias in value.split() for alias in ("football", "soccer")):
            return "football"
    return values[0] if values else "other"


def sport_icon(match):
    category = sport_category(match)
    if category in SPORT_ICON_BY_NAME:
        return SPORT_ICON_BY_NAME[category]
    for name, icon in SPORT_ICON_BY_NAME.items():
        if category == name or category.startswith(name + " ") or category.endswith(" " + name):
            return icon
    return "🏅"


def build_sport_order(items):
    discovered = {}
    for index, item in enumerate(items):
        category = sport_category(item["match"])
        kickoff = item["state"].get("kickoff") or get_kickoff(item["match"]) or 0
        if category not in discovered:
            discovered[category] = {"first_seen": index, "earliest": kickoff if kickoff > 0 else 2**63 - 1}
        elif kickoff > 0:
            discovered[category]["earliest"] = min(discovered[category]["earliest"], kickoff)
    others = [category for category in discovered if category != "football"]
    others.sort(key=lambda category: (discovered[category]["earliest"], discovered[category]["first_seen"], category))
    order = {"football": 0}
    for index, category in enumerate(others, start=1):
        order[category] = index
    return order


def match_sort_key(item, sport_order):
    match = item["match"]
    state = item["state"]
    category = sport_category(match)
    kickoff = state.get("kickoff") or get_kickoff(match) or 0
    missing_time_rank = 0 if kickoff == 0 and state.get("rank") == 0 else 1
    time_key = kickoff if kickoff > 0 else 0
    return (
        sport_order.get(category, 10**9),
        missing_time_rank,
        time_key,
        state.get("rank", 9),
        normalize_text(match.get("competition") or match.get("league")),
        normalize_text(match.get("name")),
    )


def escape_attr(value):
    return str(value or "").replace("&", "&amp;").replace('"', "&quot;")


def first_text(*values):
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def header_value(headers, name):
    if not isinstance(headers, dict):
        return ""
    wanted = name.lower()
    for key, value in headers.items():
        if str(key).lower() == wanted:
            return str(value or "")
    return ""


def normalize_headers(headers):
    if not isinstance(headers, dict):
        return {}
    result = {}
    for key, value in headers.items():
        if value is None:
            continue
        text = str(value).strip()
        if text:
            result[str(key)] = str(value)
    return result


def infer_format(source):
    direct = first_text(
        source.get("format"),
        source.get("type"),
        source.get("protocol"),
        source.get("extension"),
        source.get("ext"),
    ).upper()
    if direct and direct not in {"STREAM", "VIDEO", "URL"}:
        return direct
    url = str(source.get("url") or "").lower().split("?", 1)[0]
    if ".m3u8" in url:
        return "HLS"
    if ".flv" in url:
        return "FLV"
    if ".mpd" in url:
        return "DASH"
    if ".ts" in url:
        return "TS"
    if ".mp4" in url:
        return "MP4"
    return ""


def normalize_source(raw, fallback_headers=None):
    fallback_headers = fallback_headers or {}
    if not raw:
        return None
    if isinstance(raw, str):
        url = raw.strip()
        return {"url": url, "headers": normalize_headers(fallback_headers)} if url else None
    if not isinstance(raw, dict):
        return None
    url = first_text(
        raw.get("url"),
        raw.get("link"),
        raw.get("src"),
        raw.get("stream_url"),
        raw.get("streamUrl"),
        raw.get("play_url"),
        raw.get("playUrl"),
    )
    if not url:
        return None
    result = dict(raw)
    result["url"] = url
    own_headers = raw.get("headers")
    result["headers"] = normalize_headers(own_headers if isinstance(own_headers, dict) else fallback_headers)
    return result


def commentator_of(source):
    return first_text(
        source.get("commentator"),
        source.get("blv"),
        source.get("caster"),
        source.get("audio_name"),
        source.get("audioName"),
    )


def source_key(source):
    headers = sorted(
        (str(key).lower().strip(), str(value).strip())
        for key, value in (source.get("headers") or {}).items()
    )
    header_text = "\n".join(f"{key}:{value}" for key, value in headers)
    return f"{str(source.get('url') or '').strip()}\n{header_text}"


def merge_duplicate(existing, incoming):
    names = []
    seen = set()
    values = list(existing.get("commentators") or []) + [commentator_of(existing), commentator_of(incoming)]
    for value in values:
        if value and value not in seen:
            seen.add(value)
            names.append(value)
    merged = dict(existing)
    for key in ("commentator", "blv", "name", "label", "server", "quality", "resolution", "format"):
        if not merged.get(key) and incoming.get(key):
            merged[key] = incoming.get(key)
    merged["commentators"] = names
    return merged


def source_priority(source):
    fmt = infer_format(source)
    return {"HLS": 0, "TS": 1, "DASH": 2, "FLV": 3, "MP4": 4}.get(fmt, 5)


def extract_sources(body, match):
    body = body if isinstance(body, dict) else {}
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    arrays = [
        body.get("sources"),
        body.get("streams"),
        body.get("links"),
        data.get("sources"),
        data.get("streams"),
        data.get("links"),
        match.get("sources"),
        match.get("streams"),
        match.get("links"),
    ]
    fallback_headers = body.get("headers") if isinstance(body.get("headers"), dict) else match.get("headers") or {}
    merged = {}

    def add(raw):
        source = normalize_source(raw, fallback_headers)
        if not source:
            return
        key = source_key(source)
        merged[key] = merge_duplicate(merged[key], source) if key in merged else source

    for array in arrays:
        if isinstance(array, list):
            for raw in array:
                add(raw)
    add(body.get("source") or data.get("source") or match.get("source"))
    return sorted(merged.values(), key=source_priority)


def source_meta(source, match, index, count):
    names = []
    seen = set()
    for value in list(source.get("commentators") or []) + [commentator_of(source)]:
        if value and value not in seen:
            seen.add(value)
            names.append(value)
    match_commentator = first_text(match.get("commentator"), match.get("blv"))
    if not names and match_commentator:
        names.append(match_commentator)
    fmt = infer_format(source)
    quality = first_text(
        source.get("quality"),
        source.get("resolution"),
        source.get("video_quality"),
        source.get("videoQuality"),
    )
    parts = []
    if fmt:
        parts.append(fmt)
    if quality:
        parts.append(quality)
    if count > 1:
        parts.append(f"{index + 1}/{count}")
    return {"commentator": "/".join(names), "stream_info": " • ".join(parts)}


def encode_component(value):
    return quote(str(value), safe="-_.!~*'()")


def stream_url_with_headers(source):
    blocked = {"host", "content-length", "transfer-encoding", "connection"}
    params = []
    for name, value in (source.get("headers") or {}).items():
        if str(name).lower() in blocked:
            continue
        params.append(f"{encode_component(name)}={encode_component(value)}")
    if not params:
        return source["url"]
    separator = "&" if "|" in source["url"] else "|"
    return f"{source['url']}{separator}{'&'.join(params)}"


def stable_source_id(match, source):
    match_id = match.get("id") or "match"
    base_name = match.get("id") or match.get("name") or "match"
    raw = f"{base_name}\n{source_key(source)}".encode("utf-8")
    digest = hashlib.sha1(raw).hexdigest()[:10]
    return f"{match_id}-{digest}"


def resolve_match(match):
    direct_sources = extract_sources({}, match)
    resolver = match.get("resolver")
    if not resolver:
        return {"match": match, "sources": direct_sources} if direct_sources else None
    body = fetch_json(str(resolver), timeout=7, retries=2, cache_bust=False)
    if body is not None:
        sources = extract_sources(body, match)
        if sources:
            return {"match": match, "sources": sources}
    return {"match": match, "sources": direct_sources} if direct_sources else None


def build_playlist():
    data = fetch_json(SPORT_API, timeout=10, retries=3, cache_bust=True)
    if not isinstance(data, dict):
        raise RuntimeError("Không lấy được sport.json")
    matches = data.get("matches") if isinstance(data.get("matches"), list) else []
    current_ms = now_ms()
    future_end_ms = end_of_next_vietnam_day(current_ms)
    candidates = []
    for match in matches:
        if not isinstance(match, dict):
            continue
        state = classify_match(match, current_ms, future_end_ms)
        if state:
            candidates.append({"match": match, "state": state})

    resolved = []
    workers = min(MAX_WORKERS, max(1, len(candidates)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(resolve_match, item["match"]): item for item in candidates}
        for future in as_completed(futures):
            item = futures[future]
            try:
                value = future.result()
            except Exception:
                value = None
            if value:
                value["state"] = item["state"]
                resolved.append(value)

    buckets = {provider["key"]: [] for provider in ORDERED_PROVIDERS}
    for item in resolved:
        key = item["match"].get("provider") or ""
        if key in buckets:
            buckets[key].append(item)

    sport_order = build_sport_order(candidates)
    for provider in ORDERED_PROVIDERS:
        buckets[provider["key"]].sort(key=lambda item: match_sort_key(item, sport_order))

    update_group = format_update_group(current_ms)
    lines = ['#EXTM3U x-tvg-url=""']
    lines.append(
        f'#EXTINF:-1 tvg-id="playlist-update" tvg-name="{escape_attr(update_group)}" '
        f'group-title="{escape_attr(update_group)}",{update_group}'
    )
    lines.append("http://127.0.0.1/")
    total_streams = 0
    total_matches = 0

    for provider in ORDERED_PROVIDERS:
        group_name = provider["name"]
        for item in buckets[provider["key"]]:
            match = item["match"]
            sources = item["sources"]
            state = item["state"]
            if not sources:
                continue
            total_matches += 1
            kickoff = get_kickoff(match)
            live_dot = "🔴 " if state["rank"] in (0, 1) else ""
            competition = match.get("competition") or match.get("league") or match.get("sport_name") or "Thể thao"
            logo = match.get("home_logo") or match.get("away_logo") or ""

            for index, source in enumerate(sources):
                if not source.get("url"):
                    continue
                meta = source_meta(source, match, index, len(sources))
                blv_part = f" • BLV {meta['commentator']}" if meta["commentator"] else ""
                competition_part = f" • {competition}" if competition else ""
                stream_part = f" [{meta['stream_info']}]" if meta["stream_info"] else ""
                title = (
                    f"{live_dot}{format_match_time(kickoff)} {sport_icon(match)} {match.get('name') or ''}"
                    f"{blv_part}{competition_part}{stream_part}"
                )
                unique_id = stable_source_id(match, source)
                referer = header_value(source.get("headers"), "Referer")
                origin = header_value(source.get("headers"), "Origin")
                user_agent = header_value(source.get("headers"), "User-Agent")
                lines.append(
                    f'#EXTINF:-1 tvg-id="{escape_attr(unique_id)}" tvg-name="{escape_attr(match.get("name") or "")}" '
                    f'tvg-logo="{escape_attr(logo)}" group-title="{escape_attr(group_name)}",{title}'
                )
                lines.append(f"#EXTGRP:{group_name}")
                if referer:
                    lines.append(f"#EXTVLCOPT:http-referrer={referer}")
                if origin:
                    lines.append(f"#EXTVLCOPT:http-origin={origin}")
                if user_agent:
                    lines.append(f"#EXTVLCOPT:http-user-agent={user_agent}")
                lines.append(stream_url_with_headers(source))
                total_streams += 1

    Path("playlist.m3u").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Đã xuất {total_streams} luồng từ {total_matches} trận. {update_group}")


if __name__ == "__main__":
    try:
        build_playlist()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
