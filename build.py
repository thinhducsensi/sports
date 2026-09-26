import hashlib
import json
import math
import sys
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time, timedelta
from pathlib import Path
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

SPORT_API = "https://sport-stream-resolver.viet-ng228.workers.dev/sport.json"
TIME_ZONE = ZoneInfo("Asia/Ho_Chi_Minh")
RECENT_WINDOW_MS = 135 * 60 * 1000
MAX_WORKERS = 16
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

PREFERRED_PROVIDERS = [
    ("chuoichien", "Chuối Chiên"),
    ("colatv", "CoLaTV"),
    ("gavang33", "Gà Vàng 33"),
    ("giovang", "Giờ Vàng"),
    ("phalang", "PhaLangTV"),
    ("xoilacxth", "XoilacXTH"),
]

FOOTBALL_ALIASES = {"football", "soccer", "bong da", "bóng đá"}

SPORT_ICON_BY_NAME = {
    "football": "⚽", "soccer": "⚽", "futsal": "⚽",
    "basketball": "🏀", "volleyball": "🏐", "tennis": "🎾",
    "badminton": "🏸", "table tennis": "🏓", "billiards": "🎱",
    "billiard": "🎱", "snooker": "🎱", "pool": "🎱",
    "baseball": "⚾", "hockey": "🏒", "handball": "🤾",
    "rugby": "🏉", "cricket": "🏏", "golf": "⛳",
    "boxing": "🥊", "mma": "🥊", "motorsport": "🏁",
    "racing": "🏁", "esports": "🎮",
}

TERMINAL_STATUSES = {
    "ft", "finished", "finish", "ended", "end", "completed", "complete",
    "cancelled", "canceled", "postponed", "abandoned", "kết thúc", "ket thuc",
    "hủy", "huy",
}

LIVE_STATUSES = {
    "live", "playing", "inplay", "in-play", "in play", "1h", "2h", "ht",
    "halftime", "ongoing", "running",
}

KICKOFF_FIELDS = (
    "kickoff", "time_start", "start_time", "startTime", "start_timestamp",
    "startTimestamp", "timestamp",
)


def normalize_text(value):
    text = str(value or "").strip().lower().replace("_", " ").replace("-", " ")
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return " ".join(text.split())


def first_text(*values):
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def with_cache_buster(url):
    parts = urlsplit(str(url))
    query = parse_qsl(parts.query, keep_blank_values=True)
    query.append(("_fresh", str(int(datetime.now().timestamp() * 1000))))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def fetch_json(url, timeout=8, retries=2, cache_bust=False):
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


def parse_timestamp_ms(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        text = str(value).strip()
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
    for field in KICKOFF_FIELDS:
        timestamp = parse_timestamp_ms(match.get(field))
        if timestamp is not None:
            return timestamp
    return None


def format_match_time(timestamp_ms):
    if not timestamp_ms:
        return "[LIVE]"
    value = datetime.fromtimestamp(timestamp_ms / 1000, TIME_ZONE)
    return value.strftime("[%H:%M] %d/%m")


def format_update_group(timestamp_ms):
    value = datetime.fromtimestamp(timestamp_ms / 1000, TIME_ZONE)
    return value.strftime("🕒 Cập nhật %H:%M %d/%m")


def end_of_next_vietnam_day(timestamp_ms):
    current = datetime.fromtimestamp(timestamp_ms / 1000, TIME_ZONE)
    target = datetime.combine(current.date() + timedelta(days=1), time.max, tzinfo=TIME_ZONE)
    return int(target.timestamp() * 1000)


def status_values(match):
    values = set()
    for key in ("status_code", "status", "state", "phase"):
        value = match.get(key)
        if value not in (None, ""):
            values.add(str(value).strip().lower())
    return values


def is_terminal(match):
    return bool(status_values(match) & TERMINAL_STATUSES)


def is_explicit_live(match):
    return match.get("live") is True or bool(status_values(match) & LIVE_STATUSES)


def classify_match(match, current_ms, future_end_ms):
    if is_terminal(match):
        return None
    kickoff = get_kickoff(match)
    explicit_live = is_explicit_live(match)
    if kickoff is None:
        return {"kind": "live", "block": 0, "rank": 0, "kickoff": 0} if explicit_live else None
    if kickoff > current_ms:
        if kickoff <= future_end_ms:
            return {"kind": "upcoming", "block": 1, "rank": 2, "kickoff": kickoff}
        return None
    age = current_ms - kickoff
    if explicit_live:
        return {"kind": "live", "block": 0, "rank": 0, "kickoff": kickoff}
    if 0 <= age <= RECENT_WINDOW_MS:
        return {"kind": "recent", "block": 0, "rank": 1, "kickoff": kickoff}
    return None


def raw_sport_name(match):
    return first_text(match.get("sport"), match.get("sport_name"), match.get("sportType"), match.get("category"))


def sport_category(match):
    raw = normalize_text(raw_sport_name(match))
    aliases = {normalize_text(value) for value in FOOTBALL_ALIASES}
    if raw in aliases or raw in {"association football", "men football", "women football"}:
        return "football"
    return raw or "other"


def sport_icon(match):
    direct = first_text(match.get("sport_icon"), match.get("sportIcon"), match.get("icon"), match.get("emoji"))
    if direct and len(direct) <= 8:
        return direct
    category = sport_category(match)
    if category in SPORT_ICON_BY_NAME:
        return SPORT_ICON_BY_NAME[category]
    for name, icon in SPORT_ICON_BY_NAME.items():
        if category.startswith(name + " ") or category.endswith(" " + name):
            return icon
    return "🏅"


def provider_key(match):
    return normalize_text(first_text(match.get("provider"), match.get("source"), match.get("provider_key"))) or "other"


def provider_display_name(match):
    key = provider_key(match)
    preferred = {normalize_text(k): v for k, v in PREFERRED_PROVIDERS}
    if key in preferred:
        return preferred[key]
    return first_text(match.get("provider_name"), match.get("source_name"), match.get("provider"), match.get("source"), "Khác")


def build_provider_order(items):
    discovered = {}
    for index, item in enumerate(items):
        key = provider_key(item["match"])
        if key not in discovered:
            discovered[key] = {"first_seen": index, "name": provider_display_name(item["match"])}
    order = []
    preferred_keys = [normalize_text(key) for key, _ in PREFERRED_PROVIDERS]
    for key in preferred_keys:
        if key in discovered:
            order.append((key, discovered[key]["name"]))
    unknown = [key for key in discovered if key not in preferred_keys]
    unknown.sort(key=lambda key: (discovered[key]["first_seen"], discovered[key]["name"].lower()))
    order.extend((key, discovered[key]["name"]) for key in unknown)
    return order


def build_sport_order(items):
    discovered = {}
    for index, item in enumerate(items):
        category = sport_category(item["match"])
        kickoff = item["state"].get("kickoff") or get_kickoff(item["match"]) or 0
        if category not in discovered:
            discovered[category] = {"first_seen": index, "earliest": kickoff if kickoff > 0 else 2**63 - 1}
        elif kickoff > 0:
            discovered[category]["earliest"] = min(discovered[category]["earliest"], kickoff)
    order = {}
    position = 0
    if "football" in discovered:
        order["football"] = position
        position += 1
    others = [category for category in discovered if category != "football"]
    others.sort(key=lambda category: (discovered[category]["earliest"], discovered[category]["first_seen"], category))
    for category in others:
        order[category] = position
        position += 1
    return order


def match_name(match):
    return first_text(match.get("name"), match.get("match_name"), match.get("matchName"), match.get("title"))


def competition_name(match):
    return first_text(
        match.get("competition"), match.get("league"), match.get("tournament"),
        match.get("championship"), match.get("competition_name"), match.get("league_name"),
    )


def match_identity(match):
    key = provider_key(match)
    match_id = first_text(match.get("id"), match.get("match_id"), match.get("matchId"))
    if match_id:
        return f"{key}|id:{match_id}"
    kickoff = get_kickoff(match) or 0
    return f"{key}|{sport_category(match)}|{kickoff}|{normalize_text(match_name(match))}"


def match_sort_key(item, sport_order):
    match = item["match"]
    state = item["state"]
    category = sport_category(match)
    kickoff = state.get("kickoff") or get_kickoff(match) or 0
    untimed_live = 0 if kickoff == 0 and state.get("block") == 0 else 1
    return (
        state.get("block", 9),
        sport_order.get(category, 10**9),
        untimed_live,
        kickoff if kickoff > 0 else 0,
        state.get("rank", 9),
        normalize_text(competition_name(match)),
        normalize_text(match_name(match)),
    )


def escape_attr(value):
    return str(value or "").replace("&", "&amp;").replace('"', "&quot;")


def normalize_headers(headers):
    result = {}
    if isinstance(headers, dict):
        for key, value in headers.items():
            if value is None:
                continue
            text = str(value).strip()
            if text:
                result[str(key)] = text
    return result


def source_headers(raw, fallback=None):
    headers = normalize_headers(fallback)
    if isinstance(raw, dict):
        headers.update(normalize_headers(raw.get("headers")))
        aliases = {
            "Referer": first_text(raw.get("referer"), raw.get("referrer"), raw.get("http_referrer")),
            "Origin": first_text(raw.get("origin"), raw.get("http_origin")),
            "User-Agent": first_text(raw.get("user_agent"), raw.get("userAgent"), raw.get("ua")),
            "Cookie": first_text(raw.get("cookie"), raw.get("cookies")),
            "Authorization": first_text(raw.get("authorization"), raw.get("auth")),
        }
        for key, value in aliases.items():
            if value and not any(existing.lower() == key.lower() for existing in headers):
                headers[key] = value
    return headers


def infer_format(source):
    direct = first_text(source.get("format"), source.get("type"), source.get("protocol"), source.get("extension"), source.get("ext")).upper()
    if direct and direct not in {"STREAM", "VIDEO", "URL", "SOURCE"}:
        return direct
    url = str(source.get("url") or "").lower().split("?", 1)[0]
    if ".m3u8" in url:
        return "HLS"
    if ".mpd" in url:
        return "DASH"
    if ".ts" in url:
        return "TS"
    if ".flv" in url:
        return "FLV"
    if ".mp4" in url:
        return "MP4"
    return ""


def normalize_source(raw, fallback_headers=None):
    if not raw:
        return None
    if isinstance(raw, str):
        url = raw.strip()
        return {"url": url, "headers": source_headers({}, fallback_headers)} if url else None
    if not isinstance(raw, dict):
        return None
    url = first_text(
        raw.get("url"), raw.get("link"), raw.get("src"), raw.get("stream_url"), raw.get("streamUrl"),
        raw.get("play_url"), raw.get("playUrl"), raw.get("file"),
    )
    if not url:
        return None
    result = dict(raw)
    result["url"] = url
    result["headers"] = source_headers(raw, fallback_headers)
    return result


def explicit_source_commentator(source):
    return first_text(
        source.get("commentator"),
        source.get("blv"),
        source.get("caster"),
        source.get("audio_name"),
        source.get("audioName"),
    )


def match_commentator(match):
    return first_text(match.get("commentator"), match.get("blv"), match.get("caster"))


def is_generic_source_label(value, source, match):
    label = normalize_text(value)
    if not label:
        return True
    generic_exact = {
        "stream", "source", "sport stream", "sports stream", "main", "primary", "backup",
        "hls", "dash", "flv", "ts", "mp4", "hd", "fhd", "uhd", "4k", "sd",
        "auto", "default", "server", "link", "live",
    }
    if label in generic_exact:
        return True
    generic_prefixes = ("server ", "stream ", "source ", "link ", "backup ", "luong ", "luồng ")
    if label.startswith(generic_prefixes):
        return True
    provider_names = {
        normalize_text(match.get("provider")),
        normalize_text(match.get("provider_name")),
        normalize_text(source.get("provider")),
    }
    provider_names.discard("")
    if label in provider_names:
        return True
    quality = normalize_text(first_text(source.get("quality"), source.get("resolution"), source.get("video_quality"), source.get("videoQuality")))
    fmt = normalize_text(infer_format(source))
    if label in {quality, fmt} - {""}:
        return True
    if label == normalize_text(match_name(match)):
        return True
    return False


def source_commentator(source, match):
    explicit = explicit_source_commentator(source)
    if explicit:
        return explicit
    for key in ("name", "label", "title", "server"):
        candidate = first_text(source.get(key))
        if candidate and not is_generic_source_label(candidate, source, match):
            return candidate
    return match_commentator(match)


def source_key(source):
    headers = sorted((str(key).lower().strip(), str(value).strip()) for key, value in (source.get("headers") or {}).items())
    header_text = "\n".join(f"{key}:{value}" for key, value in headers)
    return f"{str(source.get('url') or '').strip()}\n{header_text}"


def merge_duplicate_source(existing, incoming):
    merged = dict(existing)
    for key in (
        "commentator", "blv", "caster", "audio_name", "audioName",
        "name", "label", "title", "server",
        "quality", "resolution", "video_quality", "videoQuality", "format", "type",
    ):
        if not merged.get(key) and incoming.get(key):
            merged[key] = incoming.get(key)
    return merged


def quality_rank(source):
    quality = normalize_text(first_text(source.get("quality"), source.get("resolution"), source.get("video_quality"), source.get("videoQuality")))
    if "4k" in quality or "2160" in quality:
        return 0
    if "fhd" in quality or "1080" in quality:
        return 1
    if quality == "hd" or "720" in quality:
        return 2
    if "540" in quality:
        return 3
    if "sd" in quality or "480" in quality or "360" in quality:
        return 4
    return 5


def source_priority(source):
    fmt = infer_format(source)
    format_rank = {"HLS": 0, "TS": 1, "DASH": 2, "FLV": 3, "MP4": 4}.get(fmt, 5)
    return (format_rank, quality_rank(source), normalize_text(explicit_source_commentator(source)), source.get("url", ""))


def dedupe_sources(sources):
    merged = {}
    for source in sources:
        if not source or not source.get("url"):
            continue
        key = source_key(source)
        merged[key] = merge_duplicate_source(merged[key], source) if key in merged else source
    return sorted(merged.values(), key=source_priority)


def extract_sources(body, match):
    body = body if isinstance(body, dict) else {}
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    fallback_headers = {}
    fallback_headers.update(normalize_headers(match.get("headers")))
    fallback_headers.update(normalize_headers(body.get("headers")))
    arrays = [
        body.get("sources"), body.get("streams"), body.get("links"), body.get("urls"),
        data.get("sources"), data.get("streams"), data.get("links"), data.get("urls"),
        match.get("sources"), match.get("streams"), match.get("links"), match.get("urls"),
    ]
    sources = []
    for array in arrays:
        if isinstance(array, list):
            for raw in array:
                source = normalize_source(raw, fallback_headers)
                if source:
                    sources.append(source)
    for raw in (body.get("source"), data.get("source"), body, data, match.get("source"), match):
        source = normalize_source(raw, fallback_headers)
        if source:
            sources.append(source)
    return dedupe_sources(sources)


def looks_like_stream_url(url):
    path = str(url or "").lower().split("?", 1)[0]
    return any(ext in path for ext in (".m3u8", ".mpd", ".ts", ".flv", ".mp4"))


def resolve_match(match):
    direct_sources = extract_sources({}, match)
    resolver = first_text(match.get("resolver"), match.get("resolve_url"), match.get("resolver_url"))
    all_sources = list(direct_sources)
    if resolver:
        body = fetch_json(resolver, timeout=8, retries=2, cache_bust=False)
        if body is not None:
            all_sources.extend(extract_sources(body, match))
        elif looks_like_stream_url(resolver):
            direct = normalize_source({"url": resolver}, match.get("headers"))
            if direct:
                all_sources.append(direct)
    sources = dedupe_sources(all_sources)
    return {"match": match, "sources": sources} if sources else None


def merge_match_items(items):
    merged = {}
    for item in items:
        key = match_identity(item["match"])
        if key not in merged:
            merged[key] = {"match": dict(item["match"]), "state": dict(item["state"]), "sources": list(item["sources"])}
            continue
        current = merged[key]
        current["sources"] = dedupe_sources(current["sources"] + item["sources"])
        if item["state"].get("rank", 9) < current["state"].get("rank", 9):
            current["state"] = dict(item["state"])
        for field in ("home_logo", "away_logo", "competition", "league", "tournament", "commentator", "blv", "caster", "sport_icon"):
            if not current["match"].get(field) and item["match"].get(field):
                current["match"][field] = item["match"][field]
    return list(merged.values())


def header_value(headers, name):
    wanted = name.lower()
    for key, value in (headers or {}).items():
        if str(key).lower() == wanted:
            return str(value or "")
    return ""


def source_meta(source, match):
    commentator = source_commentator(source, match)
    quality = first_text(source.get("quality"), source.get("resolution"), source.get("video_quality"), source.get("videoQuality"))
    fmt = infer_format(source)
    stream_parts = []
    if quality:
        stream_parts.append(quality)
    if fmt and normalize_text(fmt) != normalize_text(quality):
        stream_parts.append(fmt)
    return {"commentator": commentator, "stream_info": " • ".join(stream_parts)}


def encode_component(value):
    return quote(str(value), safe="-_.!~*'()")


def stream_url_with_headers(source):
    blocked = {"host", "content-length", "transfer-encoding", "connection"}
    preferred = ["User-Agent", "Referer", "Origin", "Cookie", "Authorization"]
    headers = source.get("headers") or {}
    ordered = []
    used = set()
    for wanted in preferred:
        for key, value in headers.items():
            if key.lower() == wanted.lower() and key.lower() not in used:
                ordered.append((key, value))
                used.add(key.lower())
    for key, value in sorted(headers.items(), key=lambda item: item[0].lower()):
        lower = key.lower()
        if lower not in used and lower not in blocked:
            ordered.append((key, value))
            used.add(lower)
    params = [f"{encode_component(name)}={encode_component(value)}" for name, value in ordered if name.lower() not in blocked]
    if not params:
        return source["url"]
    separator = "&" if "|" in source["url"] else "|"
    return f"{source['url']}{separator}{'&'.join(params)}"


def stable_source_id(match, source):
    base = f"{match_identity(match)}\n{source_key(source)}".encode("utf-8")
    digest = hashlib.sha1(base).hexdigest()[:12]
    return f"sport-{digest}"


def build_title(match, state, source):
    parts = []
    if state.get("block") == 0:
        parts.append("🔴")
    time_part = format_match_time(get_kickoff(match))
    icon = sport_icon(match)
    name = match_name(match)
    head = " ".join(part for part in (" ".join(parts), time_part, icon, name) if part)
    meta = source_meta(source, match)
    if meta["commentator"]:
        head += f" (BLV {meta['commentator']})"
    competition = competition_name(match)
    if competition:
        head += f" • {competition}"
    if meta["stream_info"]:
        head += f" [{meta['stream_info']}]"
    return head.strip()


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
    if candidates:
        workers = min(MAX_WORKERS, len(candidates))
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

    resolved = merge_match_items(resolved)
    if candidates and not resolved:
        raise RuntimeError("Có trận hợp lệ nhưng không resolve được luồng nào; giữ playlist cũ")

    provider_order = build_provider_order(resolved)
    sport_order = build_sport_order(resolved)
    buckets = {key: [] for key, _ in provider_order}
    for item in resolved:
        buckets.setdefault(provider_key(item["match"]), []).append(item)
    for key in buckets:
        buckets[key].sort(key=lambda item: match_sort_key(item, sport_order))

    update_group = format_update_group(current_ms)
    lines = ['#EXTM3U x-tvg-url=""']
    lines.append(
        f'#EXTINF:-1 tvg-id="playlist-update" tvg-name="{escape_attr(update_group)}" '
        f'group-title="{escape_attr(update_group)}",{update_group}'
    )
    lines.append("http://127.0.0.1/")
    total_streams = 0
    total_matches = 0

    for provider, group_name in provider_order:
        for item in buckets.get(provider, []):
            match = item["match"]
            state = item["state"]
            sources = item["sources"]
            if not sources:
                continue
            name = match_name(match)
            if not name:
                continue
            total_matches += 1
            logo = first_text(match.get("home_logo"), match.get("away_logo"), match.get("logo"))
            for source in sources:
                title = build_title(match, state, source)
                unique_id = stable_source_id(match, source)
                referer = header_value(source.get("headers"), "Referer")
                origin = header_value(source.get("headers"), "Origin")
                user_agent = header_value(source.get("headers"), "User-Agent")
                lines.append(
                    f'#EXTINF:-1 tvg-id="{escape_attr(unique_id)}" tvg-name="{escape_attr(name)}" '
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

    output = "\n".join(lines) + "\n"
    temp = Path("playlist.m3u.tmp")
    temp.write_text(output, encoding="utf-8")
    temp.replace("playlist.m3u")
    print(f"Đã xuất {total_streams} luồng từ {total_matches} trận. {update_group}")


if __name__ == "__main__":
    try:
        build_playlist()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
