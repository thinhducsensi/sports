import hashlib
import json
import re
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

SPORTS = [
    ("football", "⚽", {"football", "soccer", "bong da"}),
    ("futsal", "⚽", {"futsal"}),
    ("basketball", "🏀", {"basketball", "bong ro"}),
    ("volleyball", "🏐", {"volleyball", "bong chuyen"}),
    ("tennis", "🎾", {"tennis", "quan vot"}),
    ("badminton", "🏸", {"badminton", "cau long"}),
    ("table_tennis", "🏓", {"table tennis", "table_tennis", "ping pong", "bong ban"}),
    ("billiards", "🎱", {"billiards", "billiard", "pool", "snooker", "bida", "bi da"}),
    ("baseball", "⚾", {"baseball", "bong chay"}),
    ("hockey", "🏒", {"hockey", "ice hockey", "ice_hockey"}),
    ("handball", "🤾", {"handball", "bong nem"}),
    ("rugby", "🏉", {"rugby"}),
    ("cricket", "🏏", {"cricket"}),
    ("golf", "⛳", {"golf"}),
    ("boxing", "🥊", {"boxing", "mma", "ufc", "kickboxing", "muay thai"}),
    ("motorsport", "🏁", {"motorsport", "racing", "formula 1", "f1", "moto gp", "motogp"}),
    ("esports", "🎮", {"esports", "e-sports", "e sports"}),
]

SPORT_ORDER = {name: index for index, (name, _, _) in enumerate(SPORTS)}
SPORT_ICON = {name: icon for name, icon, _ in SPORTS}
SPORT_ALIAS = {alias: name for name, _, aliases in SPORTS for alias in aliases}

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

LIVE_MAX_AGE_MINUTES = {
    "football": 180,
    "futsal": 180,
    "basketball": 240,
    "volleyball": 360,
    "tennis": 720,
    "badminton": 480,
    "table_tennis": 480,
    "billiards": 720,
    "baseball": 480,
    "hockey": 240,
    "handball": 240,
    "rugby": 300,
    "cricket": 1080,
    "golf": 1080,
    "boxing": 480,
    "motorsport": 720,
    "esports": 720,
    "other": 720,
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


def live_max_age_ms(match):
    category = sport_category(match)
    minutes = LIVE_MAX_AGE_MINUTES.get(category, LIVE_MAX_AGE_MINUTES["other"])
    return minutes * 60 * 1000


def classify_match(match, current_ms, future_end_ms):
    if is_finished(match):
        return None
    kickoff = get_kickoff(match)
    live_flag = is_explicit_live(match)
    if kickoff is None:
        return {"rank": 0, "kickoff": 0} if live_flag else None
    if kickoff > current_ms:
        if kickoff <= future_end_ms:
            return {"rank": 2, "kickoff": kickoff}
        return None
    age = current_ms - kickoff
    if live_flag and 0 <= age <= live_max_age_ms(match):
        return {"rank": 0, "kickoff": kickoff}
    if 0 <= age <= MATCH_DURATION_MS:
        return {"rank": 1, "kickoff": kickoff}
    return None


def sport_category(match):
    raw_values = [match.get("sport"), match.get("sport_name")]
    normalized = [normalize_text(value) for value in raw_values if value not in (None, "")]
    for value in normalized:
        if value in SPORT_ALIAS:
            return SPORT_ALIAS[value]
    for value in normalized:
        for alias, category in SPORT_ALIAS.items():
            if alias and (value.startswith(alias + " ") or value.endswith(" " + alias)):
                return category
    return normalized[0] if normalized else "other"


def sport_icon(match):
    return SPORT_ICON.get(sport_category(match), "🏅")


def match_sort_key(item):
    match = item["match"]
    state = item["state"]
    category = sport_category(match)
    category_rank = SPORT_ORDER.get(category, len(SPORT_ORDER))
    state_rank = state["rank"]
    kickoff = state.get("kickoff") or 0
    if state_rank in (0, 1):
        time_key = -kickoff
    else:
        time_key = kickoff
    return (
        category_rank,
        category,
        state_rank,
        time_key,
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


def xoilac_label_commentator(source, match):
    """Return a caster only when a XoiLac per-stream label really contains a human label.

    Generic source labels such as "Xoilac Nguồn 1 FLV" must never become a commentator.
    """
    raw = first_text(
        source.get("label"),
        source.get("button"),
        source.get("button_text"),
        source.get("buttonText"),
        source.get("display_name"),
        source.get("displayName"),
        source.get("stream_name"),
        source.get("streamName"),
        source.get("server_name"),
        source.get("serverName"),
        source.get("name"),
        source.get("title"),
        source.get("server"),
    )
    if not raw:
        return ""
    text = " ".join(str(raw).replace("|", " ").replace("•", " ").split()).strip()
    if not text:
        return ""
    match_name = normalize_text(match.get("name"))
    if match_name and normalize_text(text) == match_name:
        return ""

    # Remove only playback/provider decoration. What remains must look like a real caster label.
    cleaned = re.sub(r"^[\s▶▷►▸⏵⏯️]+", "", text)
    cleaned = re.sub(r"(?i)\b(?:FULL\s*HD|FHD|UHD|4K|2K|2160P|1440P|1080P|720P|576P|540P|480P|360P|HD|SD)\b", " ", cleaned)
    cleaned = re.sub(r"(?i)\b(?:HLS|FLV|DASH|MPD|M3U8|TS|MP4)\b", " ", cleaned)
    cleaned = re.sub(r"(?i)\b(?:XOILAC(?:Z)?(?:\.IO)?|XOI\s*LAC|XTH)\b", " ", cleaned)
    cleaned = re.sub(r"(?i)\b(?:NGUỒN|NGUON|LUỒNG|LUONG|SOURCE|SERVER|STREAM|LINK|FEED|CAM)\s*#?\d*\b", " ", cleaned)
    cleaned = re.sub(r"(?i)\b(?:PLAY|LIVE|VIDEO|AUTO|DEFAULT|MAIN|PRIMARY)\b", " ", cleaned)
    cleaned = re.sub(r"\b\d+\b", " ", cleaned)
    cleaned = re.sub(r"^[\s:|/\-–—]+|[\s:|/\-–—]+$", "", cleaned)
    cleaned = " ".join(cleaned.split()).strip()
    if not cleaned:
        return ""

    norm = normalize_text(cleaned)
    generic = {
        "xoilac", "xoilacz", "nguon", "luong", "source", "server", "stream", "link",
        "hls", "flv", "dash", "mpd", "ts", "hd", "fhd", "full hd", "live", "video",
    }
    if not norm or norm in generic or norm.isdigit():
        return ""
    # Reject leftovers that are still just generic source numbering/quality text.
    tokens = [t for t in norm.split() if t]
    if tokens and all(t in generic or t.isdigit() for t in tokens):
        return ""
    return cleaned


def gavang_competition_from_body(body):
    """Read only competition metadata from Gà Vàng resolver detail.

    Do not touch commentator/source metadata and do not fall back to sport_name here.
    """
    keys = (
        "competition", "competition_name", "competitionName",
        "league", "league_name", "leagueName",
        "tournament", "tournament_name", "tournamentName",
        "championship", "championship_name", "championshipName",
    )
    nested = ("data", "meta", "event", "match", "detail", "info")

    def pick(obj, depth=0):
        if not isinstance(obj, dict) or depth > 3:
            return ""
        for key in keys:
            value = obj.get(key)
            if isinstance(value, dict):
                value = first_text(value.get("name"), value.get("title"), value.get("label"))
            text = first_text(value)
            if text:
                return text
        for key in nested:
            text = pick(obj.get(key), depth + 1)
            if text:
                return text
        return ""

    return pick(body)


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


def source_meta(source, match, index, count, xoilac_labels_available=False, xoilac_explicit_repeated=False):
    provider = str(match.get("provider") or "").lower()
    names = []
    seen = set()

    if provider == "xoilacxth":
        # XoiLac multi-source pages expose the caster on each stream button. Prefer that label.
        label_commentator = xoilac_label_commentator(source, match)
        if label_commentator:
            names.append(label_commentator)
            seen.add(label_commentator)
        else:
            explicit = commentator_of(source)
            # A single explicit value copied to every stream is match-level metadata, not per-stream.
            if explicit and not (count > 1 and xoilac_explicit_repeated and xoilac_labels_available):
                names.append(explicit)
                seen.add(explicit)
        match_commentator = first_text(match.get("commentator"), match.get("blv"), match.get("caster"))
        if not names and count <= 1 and match_commentator:
            names.append(match_commentator)
    else:
        # Preserve the original, already-working commentator path for Gà Vàng and every other source.
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
            resolved_match = match
            if str(match.get("provider") or "").lower() == "gavang33" and not first_text(match.get("competition"), match.get("league"), match.get("tournament")):
                competition = gavang_competition_from_body(body)
                if competition:
                    resolved_match = dict(match)
                    resolved_match["competition"] = competition
            return {"match": resolved_match, "sources": sources}
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

    for provider in ORDERED_PROVIDERS:
        buckets[provider["key"]].sort(key=match_sort_key)

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

            provider_key = str(match.get("provider") or "").lower()
            xoilac_label_values = [xoilac_label_commentator(src, match) for src in sources] if provider_key == "xoilacxth" else []
            xoilac_labels_available = any(xoilac_label_values)
            xoilac_explicit_values = [commentator_of(src) for src in sources if commentator_of(src)] if provider_key == "xoilacxth" else []
            xoilac_explicit_repeated = len(sources) > 1 and bool(xoilac_explicit_values) and len(set(xoilac_explicit_values)) == 1

            for index, source in enumerate(sources):
                if not source.get("url"):
                    continue
                meta = source_meta(
                    source,
                    match,
                    index,
                    len(sources),
                    xoilac_labels_available=xoilac_labels_available,
                    xoilac_explicit_repeated=xoilac_explicit_repeated,
                )
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
