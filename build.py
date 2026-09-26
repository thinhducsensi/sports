import hashlib
import json
import math
import re
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
MAX_WORKERS = 24
RESOLVER_TIMEOUT = 5.0
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

TERMINAL_STATUSES = {
    "ft", "finished", "finish", "ended", "end", "completed", "complete",
    "cancelled", "canceled", "postponed", "abandoned", "ket thuc", "huy",
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
    text = scalar_text(value).lower().replace("_", " ").replace("-", " ")
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


def fetch_json(url, timeout=8, retries=1, cache_bust=False):
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
        except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError, UnicodeDecodeError):
            if attempt + 1 < retries:
                sleep(1.0 + attempt)
    return None


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
    return match.get("live") is True or bool(status_values(match) & LIVE_STATUSES)


def classify_match(match, current_ms, future_end_ms):
    if is_terminal(match):
        return None
    kickoff = get_kickoff(match)
    explicit_live = is_explicit_live(match)
    if kickoff is None:
        return {"block": 0, "kind": "live", "kickoff": 0} if explicit_live else None
    if kickoff > current_ms:
        if kickoff <= future_end_ms:
            return {"block": 1, "kind": "upcoming", "kickoff": kickoff}
        return None
    age = current_ms - kickoff
    if explicit_live:
        return {"block": 0, "kind": "live", "kickoff": kickoff}
    if 0 <= age <= RECENT_WINDOW_MS:
        return {"block": 0, "kind": "recent", "kickoff": kickoff}
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
    if raw in {"football", "soccer", "bong da", "association football"}:
        return "football"
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
                if source:
                    sources.append(source)
    return dedupe_sources(sources)


def resolver_sources(body, match):
    if not isinstance(body, dict):
        return []
    array = body.get("sources")
    if not isinstance(array, list):
        return []
    base_headers = normalize_headers(match.get("headers"))
    sources = []
    for index, raw in enumerate(array):
        source = normalize_source(raw, base_headers, index)
        if source:
            sources.append(source)
    return dedupe_sources(sources)


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


def source_commentator(source, match):
    known = match_known_people(match)
    explicit = explicit_source_people(source)
    evidence_values = [
        source.get("provider"), source.get("name"), source.get("label"), source.get("title"), source.get("server"), source.get("url")
    ]
    evidence_norm = " ".join(normalize_text(v) for v in evidence_values if scalar_text(v))
    evidence_slug = slug_text(" ".join(scalar_text(v) for v in evidence_values if scalar_text(v)))

    if explicit:
        if len(explicit) == 1:
            return explicit[0]
        matched = [person for person in explicit if slug_text(person) and slug_text(person) in evidence_slug]
        if len(matched) == 1:
            return matched[0]

    if known:
        matched = []
        for person in known:
            norm = normalize_text(person)
            slug = slug_text(person)
            if (norm and norm in evidence_norm) or (slug and slug in evidence_slug):
                matched.append(person)
        if len(matched) == 1:
            return matched[0]

    provider = first_text(source.get("provider"))
    if provider and not technical_label(provider):
        human = clean_human_label(provider)
        if human:
            return human

    for key in ("name", "label", "title", "server"):
        human = clean_human_label(source.get(key))
        if human:
            if known:
                matches = [person for person in known if slug_text(person) and slug_text(person) in slug_text(human)]
                if len(matches) == 1:
                    return matches[0]
            return human

    if len(known) == 1:
        return known[0]
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
    return safe_title_text(value).replace("&", "&amp;").replace('"', "&quot;")


def header_value(headers, name):
    wanted = name.lower()
    for key, value in (headers or {}).items():
        if str(key).lower() == wanted:
            return scalar_text(value)
    return ""


def encode_component(value):
    return quote(str(value), safe="-_.!~*'()")


def stream_url_with_headers(source):
    blocked = {"host", "content-length", "transfer-encoding", "connection"}
    headers = source.get("headers") or {}
    params = []
    for key, value in headers.items():
        if str(key).lower() in blocked:
            continue
        params.append(f"{encode_component(key)}={encode_component(value)}")
    if not params:
        return source["url"]
    separator = "&" if "|" in source["url"] else "|"
    return f"{source['url']}{separator}{'&'.join(params)}"


def stable_source_id(match, source):
    raw = f"{match_identity(match)}\n{source_key(source)}".encode("utf-8")
    return "sport-" + hashlib.sha1(raw).hexdigest()[:12]


def resolve_all(candidates):
    resolver_to_matches = {}
    direct_map = {}
    for item in candidates:
        match = item["match"]
        direct_map[item["api_index"]] = direct_sources(match)
        url = resolver_url(match)
        if url:
            resolver_to_matches.setdefault(url, []).append(item["api_index"])

    bodies = {}
    urls = list(resolver_to_matches)
    if urls:
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(urls))) as executor:
            future_map = {executor.submit(fetch_json, url, RESOLVER_TIMEOUT, 1, False): url for url in urls}
            for future in as_completed(future_map):
                url = future_map[future]
                try:
                    bodies[url] = future.result()
                except Exception:
                    bodies[url] = None

    resolved = []
    for item in candidates:
        match = item["match"]
        direct = direct_map.get(item["api_index"], [])
        url = resolver_url(match)
        remote = resolver_sources(bodies.get(url), match) if url else []
        sources = dedupe_sources(remote + direct)
        sources = order_sources(sources, match)
        if sources:
            resolved.append({**item, "sources": sources})
    return resolved


def build_title(match, state, source):
    live = "🔴 " if state.get("block") == 0 else ""
    name = safe_title_text(match_name(match))
    caster, stream_info = source_meta(source, match)
    competition = safe_title_text(competition_name(match))
    caster_part = f" ({safe_title_text(caster)})" if caster else ""
    competition_part = f" • {competition}" if competition else ""
    stream_part = f" [{stream_info}]" if stream_info else ""
    return f"{live}{format_match_time(get_kickoff(match))} {sport_icon(match)} {name}{caster_part}{competition_part}{stream_part}".strip()


def build_playlist():
    data = fetch_json(SPORT_API, timeout=10, retries=2, cache_bust=True)
    if not isinstance(data, dict):
        raise RuntimeError("Không lấy được sport.json")
    matches = data.get("matches") if isinstance(data.get("matches"), list) else []
    current = now_ms()
    future_end = end_of_next_vietnam_day(current)
    candidates = []
    for api_index, match in enumerate(matches):
        if not isinstance(match, dict):
            continue
        state = classify_match(match, current, future_end)
        if state:
            candidates.append({"match": match, "state": state, "api_index": api_index})

    resolved = resolve_all(candidates)
    if candidates and not resolved:
        raise RuntimeError("Không resolve được luồng nào; giữ playlist cũ")

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
                referer = header_value(source.get("headers"), "Referer")
                origin = header_value(source.get("headers"), "Origin")
                user_agent = header_value(source.get("headers"), "User-Agent")
                if referer:
                    lines.append(f"#EXTVLCOPT:http-referrer={referer}")
                if origin:
                    lines.append(f"#EXTVLCOPT:http-origin={origin}")
                if user_agent:
                    lines.append(f"#EXTVLCOPT:http-user-agent={user_agent}")
                lines.append(stream_url_with_headers(source))
                total_streams += 1

    temp = Path("playlist.m3u.tmp")
    temp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    temp.replace("playlist.m3u")
    print(f"v23: {total_streams} luồng / {total_matches} trận / {len(provider_sequence)} nguồn")


if __name__ == "__main__":
    try:
        build_playlist()
    except Exception as exc:
        print(f"build.py: {exc}", file=sys.stderr)
        sys.exit(1)
