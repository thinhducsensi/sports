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
MAX_WORKERS = 32
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
BUILD_VERSION = "v20"

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
    return first_text(match.get("provider_name"), match.get("source_name"), match.get("provider"), match.get("source"), "Khác")


def api_provider_order(data, items):
    order = []
    seen = set()
    providers = data.get("providers") if isinstance(data, dict) else None
    if isinstance(providers, list):
        for raw in providers:
            if isinstance(raw, dict):
                key = normalize_text(first_text(raw.get("id"), raw.get("key"), raw.get("provider"), raw.get("name")))
                name = first_text(raw.get("name"), raw.get("provider_name"), raw.get("id"), raw.get("key"))
            else:
                key = normalize_text(raw)
                name = str(raw or "").strip()
            if key and key not in seen:
                order.append((key, name or key))
                seen.add(key)
    for item in sorted(items, key=lambda x: x.get("api_index", 10**9)):
        key = provider_key(item["match"])
        if key and key not in seen:
            order.append((key, provider_display_name(item["match"])))
            seen.add(key)
    return order


def build_sport_order_by_block(items):
    orders = {}
    positions = {}
    for item in sorted(items, key=lambda x: x.get("api_index", 10**9)):
        block = item["state"].get("block", 9)
        category = sport_category(item["match"])
        if block not in orders:
            orders[block] = {}
            positions[block] = 0
        if category not in orders[block]:
            orders[block][category] = positions[block]
            positions[block] += 1
    return orders


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
    kickoff = get_kickoff(match) or 0
    name = normalize_text(match_name(match))
    competition = normalize_text(competition_name(match))
    commentator = normalize_text(direct_match_commentator(match))
    return f"{key}|{match_id}|{kickoff}|{name}|{competition}|{commentator}"

def match_sort_key(item, sport_orders):
    match = item["match"]
    state = item["state"]
    block = state.get("block", 9)
    category = sport_category(match)
    kickoff = state.get("kickoff") or get_kickoff(match) or 0
    untimed_live = 0 if kickoff == 0 and block == 0 else 1
    sport_order = sport_orders.get(block, {})
    return (
        block,
        sport_order.get(category, 10**9),
        untimed_live,
        kickoff if kickoff > 0 else 0,
        item.get("api_index", 10**9),
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
    direct = normalize_text(first_text(source.get("format"), source.get("type"), source.get("protocol"), source.get("extension"), source.get("ext")))
    format_map = {
        "hls": "HLS",
        "m3u8": "HLS",
        "application x mpegurl": "HLS",
        "application vnd apple mpegurl": "HLS",
        "dash": "DASH",
        "mpd": "DASH",
        "application dash+xml": "DASH",
        "flv": "FLV",
        "video x flv": "FLV",
        "ts": "TS",
        "mpegts": "TS",
        "mpeg ts": "TS",
        "mp4": "MP4",
        "video mp4": "MP4",
    }
    if direct in format_map:
        return format_map[direct]
    url = str(source.get("url") or "").lower().split("?", 1)[0]
    if ".m3u8" in url:
        return "HLS"
    if ".mpd" in url:
        return "DASH"
    if ".flv" in url:
        return "FLV"
    if ".ts" in url:
        return "TS"
    if ".mp4" in url:
        return "MP4"
    # SportStream resolver source.name often carries the stream kind (HLS1/FLV2)
    # even when `type` is missing.  Use only an explicit technical token.
    label = first_text(source.get("name"), source.get("label"), source.get("title"))
    label_norm = normalize_text(label)
    if re.search(r"(?:^|\s)hls\d*(?:$|\s)", label_norm):
        return "HLS"
    if re.search(r"(?:^|\s)flv\d*(?:$|\s)", label_norm):
        return "FLV"
    if re.search(r"(?:^|\s)dash\d*(?:$|\s)", label_norm):
        return "DASH"
    if re.search(r"(?:^|\s)ts\d*(?:$|\s)", label_norm):
        return "TS"
    if re.search(r"(?:^|\s)mp4\d*(?:$|\s)", label_norm):
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


def direct_match_commentator(match):
    for key in ("commentator", "blv", "caster"):
        value = match.get(key)
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _split_commentator_candidates(value):
    text = str(value or "").strip()
    if not text:
        return []
    parts = re.split(r"\s*(?:/|\||•|,|;|\n|\r|\s+&\s+|\s+\+\s+)\s*", text)
    out = []
    seen = set()
    for part in parts:
        clean = part.strip(" ()[]{}-–—")
        norm = normalize_text(clean)
        if not clean or not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(clean)
    return out


def _explicit_source_commentator(source):
    containers = [source]
    for key in ("audio", "metadata", "meta"):
        nested = source.get(key)
        if isinstance(nested, dict):
            containers.append(nested)
    for container in containers:
        for key in ("commentator", "blv", "caster", "audio_name", "audioName", "audio_label", "audioLabel"):
            value = container.get(key)
            if isinstance(value, bool) or not isinstance(value, (str, int, float)):
                continue
            text = str(value).strip()
            if text:
                return text
    return ""


def _is_technical_stream_label(value):
    raw = str(value or "").strip()
    if not raw:
        return True
    norm = normalize_text(raw)
    if not norm:
        return True
    # Pure technical labels shown by resolver, not caster names.
    if re.fullmatch(r"(?i)(?:hls|flv|dash|ts|mp4)(?:\s*[-_]?\s*\d+)?", raw):
        return True
    if re.fullmatch(r"(?i)(?:4k|uhd|qhd|fhd|hd|sd|2160p?|1440p?|1080p?|720p?|576p?|540p?|480p?|360p?|\d{3,4}\s*[xX×]\s*\d{3,4})", raw):
        return True
    if re.fullmatch(r"(?i)(?:server|sv|stream|source|backup|main|primary|mirror)(?:\s*[-_#]?\s*\d+)?", raw):
        return True
    if norm in {"sport", "sport stream", "sports", "default", "unknown"}:
        return True
    return False


def _match_provider_aliases(match):
    aliases = set()
    for key in ("provider", "provider_name", "source", "source_name", "provider_key"):
        value = normalize_text(match.get(key))
        if value:
            aliases.add(value)
    return aliases


def _clean_stream_human_label(value):
    """Extract only the human/caster part from a resolver label.

    SportStream itself displays resolver sources as `provider • name` (rb0.toString()).
    Some resolvers put the caster in `provider`, others in `name`, while technical
    bits (HLS/FLV/FHD/Backup/Server...) live beside it.  Strip only those technical
    tokens and preserve the remaining human label exactly.
    """
    raw = str(value or "").strip()
    if not raw:
        return ""
    # Split only on strong UI separators; do not split normal spaces in names.
    parts = [part.strip(" ()[]{}-–—") for part in re.split(r"\s*(?:•|\||;|,|/|\\)\s*", raw)]
    human = []
    for part in parts:
        if not part:
            continue
        # Whole labels such as "Server 2" / "Backup 1" are purely technical.
        if re.fullmatch(r"(?i)(?:server|sv|stream|source|backup|main|primary|mirror)(?:\s*[-_#]?\s*\d+)?", part):
            continue
        # remove common technical suffix/prefix tokens while keeping actual names
        tokens = part.split()
        kept = []
        for token in tokens:
            t = token.strip("()[]{}-–—")
            low = normalize_text(t)
            if re.fullmatch(r"(?i)(?:hls|flv|dash|ts|mp4)(?:\d+)?", t):
                continue
            if re.fullmatch(r"(?i)(?:4k|uhd|qhd|fhd|hd|sd|2160p?|1440p?|1080p?|720p?|576p?|540p?|480p?|360p?|\d{3,4}[xX×]\d{3,4})", t):
                continue
            if low in {"backup", "main", "primary", "mirror", "stream", "source", "server", "sv"}:
                continue
            kept.append(token)
        candidate = " ".join(kept).strip(" ()[]{}-–—")
        if candidate and not _is_technical_stream_label(candidate):
            human.append(candidate)
    # If stripping did not produce a useful human label, do not invent one.
    if not human:
        return ""
    # Preserve order, remove duplicate pieces.
    out = []
    seen = set()
    for part in human:
        norm = normalize_text(part)
        if norm and norm not in seen:
            seen.add(norm)
            out.append(part)
    return " • ".join(out)


def source_commentator(source, match):
    # 1) Strongest signal: a commentator/caster field attached to this exact source.
    explicit = _explicit_source_commentator(source)
    if explicit:
        candidates = _split_commentator_candidates(explicit)
        if len(candidates) == 1:
            return candidates[0]
        # Never print an ambiguous merged source field for one stream.
        return ""

    # 2) Mirror SportStream resolver semantics.  rb0 is constructed from
    #    source.name + source.provider and its toString() is `provider • name`.
    #    Resolver providers are therefore a per-stream discriminator and must be
    #    checked before the match-level commentator list.
    source_provider = str(source.get("provider") or "").strip()
    provider_norm = normalize_text(source_provider)
    if source_provider and provider_norm not in _match_provider_aliases(match) and not _is_technical_stream_label(source_provider):
        human = _clean_stream_human_label(source_provider)
        if human:
            return human

    # 3) Some resolvers put the caster in source.name rather than provider.
    source_name = first_text(source.get("name"), source.get("label"), source.get("title"))
    human = _clean_stream_human_label(source_name)
    if human:
        # Prefer a known match-level name if the source label contains exactly one
        # of them; otherwise the resolver's per-stream human label is still more
        # precise than a merged match-level list.
        match_candidates = _split_commentator_candidates(direct_match_commentator(match))
        if match_candidates:
            label_norm = normalize_text(human)
            matched = [c for c in match_candidates if normalize_text(c) and normalize_text(c) in label_norm]
            if len(matched) == 1:
                return matched[0]
        return human

    # 4) Match-level fallback is safe only when there is exactly one commentator.
    match_candidates = _split_commentator_candidates(direct_match_commentator(match))
    if len(match_candidates) == 1:
        return match_candidates[0]
    return ""

def safe_title_text(value):
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    for mark in (",", "，", "︐", "︑", "﹐", "،", "、"):
        text = text.replace(mark, " / ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def source_key(source):
    headers = sorted((str(key).lower().strip(), str(value).strip()) for key, value in (source.get("headers") or {}).items())
    header_text = "\n".join(f"{key}:{value}" for key, value in headers)
    return f"{str(source.get('url') or '').strip()}\n{header_text}"


def merge_duplicate_source(existing, incoming):
    merged = dict(existing)
    for key in (
        "commentator", "blv", "caster", "audio_name", "audioName",
        "name", "label", "title", "server", "provider",
        "quality", "resolution", "video_quality", "videoQuality",
        "video_resolution", "videoResolution", "dimensions", "dimension",
        "width", "height", "video_width", "video_height", "videoWidth", "videoHeight",
        "format", "type",
    ):
        if not merged.get(key) and incoming.get(key):
            merged[key] = incoming.get(key)
    return merged


def _dimension_label(value):
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.search(r"(?<!\d)(\d{3,4})\s*[xX×]\s*(\d{3,4})(?!\d)", text)
    if not match:
        return ""
    return f"{match.group(1)}x{match.group(2)}"


def _quality_token(value):
    text = str(value or "").strip()
    if not text:
        return ""
    dimension = _dimension_label(text)
    if dimension:
        return dimension
    normalized = normalize_text(text)
    patterns = (
        (r"(?<!\d)2160p?(?!\d)", "2160p"),
        (r"(?<!\d)1440p?(?!\d)", "1440p"),
        (r"(?<!\d)1080p?(?!\d)", "1080p"),
        (r"(?<!\d)720p?(?!\d)", "720p"),
        (r"(?<!\d)576p?(?!\d)", "576p"),
        (r"(?<!\d)540p?(?!\d)", "540p"),
        (r"(?<!\d)480p?(?!\d)", "480p"),
        (r"(?<!\d)360p?(?!\d)", "360p"),
    )
    for pattern, label in patterns:
        if re.search(pattern, normalized):
            return label
    words = {word for word in normalized.split() if word}
    if "4k" in words:
        return "4K"
    if "uhd" in words:
        return "UHD"
    if "fhd" in words or ("full" in words and "hd" in words):
        return "FHD"
    if normalized == "hd":
        return "HD"
    if normalized == "sd":
        return "SD"
    return ""


def canonical_quality(value):
    token = _quality_token(value)
    if not token:
        return ""
    dimension = _dimension_label(token)
    if dimension:
        try:
            width, height = [int(x) for x in dimension.split("x", 1)]
        except ValueError:
            return dimension
        if width >= 3800 or height >= 2100:
            return "4K"
        if width >= 1900 or height >= 1000:
            return "FHD"
        if width >= 1200 or height >= 700:
            return "HD"
        return dimension
    mapping = {"2160p": "4K", "1440p": "QHD", "1080p": "FHD", "720p": "HD"}
    return mapping.get(token, token)


def source_quality_label(source):
    width = first_text(source.get("width"), source.get("video_width"), source.get("videoWidth"))
    height = first_text(source.get("height"), source.get("video_height"), source.get("videoHeight"))
    try:
        width_num = int(float(width)) if width else 0
        height_num = int(float(height)) if height else 0
    except (TypeError, ValueError):
        width_num = height_num = 0
    if width_num > 0 and height_num > 0:
        return canonical_quality(f"{width_num}x{height_num}")

    for key in ("resolution", "video_resolution", "videoResolution", "dimensions", "dimension"):
        label = canonical_quality(source.get(key))
        if label:
            return label

    for key in ("video_quality", "videoQuality", "quality"):
        label = canonical_quality(source.get(key))
        if label:
            return label

    # source.name is a stream label in SportStream. Only use it as quality
    # when the whole value itself is a quality/resolution token.
    for key in ("name", "label", "title"):
        raw = source.get(key)
        if not isinstance(raw, (str, int, float)) or isinstance(raw, bool):
            continue
        compact = str(raw).strip()
        if re.fullmatch(r"(?i)(?:\d{3,4}\s*[xX×]\s*\d{3,4}|2160p?|1440p?|1080p?|720p?|576p?|540p?|480p?|360p?|4K|UHD|QHD|FHD|HD|SD)", compact):
            label = canonical_quality(compact)
            if label:
                return label
    return ""


def quality_rank(source):
    quality = normalize_text(source_quality_label(source))
    if "4k" in quality or "uhd" in quality or "2160" in quality:
        return 0
    if "fhd" in quality or "1080" in quality or "1920x1080" in quality:
        return 1
    if quality == "hd" or "720" in quality or "1280x720" in quality:
        return 2
    if "540" in quality:
        return 3
    if "sd" in quality or "480" in quality or "360" in quality:
        return 4
    return 5


def source_priority(source):
    fmt = infer_format(source)
    format_rank = {"HLS": 0, "FLV": 1, "TS": 2, "DASH": 3, "MP4": 4}.get(fmt, 5)
    return (format_rank, quality_rank(source), source.get("url", ""))


def order_sources_by_commentator_and_format(sources, match):
    # Primary block: commentator/caster, in the order first seen from the API.
    # Secondary block inside each commentator: HLS -> FLV -> TS -> DASH -> MP4 -> other.
    commentator_order = {}
    next_index = 0
    decorated = []
    for api_index, source in enumerate(sources):
        commentator = source_commentator(source, match)
        norm = normalize_text(commentator)
        if norm:
            if norm not in commentator_order:
                commentator_order[norm] = next_index
                next_index += 1
            commentator_rank = commentator_order[norm]
            unknown_rank = 0
        else:
            commentator_rank = 10**9
            unknown_rank = 1
        fmt = infer_format(source)
        format_rank = {"HLS": 0, "FLV": 1, "TS": 2, "DASH": 3, "MP4": 4}.get(fmt, 5)
        decorated.append(((unknown_rank, commentator_rank, format_rank, api_index), source))
    decorated.sort(key=lambda item: item[0])
    return [source for _, source in decorated]


def dedupe_sources(sources):
    merged = {}
    order = []
    for source in sources:
        if not source or not source.get("url"):
            continue
        key = source_key(source)
        if key in merged:
            merged[key] = merge_duplicate_source(merged[key], source)
        else:
            merged[key] = source
            order.append(key)
    return [merged[key] for key in order]


def direct_sources_from_match(match):
    sources = []
    fallback_headers = normalize_headers(match.get("headers"))
    for key in ("sources", "streams", "links", "urls"):
        array = match.get(key)
        if isinstance(array, list):
            for raw in array:
                source = normalize_source(raw, fallback_headers)
                if source:
                    sources.append(source)
    source = normalize_source(match.get("source"), fallback_headers)
    if source:
        sources.append(source)
    return dedupe_sources(sources)


def resolver_url_for_match(match):
    resolver = match.get("resolver")
    if isinstance(resolver, str) and resolver.strip():
        return resolver.strip()
    sources = match.get("sources")
    if isinstance(sources, list) and sources and isinstance(sources[0], dict):
        resolver = sources[0].get("resolver")
        if isinstance(resolver, str) and resolver.strip():
            return resolver.strip()
    return ""


def resolver_sources_exact(body, match):
    if not isinstance(body, dict):
        return []
    array = body.get("sources")
    if not isinstance(array, list):
        return []
    fallback_headers = normalize_headers(match.get("headers"))
    sources = []
    for raw in array:
        source = normalize_source(raw, fallback_headers)
        if source:
            sources.append(source)
    return dedupe_sources(sources)


def looks_like_stream_url(url):
    path = str(url or "").lower().split("?", 1)[0]
    return any(ext in path for ext in (".m3u8", ".mpd", ".ts", ".flv", ".mp4"))


def resolve_match(match):
    direct_sources = direct_sources_from_match(match)
    resolver = resolver_url_for_match(match)
    resolver_sources = []
    if resolver:
        if looks_like_stream_url(resolver):
            source = normalize_source({"url": resolver}, match.get("headers"))
            if source:
                resolver_sources.append(source)
        else:
            body = fetch_json(resolver, timeout=4.5, retries=1, cache_bust=False)
            if body is not None:
                resolver_sources = resolver_sources_exact(body, match)
    sources = dedupe_sources(resolver_sources + direct_sources)
    sources = order_sources_by_commentator_and_format(sources, match)
    return {"match": match, "sources": sources} if sources else None


def merge_match_items(items):
    merged = {}
    order = []
    for item in items:
        key = match_identity(item["match"])
        if key not in merged:
            merged[key] = {
                "match": dict(item["match"]),
                "state": dict(item["state"]),
                "sources": list(item["sources"]),
                "api_index": item.get("api_index", 10**9),
            }
            order.append(key)
            continue
        current = merged[key]
        current["sources"] = dedupe_sources(current["sources"] + item["sources"])
        current["sources"] = order_sources_by_commentator_and_format(current["sources"], current["match"])
        current["api_index"] = min(current.get("api_index", 10**9), item.get("api_index", 10**9))
        if item["state"].get("rank", 9) < current["state"].get("rank", 9):
            current["state"] = dict(item["state"])
    return [merged[key] for key in order]


def header_value(headers, name):
    wanted = name.lower()
    for key, value in (headers or {}).items():
        if str(key).lower() == wanted:
            return str(value or "")
    return ""


def source_meta(source, match):
    commentator = source_commentator(source, match)
    quality = source_quality_label(source)
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
    name = safe_title_text(match_name(match))
    head = " ".join(part for part in (" ".join(parts), time_part, icon, name) if part)
    meta = source_meta(source, match)
    commentator = safe_title_text(meta["commentator"])
    if commentator:
        head += f" ({commentator})"
    competition = safe_title_text(competition_name(match))
    if competition:
        head += f" • {competition}"
    stream_info = safe_title_text(meta["stream_info"])
    if stream_info:
        head += f" [{stream_info}]"
    title = safe_title_text(head)
    if "," in title:
        raise ValueError(f"Unsafe comma remained in title: {title}")
    return title


def build_playlist():
    data = fetch_json(SPORT_API, timeout=10, retries=3, cache_bust=True)
    if not isinstance(data, dict):
        raise RuntimeError("Không lấy được sport.json")
    matches = data.get("matches") if isinstance(data.get("matches"), list) else []
    current_ms = now_ms()
    future_end_ms = end_of_next_vietnam_day(current_ms)
    candidates = []
    for api_index, match in enumerate(matches):
        if not isinstance(match, dict):
            continue
        state = classify_match(match, current_ms, future_end_ms)
        if state:
            candidates.append({"match": match, "state": state, "api_index": api_index})

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
                    value["api_index"] = item["api_index"]
                    resolved.append(value)

    resolved = merge_match_items(resolved)
    if candidates and not resolved:
        raise RuntimeError("Có trận hợp lệ nhưng không resolve được luồng nào; giữ playlist cũ")

    provider_order = api_provider_order(data, resolved)
    sport_orders = build_sport_order_by_block(resolved)
    buckets = {key: [] for key, _ in provider_order}
    for item in resolved:
        buckets.setdefault(provider_key(item["match"]), []).append(item)
    for key in buckets:
        buckets[key].sort(key=lambda item: match_sort_key(item, sport_orders))

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
    print(f"{BUILD_VERSION}: Đã xuất {total_streams} luồng từ {total_matches} trận. {update_group}")


if __name__ == "__main__":
    try:
        build_playlist()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
