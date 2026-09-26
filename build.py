import hashlib
import html as html_lib
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
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit
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

DIRECT_FEEDS = {
    "colatv": {
        "url": "https://api.cltvlv.com/api/matches",
        "kind": "cola",
        "headers": {"User-Agent": USER_AGENT},
    },
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
    "socolive": {
        "url": "https://json.vnres.co/all_live_rooms.json",
        "kind": "socolive",
        "headers": {"User-Agent": "Mozilla/5.0", "Referer": "https://socolivedz.com/"},
    },
    "vuasanco": {
        "url": "https://vsc9.com/api/data/lives/matches",
        "kind": "vuasanco",
        "headers": {"User-Agent": USER_AGENT, "Origin": "https://vsc9.com", "Referer": "https://vsc9.com/"},
    },
}

PROVIDER_ALIASES = {
    "cola": "colatv",
    "colatv": "colatv",
    "chuoi chien": "chuoichien",
    "chuoichien": "chuoichien",
    "ga vang 33": "gavang33",
    "gavang33": "gavang33",
    "socolive": "socolive",
    "vua san co": "vuasanco",
    "vuasanco": "vuasanco",
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


def fetch_json_custom(url, headers=None, timeout=6):
    req_headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": USER_AGENT,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if isinstance(headers, dict):
        req_headers.update(headers)
    try:
        request = Request(with_cache_buster(url), headers=req_headers)
        with urlopen(request, timeout=timeout) as response:
            if not 200 <= getattr(response, "status", 200) < 300:
                return None
            return json.loads(response.read().decode("utf-8-sig"))
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def fetch_text(url, headers=None, timeout=5):
    req_headers = {
        "Accept": "text/html,application/xhtml+xml,application/json,text/plain,*/*",
        "User-Agent": USER_AGENT,
        "Cache-Control": "no-cache",
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


def parse_direct_cola(data, cfg):
    out = {}
    root = data.get("data") if isinstance(data, dict) else None
    if not isinstance(root, dict):
        return out
    playback_headers = {"User-Agent": USER_AGENT, "Referer": "https://cola.tv/"}
    for raw in root.values():
        if not isinstance(raw, dict):
            continue
        home, away = first_text(raw.get("homeTeamName")), first_text(raw.get("awayTeamName"))
        if not home or not away:
            continue
        sources = []
        anchors = raw.get("anchorAppointmentVoList")
        if isinstance(anchors, list):
            for i, anchor in enumerate(anchors):
                if not isinstance(anchor, dict):
                    continue
                caster = first_text(anchor.get("nickName"), anchor.get("nickname"), anchor.get("name"))
                hls = first_text(anchor.get("playStreamAddress2"))
                flv = first_text(anchor.get("playStreamAddress"))
                if hls:
                    sources.append(source_obj(hls, caster, "HLS", headers=playback_headers, name=caster, index=i*2))
                if flv:
                    sources.append(source_obj(flv, caster, "FLV", headers=playback_headers, name=caster, index=i*2+1))
        if sources:
            out[pair_key(home, away)] = dedupe_sources(sources)
    return out


def _iter_candidate_matches(data):
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    for key in ("matches", "result", "data", "response"):
        value = data.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            if key == "data":
                nested = value.get("matches")
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
        home_obj = row.get("home") if isinstance(row.get("home"), dict) else match.get("home") if isinstance(match.get("home"), dict) else {}
        away_obj = row.get("away") if isinstance(row.get("away"), dict) else match.get("away") if isinstance(match.get("away"), dict) else {}
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
                urls = anchor.get("streamUrls")
                if isinstance(urls, list):
                    for ui, url in enumerate(urls):
                        if not isinstance(url, str) or not url.strip():
                            continue
                        fmt = "FLV" if ".flv" in url.lower() else "HLS" if ".m3u8" in url.lower() else ""
                        sources.append(source_obj(url, caster, fmt, headers=playback_headers, name=caster, index=ai*100+ui))
        if sources:
            out[pair_key(home, away)] = dedupe_sources(sources)
    return out


def parse_direct_socolive(data, cfg):
    out = {}
    rows = data if isinstance(data, list) else data.get("data") if isinstance(data, dict) and isinstance(data.get("data"), list) else []
    playback_headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://socolivedz.com/"}
    for ri, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        home = first_text(row.get("hostName"), row.get("homeName"))
        away = first_text(row.get("guestName"), row.get("awayName"))
        if not home or not away:
            continue
        room = row.get("roomItem") if isinstance(row.get("roomItem"), dict) else row
        anchor = room.get("anchor") if isinstance(room.get("anchor"), dict) else {}
        caster = first_text(anchor.get("nickName"), anchor.get("nickname"), anchor.get("name"))
        stream = room.get("stream") if isinstance(room.get("stream"), dict) else row.get("stream") if isinstance(row.get("stream"), dict) else {}
        sources = []
        for idx, (key, fmt, q) in enumerate((("hdM3u8","HLS","FHD"),("m3u8","HLS",""),("hdFlv","FLV","HD"),("flv","FLV",""))):
            url = first_text(stream.get(key))
            obj = source_obj(url, caster, fmt, q, playback_headers, caster, idx)
            if obj:
                sources.append(obj)
        if sources:
            out[pair_key(home, away)] = dedupe_sources(sources)
    return out


def _extract_vuasanco_streams(container, playback_headers):
    sources = []
    seq = 0
    if not isinstance(container, dict):
        return sources
    arrays = []
    for key in ("lives", "streams", "links"):
        value = container.get(key)
        if isinstance(value, list):
            arrays.extend(value)
    for entry in arrays:
        if isinstance(entry, str):
            url, caster = entry, ""
        elif isinstance(entry, dict):
            url = first_text(entry.get("link"), entry.get("url"), entry.get("streamUrl"))
            caster = first_text(entry.get("commentator"), entry.get("blv"), entry.get("caster"))
        else:
            continue
        fmt = "HLS" if ".m3u8" in url.lower() else "FLV" if ".flv" in url.lower() else ""
        obj = source_obj(url, caster, fmt, headers=playback_headers, name=caster, index=seq)
        seq += 1
        if obj:
            sources.append(obj)
    return sources


def parse_direct_vuasanco(data, cfg):
    out = {}
    rows = _iter_candidate_matches(data)
    playback_headers = {"User-Agent": USER_AGENT, "Origin": "https://vsc9.com", "Referer": "https://vsc9.com/"}
    for row in rows:
        if not isinstance(row, dict):
            continue
        home_obj = row.get("home") if isinstance(row.get("home"), dict) else {}
        away_obj = row.get("away") if isinstance(row.get("away"), dict) else {}
        home = first_text(home_obj.get("name"), row.get("homeName"), row.get("team1"))
        away = first_text(away_obj.get("name"), row.get("awayName"), row.get("team2"))
        if not home or not away:
            continue
        sources = _extract_vuasanco_streams(row, playback_headers)
        if sources:
            out[pair_key(home, away)] = dedupe_sources(sources)
    return out


DIRECT_PARSERS = {
    "cola": parse_direct_cola,
    "chuoichien": parse_direct_chuoichien,
    "gavang33": parse_direct_gavang33,
    "socolive": parse_direct_socolive,
    "vuasanco": parse_direct_vuasanco,
}


def fetch_direct_indexes():
    indexes = {}
    def task(provider, cfg):
        data = fetch_json_custom(cfg["url"], cfg.get("headers"), timeout=5.5)
        if data is None:
            return provider, {}
        parser = DIRECT_PARSERS.get(cfg.get("kind"))
        try:
            return provider, parser(data, cfg) if parser else {}
        except Exception:
            return provider, {}
    with ThreadPoolExecutor(max_workers=len(DIRECT_FEEDS)) as executor:
        futures = [executor.submit(task, provider, cfg) for provider, cfg in DIRECT_FEEDS.items()]
        for future in as_completed(futures):
            provider, index = future.result()
            indexes[provider] = index
    return indexes


def direct_sources_for_match(match, indexes):
    provider = provider_canonical(first_text(match.get("provider"), match.get("source"), match.get("provider_name"), match.get("source_name")))
    index = indexes.get(provider)
    if not index:
        return []
    home = first_text(match.get("home_team"), match.get("homeTeam"), match.get("team1"), match.get("home"))
    away = first_text(match.get("away_team"), match.get("awayTeam"), match.get("team2"), match.get("away"))
    if not home or not away:
        home, away = split_match_teams(match_name(match))
        key = f"{home}|{away}" if home and away else ""
    else:
        key = pair_key(home, away)
    if key in index:
        return index[key]
    # tolerant fallback: match both normalized team names inside direct key
    h, a = split_match_teams(match_name(match))
    if h and a:
        for direct_key, sources in index.items():
            dh, da = direct_key.split("|", 1) if "|" in direct_key else ("", "")
            if (h == dh and a == da) or (h == da and a == dh):
                return sources
    return []


def slugify_vi(value):
    text = normalize_text(value)
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text


def _team_pair_from_match(match):
    return split_match_teams(match_name(match))


def _candidate_by_pair(candidates, provider):
    out = {}
    for item in candidates:
        if provider_key(item["match"]) != provider:
            continue
        home, away = _team_pair_from_match(item["match"])
        if home and away:
            out.setdefault(pair_key(home, away), []).append(item)
            out.setdefault(pair_key(away, home), []).append(item)
    return out


def _best_candidate_for_pair(pair_map, home, away):
    rows = pair_map.get(pair_key(home, away)) or pair_map.get(pair_key(away, home)) or []
    return rows[0] if rows else None


def _parse_giovang_detail(html_text, page_url):
    if not html_text:
        return []
    m = re.search("data-blv\\s*=\\s*([\\\"'])(.*?)\\1", html_text, flags=re.I | re.S)
    if not m:
        return []
    raw = html_lib.unescape(m.group(2)).replace("\\/", "/")
    try:
        rows = json.loads(raw)
    except Exception:
        return []
    headers = {"User-Agent": USER_AGENT, "Referer": page_url, "Origin": "https://giovang.org"}
    out = []
    if isinstance(rows, list):
        for i, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            caster = first_text(row.get("blv_name"), row.get("name"), row.get("commentator"))
            url = first_text(row.get("mobile_stream_url"), row.get("pc_stream_url"), row.get("stream_url"))
            if not url:
                continue
            fmt = "HLS" if ".m3u8" in url.lower() else "FLV" if ".flv" in url.lower() else ""
            out.append(source_obj(url, caster=caster, fmt=fmt, headers=headers, name=caster, index=i))
    return dedupe_sources(out)


def fetch_giovang_sources(candidates):
    matches = _candidate_by_pair(candidates, "giovang")
    if not matches:
        return {}
    feed = fetch_json_custom(
        "https://live-api.keonhacaitp.one/storage/livestream/live.json",
        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://giovang.org/"},
        timeout=5,
    )
    rows = feed.get("response") if isinstance(feed, dict) else None
    if not isinstance(rows, list):
        return {}
    jobs = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        teams = row.get("teams") if isinstance(row.get("teams"), dict) else {}
        home_obj = teams.get("home") if isinstance(teams.get("home"), dict) else {}
        away_obj = teams.get("away") if isinstance(teams.get("away"), dict) else {}
        home = first_text(home_obj.get("name"))
        away = first_text(away_obj.get("name"))
        item = _best_candidate_for_pair(matches, home, away)
        if not item:
            continue
        ident = first_text(row.get("fi"), row.get("id"))
        day_month = first_text(row.get("day_month")).replace("/", "-")
        hs = first_text(home_obj.get("slug")) or slugify_vi(home)
        aw_slug = first_text(away_obj.get("slug")) or slugify_vi(away)
        if not ident or not day_month or not hs or not aw_slug:
            continue
        page = f"https://giovang.org/truc-tiep-{hs}-vs-{aw_slug}-{day_month}-{ident}"
        jobs.append((item["api_index"], page))
    result = {}
    if jobs:
        with ThreadPoolExecutor(max_workers=min(8, len(jobs))) as ex:
            fmap = {ex.submit(fetch_text, page, {"User-Agent": USER_AGENT, "Referer": "https://giovang.org/"}, 4): (idx, page) for idx, page in jobs}
            for fut in as_completed(fmap):
                idx, page = fmap[fut]
                try:
                    sources = _parse_giovang_detail(fut.result(), page)
                except Exception:
                    sources = []
                if sources:
                    result[idx] = sources
    return result


def _strip_tags(text):
    text = re.sub(r"<script\\b[^>]*>.*?</script>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<style\\b[^>]*>.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\\s+", " ", html_lib.unescape(text)).strip()


def _xoilac_find_match_pages(list_html, candidates):
    if not list_html:
        return {}
    pages = {}
    anchors = list(re.finditer("<a\\b[^>]*href=[\\\"']([^\\\"']*/truc-tiep/[^\\\"']+)[\\\"'][^>]*>", list_html, flags=re.I))
    for item in candidates:
        if provider_key(item["match"]) != "xoilacxth":
            continue
        home, away = _team_pair_from_match(item["match"])
        if not home or not away:
            continue
        for m in anchors:
            start = max(0, m.start() - 2200)
            end = min(len(list_html), m.end() + 2200)
            context = normalize_text(_strip_tags(list_html[start:end]))
            if home in context and away in context:
                pages[item["api_index"]] = urljoin("https://xoilacz.vip", html_lib.unescape(m.group(1)))
                break
    return pages


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


def _xoilac_pages_from_json(text, base_url, candidates):
    if not text:
        return {}, {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}, {}
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return {}, {}
    pair_map = _candidate_by_pair(candidates, "xoilacxth")
    pages = {}
    commentators = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        home_obj = row.get("home_team") if isinstance(row.get("home_team"), dict) else {}
        away_obj = row.get("away_team") if isinstance(row.get("away_team"), dict) else {}
        home = first_text(home_obj.get("name"), row.get("home_name"), row.get("home"))
        away = first_text(away_obj.get("name"), row.get("away_name"), row.get("away"))
        item = _best_candidate_for_pair(pair_map, home, away)
        if not item:
            continue
        slug = first_text(row.get("slug"), row.get("seo_slug"))
        detail = first_text(row.get("url"), row.get("link"), row.get("detail_url"))
        if not detail and slug:
            detail = f"/truc-tiep/{slug}"
        if detail:
            pages[item["api_index"]] = urljoin(base_url.rstrip("/") + "/", detail)
        names = []
        arr = row.get("commentators")
        if isinstance(arr, list):
            for c in arr:
                if isinstance(c, dict):
                    name = first_text(c.get("name"), c.get("nickName"), c.get("nickname"))
                else:
                    name = scalar_text(c)
                if name and normalize_text(name) not in {normalize_text(x) for x in names}:
                    names.append(name)
        if names:
            commentators[item["api_index"]] = names
    return pages, commentators


def _xoilac_listing_for_domain(base_url, candidates):
    headers = {"User-Agent": USER_AGENT, "Referer": base_url.rstrip("/") + "/"}
    endpoint = base_url.rstrip("/") + "/sport/football/filter/commentator"
    listing = fetch_text(endpoint, headers, 5)
    if not listing:
        return {}, {}, headers
    pages_json, commentators = _xoilac_pages_from_json(listing, base_url, candidates)
    if pages_json:
        return pages_json, commentators, headers
    pages_html = _xoilac_find_match_pages(listing, candidates)
    # _xoilac_find_match_pages historically joins xoilacz.vip; normalize to this base.
    normalized = {}
    for idx, page in pages_html.items():
        try:
            path = urlsplit(page).path
        except Exception:
            path = page
        normalized[idx] = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    return normalized, commentators, headers


def fetch_xoilac_sources(candidates):
    relevant = [item for item in candidates if provider_key(item["match"]) == "xoilacxth"]
    if not relevant:
        return {}
    pages = {}
    listed_commentators = {}
    chosen_headers = {}
    for base in ("https://xoilacz.vip", "https://xlz.domainkqt.cc"):
        domain_pages, domain_commentators, headers = _xoilac_listing_for_domain(base, relevant)
        if domain_pages:
            pages.update({k: v for k, v in domain_pages.items() if k not in pages})
            listed_commentators.update({k: v for k, v in domain_commentators.items() if k not in listed_commentators})
            for idx in domain_pages:
                chosen_headers[idx] = headers
        if len(pages) >= len(relevant):
            break
    if not pages:
        return {}

    page_htmls = {}
    with ThreadPoolExecutor(max_workers=min(8, len(pages))) as ex:
        fmap = {}
        for idx, url in pages.items():
            headers = chosen_headers.get(idx) or {"User-Agent": USER_AGENT, "Referer": url}
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
                # Only bind a per-stream caster when the listing API explicitly gave
                # commentator names and this source label matches exactly one of them.
                # Otherwise leave it empty and use the match-level commentator from
                # sport.json, exactly like SportStream does for the match card.
                caster = ""
                people = listed_commentators.get(idx, [])
                if people:
                    caster = people_match_from_evidence(people, [label])
                obj = source_obj(
                    stream,
                    caster=caster,
                    fmt=fmt,
                    headers={"User-Agent": USER_AGENT, "Referer": page_url},
                    name=label,
                    index=order,
                )
                result.setdefault(idx, []).append(obj)
    return {idx: dedupe_sources(srcs) for idx, srcs in result.items() if srcs}


def provider_special_source_caster(source, match):
    if provider_key(match) != "phalang":
        return ""

    known = match_known_people(match)
    explicit = explicit_source_people(source)
    if len(explicit) == 1:
        return explicit[0]
    if len(explicit) > 1:
        matched = people_match_from_evidence(explicit, [source.get("name"), source.get("label"), source.get("title"), source.get("server"), source.get("provider")])
        return matched

    evidence = [source.get("name"), source.get("label"), source.get("title"), source.get("server"), source.get("provider")]
    matched = people_match_from_evidence(known, evidence)
    if matched:
        return matched

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
        if not prefixed and len(human.split()) > 5:
            continue
        return human

    if len(known) == 1:
        return known[0]
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

    special = provider_special_source_caster(source, match)
    if special:
        return special

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

    # Exact SportStream fallback. MainActivity reads the match-level field using
    # commentator -> blv -> caster and shows it on the match card. It does not
    # discard the value merely because there are several names.
    return direct_match_commentator(match)

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
    direct_indexes = fetch_direct_indexes()
    giovang_map = fetch_giovang_sources(candidates)
    xoilac_map = fetch_xoilac_sources(candidates)
    resolver_to_matches = {}
    local_direct_map = {}
    provider_direct_map = {}
    for item in candidates:
        match = item["match"]
        local_direct_map[item["api_index"]] = direct_sources(match)
        provider_direct_map[item["api_index"]] = direct_sources_for_match(match, direct_indexes)
        if item["api_index"] in giovang_map:
            provider_direct_map[item["api_index"]] = giovang_map[item["api_index"]]
        elif item["api_index"] in xoilac_map:
            provider_direct_map[item["api_index"]] = xoilac_map[item["api_index"]]
        url = resolver_url(match)
        if url and not provider_direct_map[item["api_index"]]:
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
        exact = provider_direct_map.get(item["api_index"], [])
        local = local_direct_map.get(item["api_index"], [])
        url = resolver_url(match)
        remote = resolver_sources(bodies.get(url), match) if url and not exact else []
        # Direct provider API wins because it preserves the caster<->stream relationship.
        sources = exact if exact else dedupe_sources(remote + local)
        sources = order_sources(sources, match)
        if sources:
            resolved.append({**item, "sources": sources, "direct_provider": bool(exact)})
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
    data = fetch_json(SPORT_API, timeout=10, retries=1, cache_bust=True)
    if not isinstance(data, dict):
        data = fetch_json(SPORT_API, timeout=10, retries=2, cache_bust=False)
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
    print(f"v26-direct-meta: {total_streams} luồng / {total_matches} trận / {len(provider_sequence)} nguồn")


if __name__ == "__main__":
    try:
        build_playlist()
    except Exception as exc:
        print(f"build.py: {exc}", file=sys.stderr)
        sys.exit(1)
