import datetime, json, re, requests, textwrap
from telegram import InlineQueryResultArticle, InputTextMessageContent
from telegram.constants import ParseMode
from uuid import uuid4
from html.parser import HTMLParser


class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.fed = []

    def handle_data(self, data):
        self.fed.append(data)

    def get_data(self):
        return "".join(self.fed)


def strip_tags(html):
    if not html:
        return "[nothing here]"
    s = MLStripper()
    s.feed(html)
    x = s.get_data()
    print("stripped:", x)
    return x.strip()


TG_MESSAGE_LIMIT = 4096


def fit_message(message, description):
    if len(message) <= TG_MESSAGE_LIMIT:
        return message
    placeholder = "[...]"
    overflow = len(message) - TG_MESSAGE_LIMIT
    keep = max(0, len(description) - overflow - len(placeholder))
    truncated = description[:keep].rstrip() + placeholder
    return message.replace(description, truncated, 1)


url = "https://graphql.anilist.co"


def aniquery(qry: str, vars: dict):
    return requests.post(url, json={"query": qry, "variables": vars}).json()


class AniListError(Exception):
    pass


def extract(res, *path):
    """Dig into res["data"][path...], raising AniListError with the API's
    message if the response carries no data (e.g. the API being disabled)."""
    data = res.get("data") if isinstance(res, dict) else None
    if data is None:
        errors = (res or {}).get("errors") or []
        msg = "; ".join(
            e.get("message", "unknown error") for e in errors
        ) or "AniList returned no data"
        raise AniListError(msg)
    node = data
    for key in path:
        node = node[key]
    return node


def anilist_disabled(res):
    """True when AniList answered with no data at all (as opposed to a
    query-level error), which is what it does while the API is switched off."""
    return not isinstance(res, dict) or res.get("data") is None


as_url = "https://animeschedule.net/api/v3"
as_img = "https://img.animeschedule.net/production/assets/public/img"
AS_PAGE = 18  # fixed page size of /anime

_as_status = {
    "finished": "FINISHED",
    "ongoing": "RELEASING",
    "delayed": "RELEASING",
    "upcoming": "NOT_YET_RELEASED",
}

_as_epoch = "0001-01-01"


def as_get(path, **params):
    r = requests.get(f"{as_url}/{path}", params=params, timeout=20)
    try:
        return r.json()
    except ValueError:
        # a search with no hits (or a page past the end) is a plain-text
        # "Something went wrong" with a 500. thanks, animeschedule.
        if r.status_code in (200, 500) and ("q" in params or "anilist-ids" in params):
            return {"page": 1, "totalAmount": 0, "anime": []}
        raise AniListError(f"(animeschedule fallback) HTTP {r.status_code}")


def _as_time(s):
    if not s or s.startswith(_as_epoch):
        return None
    return datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))


def _as_next_episode(a):
    """Timetables need an API token, so we guess the next airing from the
    weekday of the last known sub/jpn air time. No episode number, sadly."""
    if a.get("delayedTimetable") and _as_time(a.get("delayedUntil")):
        until = _as_time(a["delayedUntil"])
        secs = (until - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
        if secs > 0:
            return [{"timeUntilAiring": int(secs), "episode": None}]
    t = _as_time(a.get("subTime")) or _as_time(a.get("jpnTime"))
    if t is None:
        return []
    now = datetime.datetime.now(datetime.timezone.utc)
    nxt = now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
    nxt += datetime.timedelta(days=(t.weekday() - nxt.weekday()) % 7)
    if nxt <= now:
        nxt += datetime.timedelta(days=7)
    return [{"timeUntilAiring": int((nxt - now).total_seconds()), "episode": None}]


def as_media(a):
    names = a.get("names") or {}
    img = a.get("imageVersionRoute")
    types = [t["name"] for t in a.get("mediaTypes") or []]
    genres = [g["name"] for g in a.get("genres") or []]
    status = _as_status.get((a.get("status") or "").lower(), (a.get("status") or "?").upper())
    return {
        "id": a.get("id"),
        "title": {
            "romaji": names.get("romaji") or a.get("title"),
            "native": names.get("native"),
            "english": names.get("english"),
        },
        "type": "ANIME",
        "format": (types[0] if types else "?").upper(),
        "status": status,
        "description": a.get("description") or "",
        "season": ((a.get("season") or {}).get("season") or "").upper() or None,
        "startDate": {"year": a.get("year")},
        "episodes": a.get("episodes"),
        "duration": a.get("lengthMin"),
        "coverImage": {
            "medium": f"{as_img}/{img}?w=200" if img else None,
            "large": f"{as_img}/{img}" if img else None,
        },
        "genres": genres,
        "isAdult": any(g.lower() in ("hentai", "ecchi") for g in genres),
        "tags": [{"name": s["name"]} for s in a.get("studios") or []]
        + [{"name": s["name"]} for s in a.get("sources") or []],
        "airingSchedule": {
            "nodes": _as_next_episode(a) if status == "RELEASING" else []
        },
    }


def as_search(terms, page=1, per_page=5):
    # animeschedule pages by 18, we page by 5; fetch the page that covers ours
    start = (page - 1) * per_page
    res = as_get("anime", q=terms, mt="any", page=start // AS_PAGE + 1)
    off = start % AS_PAGE
    items = (res.get("anime") or [])[off : off + per_page]
    return {"data": {"Page": {"media": [as_media(a) for a in items]}}}


def as_by_anilist_id(id):
    res = as_get("anime", **{"anilist-ids": id})
    items = res.get("anime") or []
    if not items:
        raise AniListError(f"(animeschedule fallback) nothing with AniList id {id}")
    return {"data": {"Media": as_media(items[0])}}


def as_from_raw_args(args: str):
    """`one` queries pass raw AniList Media() arguments; we can only honour
    `id:` and `search:` out of those."""
    m = re.search(r'search\s*:\s*"((?:[^"\\]|\\.)*)"', args)
    if m:
        res = as_search(json.loads(f'"{m.group(1)}"'), per_page=1)
        media = res["data"]["Page"]["media"]
        if not media:
            raise AniListError("(animeschedule fallback) no results")
        return {"data": {"Media": media[0]}}
    m = re.search(r"id\s*:\s*(\d+)", args)
    if m:
        return as_by_anilist_id(m.group(1))
    raise AniListError(
        "AniList is down and the fallback only understands id:/search:"
    )


# --- kitsu (characters only) ---

kitsu_url = "https://kitsu.app/api/edge"

_kitsu_status = {
    "finished": "FINISHED",
    "current": "RELEASING",
    "upcoming": "NOT_YET_RELEASED",
    "unreleased": "NOT_YET_RELEASED",
    "tba": "NOT_YET_RELEASED",
}


def kitsu_get(path, **params):
    res = requests.get(
        f"{kitsu_url}/{path}",
        params=params,
        headers={"Accept": "application/vnd.api+json"},
        timeout=20,
    ).json()
    if "data" not in res:
        errors = res.get("errors") or []
        msg = "; ".join(
            e.get("detail") or e.get("title") or "unknown error" for e in errors
        )
        raise AniListError(f"(kitsu fallback) {msg or 'no data'}")
    return res


def _kitsu_index(res):
    return {(i["type"], i["id"]): i for i in res.get("included", [])}


def _kitsu_rel_ids(item, name):
    rel = (item.get("relationships") or {}).get(name) or {}
    data = rel.get("data")
    if data is None:
        return []
    return data if isinstance(data, list) else [data]


def kitsu_media(item, included):
    a = item["attributes"]
    titles = a.get("titles") or {}
    poster = a.get("posterImage") or {}
    year = (a.get("startDate") or "")[:4]
    genres = [
        included[("categories", r["id"])]["attributes"]["title"]
        for r in _kitsu_rel_ids(item, "categories")
        if ("categories", r["id"]) in included
    ]
    return {
        "id": int(item["id"]),
        "title": {
            "romaji": titles.get("en_jp") or a.get("canonicalTitle"),
            "native": titles.get("ja_jp"),
            "english": titles.get("en") or titles.get("en_us"),
        },
        "type": item["type"].upper(),
        "format": (a.get("subtype") or "?").upper(),
        "status": _kitsu_status.get(a.get("status"), (a.get("status") or "?").upper()),
        "description": a.get("synopsis") or a.get("description") or "",
        "season": None,
        "startDate": {"year": int(year) if year.isdigit() else None},
        "episodes": a.get("episodeCount"),
        "duration": a.get("episodeLength"),
        "coverImage": {
            "medium": poster.get("small") or poster.get("medium"),
            "large": poster.get("large") or poster.get("original"),
        },
        "genres": genres,
        "isAdult": bool(a.get("nsfw")),
        "tags": [],
        "airingSchedule": {"nodes": []},
    }


def kitsu_character(item):
    a = item["attributes"]
    names = a.get("names") or {}
    full = a.get("canonicalName") or a.get("name") or "?"
    first, _, last = full.partition(" ")
    image = a.get("image") or {}
    mal = a.get("malId")
    return {
        "name": {
            "first": first,
            "last": last,
            "native": names.get("ja_jp"),
            "alternative": a.get("otherNames") or [],
            "full": full,
        },
        "image": {
            "medium": image.get("small") or image.get("medium"),
            "large": image.get("large") or image.get("original"),
        },
        "description": a.get("description") or "",
        "siteUrl": (
            f"https://myanimelist.net/character/{mal}"
            if mal
            else f"https://kitsu.app/characters/{a.get('slug') or item['id']}"
        ),
    }


def kitsu_characters(terms, page=1, per_page=10, with_media=False):
    params = {
        "filter[name]": terms,
        "page[limit]": per_page,
        "page[offset]": (page - 1) * per_page,
    }
    if with_media:
        params["include"] = "mediaCharacters.media.categories"
        params["fields[categories]"] = "title"
    res = kitsu_get("characters", **params)
    included = _kitsu_index(res)
    characters = []
    for c in res["data"]:
        char = kitsu_character(c)
        if with_media:
            nodes = []
            for mc in _kitsu_rel_ids(c, "mediaCharacters"):
                mc = included.get(("mediaCharacters", mc["id"]))
                if not mc:
                    continue
                for m in _kitsu_rel_ids(mc, "media"):
                    m = included.get((m["type"], m["id"]))
                    if m and m["type"] == "anime":
                        nodes.append(kitsu_media(m, included))
                if len(nodes) >= 2:
                    break
            char["media"] = {"nodes": nodes[:2]}
        characters.append(char)
    return {"data": {"Page": {"characters": characters}}}


def with_fallback(res, fallback):
    """If AniList is down, run `fallback()` for an answer from elsewhere.
    A failure in the fallback becomes an AniListError (handled by callers)."""
    if not anilist_disabled(res) or fallback is None:
        return res
    try:
        return fallback()
    except AniListError:
        raise
    except Exception as e:
        raise AniListError(f"AniList is down and the fallback failed: {e}")


def error_response(terms, msg):
    return [
        InlineQueryResultArticle(
            id=str(uuid4()),
            title=f"AniList request failed for '{terms}'",
            input_message_content=InputTextMessageContent(
                f"AniList is unhappy: {msg}"
            ),
        )
    ]


def charquery_render(s, extras: dict):
    terms = s
    mquery = """
    query {
        Page(page:%d, perPage:10) {
            characters(search: %s) {
                name {
                    first
                    last
                    native
                    alternative
                    full
                }
                image {
                    medium
                    large
                }
                description(asHtml:true)
                siteUrl
            }
        }
    }
    """ % (
        extras.get("page", 1),
        json.dumps(s),
    )
    res = simple_query(litquery=mquery)
    print("got result", res)
    try:
        res = with_fallback(
            res, lambda: kitsu_characters(s, page=extras.get("page", 1))
        )
        characters = extract(res, "Page", "characters")
    except AniListError as e:
        return error_response(terms, str(e))
    responses = [
        InlineQueryResultArticle(
            id=str(uuid4()),
            title=f"{'R' if len(characters) else 'There were no r'}esults for query '{terms}'",
            input_message_content=InputTextMessageContent("Y u clickin' this?"),
        )
    ]
    for c in characters:
        desc = strip_tags(c["description"])
        responses.append(
            InlineQueryResultArticle(
                id=str(uuid4()),
                title=c["name"]["full"],
                thumbnail_url=c["image"]["medium"],
                input_message_content=InputTextMessageContent(
                    fit_message(
                        (
                            f"<b>{c['name']['last']}, {c['name']['first']} ({c['name']['full']})</b>\n"
                            + f"Native name: {c['name']['native']}\n"
                            + f"Other names: {', '.join(c['name']['alternative'] or ['No other name'])}\n"
                            + "\n"
                            + f"{desc}\n"
                            + f"<a href=\"{c['image']['large']}\"> Image</a>, <a href=\"{c['siteUrl']}\"> Anilist Page </a>"
                        ),
                        desc,
                    ),
                    parse_mode="HTML",
                ),
            )
        )
    return responses


def cquery_render(s, extras: dict):
    terms = s
    mquery = """
    query {
        Page(page:%d, perPage:5) {
            characters(search: %s) {
                name { full }
                media(perPage: 2) {
                    nodes {
                        id
                        title {
                            romaji
                            native
                            english
                        }
                        type
                        format
                        status
                        description
                        season
                        startDate { year }
                        episodes
                        duration
                        coverImage {
                            medium
                            large
                        }
                        genres
                        isAdult
                        tags { name }
                        airingSchedule {
                            nodes {
                                timeUntilAiring
                                episode
                            }
                        }
                    }
                }
            }
        }
    }
    """ % (
        extras.get("page", 1),
        json.dumps(s),
    )
    print("query is", mquery)
    media = simple_query(litquery=mquery)
    print("Got result", media)
    try:
        media = with_fallback(
            media,
            lambda: kitsu_characters(
                s, page=extras.get("page", 1), per_page=5, with_media=True
            ),
        )
        characters = extract(media, "Page", "characters")
    except AniListError as e:
        return error_response(terms, str(e))
    media = [
        (textwrap.shorten(x["name"]["full"], width=15, placeholder="..."), y)
        for x in characters
        for y in x["media"]["nodes"]
    ]
    responses = [
        InlineQueryResultArticle(
            id=str(uuid4()),
            title=f"{'R' if len(media) else 'There were no r'}esults for query '{terms}'",
            input_message_content=InputTextMessageContent("Y u clickin' this?"),
        )
    ]

    def timefmt(t):
        if not t:
            return "???"
        if t < 3600:
            return "about an hour or so"
        if t < 24 * 3600:
            return f"about {t/3600} hours or so"
        return f"{int(t/(3600*24))} days"

    def nextEpisode(episodes: list):
        eps, time = None, None
        for ex in episodes:
            if ex["timeUntilAiring"] > 0:
                eps = ex["episode"]
                time = ex["timeUntilAiring"]
                break

        return f"episode {eps or '???'} in {timefmt(time)}"

    for n, m in media:
        desc = strip_tags(m["description"])
        responses.append(
            InlineQueryResultArticle(
                id=str(uuid4()),
                title=(
                    lambda t: f"[{n}] {'[🌶] ' if m['isAdult'] else ''}[{m['format']}] {t['english'] or t['romaji']}"
                )(m["title"]),
                thumbnail_url=m["coverImage"]["medium"],
                input_message_content=InputTextMessageContent(
                    fit_message(
                        (
                            f"<b>{m['title']['english'] or m['title']['romaji']} ({m['startDate']['year']})</b>\n"
                            + f"Original name: {m['title']['native']}\n"
                            + f"Romaji name: {m['title']['romaji']}\n"
                            + f"Status: {m['status']}\n"
                            + f"Genres: {', '.join(m.get('genres', None) or ['Nothing'])}\n"
                            + f"Tags: {', '.join(i['name'] for i in (m.get('tags', []))) or 'Nothing'}\n"
                            + f"Total episode count: {m['episodes']}\n"
                            + (
                                f"Next episode: {nextEpisode(m['airingSchedule']['nodes'])}\n"
                                if m["status"] == "RELEASING"
                                else ""
                            )
                            + "\nHere be dragons\n"
                            + f"Description: {desc}\n"
                            + f"<a href=\"{m['coverImage']['large']}\"> Cover Image </a>"
                        ),
                        desc,
                    ),
                    parse_mode="HTML",
                ),
            )
        )
    return responses


def qquery_render(s, extras: dict):
    terms = s
    media = simple_query(_query=s, page=extras.get("page", 1))
    print("Got result", media)
    try:
        media = with_fallback(media, lambda: as_from_raw_args(s))
        media = [extract(media, "Media")]
    except AniListError as e:
        return error_response(s, str(e))
    responses = [
        InlineQueryResultArticle(
            id=str(uuid4()),
            title=f"{'R' if len(media) else 'There were no r'}esults for query '{terms}'",
            input_message_content=InputTextMessageContent("Y u clickin' this?"),
        )
    ]

    def timefmt(t):
        if not t:
            return "???"
        if t < 3600:
            return "about an hour or so"
        if t < 24 * 3600:
            return f"about {t/3600} hours or so"
        return f"{int(t/(3600*24))} days"

    def nextEpisode(episodes: list):
        eps, time = None, None
        for ex in episodes:
            if ex["timeUntilAiring"] > 0:
                eps = ex["episode"]
                time = ex["timeUntilAiring"]
                break

        return f"episode {eps or '???'} in {timefmt(time)}"

    for m in media:
        desc = strip_tags(m["description"])
        responses.append(
            InlineQueryResultArticle(
                id=str(uuid4()),
                title=(
                    lambda t: f"{'[🌶] ' if m['isAdult'] else ''}[{m['format']}] {t['english'] or t['romaji']}"
                )(m["title"]),
                thumbnail_url=m["coverImage"]["medium"],
                input_message_content=InputTextMessageContent(
                    fit_message(
                        (
                            f"<b>{m['title']['english'] or m['title']['romaji']} ({m['startDate']['year']})</b>\n"
                            + f"Original name: {m['title']['native']}\n"
                            + f"Romaji name: {m['title']['romaji']}\n"
                            + f"Status: {m['status']}\n"
                            + f"Genres: {', '.join(m.get('genres', None) or ['Nothing'])}\n"
                            + f"Tags: {', '.join(i['name'] for i in (m.get('tags', []))) or 'Nothing'}\n"
                            + f"Total episode count: {m['episodes']}\n"
                            + (
                                f"Next episode: {nextEpisode(m['airingSchedule']['nodes'])}\n"
                                if m["status"] == "RELEASING"
                                else ""
                            )
                            + "\nHere be dragons\n"
                            + f"Description: {desc}\n"
                            + f"<a href=\"{m['coverImage']['large']}\"> Cover Image </a>"
                        ),
                        desc,
                    ),
                    parse_mode="HTML",
                ),
            )
        )
    return responses


def iquery_render(id, extras: dict):
    terms = id
    media = simple_query(_query=f"id:{id}", page=extras.get("page", 1))
    print("Got result", media)
    try:
        media = with_fallback(media, lambda: as_by_anilist_id(id))
        media = [extract(media, "Media")]
    except AniListError as e:
        return error_response(id, str(e))
    responses = [
        InlineQueryResultArticle(
            id=str(uuid4()),
            title=f"{'R' if len(media) else 'There were no r'}esults for query '{terms}'",
            input_message_content=InputTextMessageContent("Y u clickin' this?"),
        )
    ]

    def timefmt(t):
        if not t:
            return "???"
        if t < 3600:
            return "about an hour or so"
        if t < 24 * 3600:
            return f"about {t/3600} hours or so"
        return f"{int(t/(3600*24))} days"

    def nextEpisode(episodes: list):
        eps, time = None, None
        for ex in episodes:
            if ex["timeUntilAiring"] > 0:
                eps = ex["episode"]
                time = ex["timeUntilAiring"]
                break

        return f"episode {eps or '???'} in {timefmt(time)}"

    for m in media:
        desc = strip_tags(m["description"])
        responses.append(
            InlineQueryResultArticle(
                id=str(uuid4()),
                title=(
                    lambda t: f"{'[🌶] ' if m['isAdult'] else ''}[{m['format']}] {t['english'] or t['romaji']}"
                )(m["title"]),
                thumbnail_url=m["coverImage"]["medium"],
                input_message_content=InputTextMessageContent(
                    fit_message(
                        (
                            f"<b>{m['title']['english'] or m['title']['romaji']} ({m['startDate']['year']})</b>\n"
                            + f"Original name: {m['title']['native']}\n"
                            + f"Romaji name: {m['title']['romaji']}\n"
                            + f"Status: {m['status']}\n"
                            + f"Genres: {', '.join(m.get('genres', None) or ['Nothing'])}\n"
                            + f"Tags: {', '.join(i['name'] for i in (m.get('tags', []))) or 'Nothing'}\n"
                            + f"Total episode count: {m['episodes']}\n"
                            + (
                                f"Next episode: {nextEpisode(m['airingSchedule']['nodes'])}\n"
                                if m["status"] == "RELEASING"
                                else ""
                            )
                            + "\nHere be dragons\n"
                            + f"Description: {desc}\n"
                            + f"<a href=\"{m['coverImage']['large']}\"> Cover Image </a>"
                        ),
                        desc,
                    ),
                    parse_mode="HTML",
                ),
            )
        )
    return responses


def squery_render(terms: str, extras: dict):
    media = simple_query(terms, page=extras.get("page", 1))
    print("Got result", media)
    try:
        media = with_fallback(
            media, lambda: as_search(terms, page=extras.get("page", 1))
        )
        media = extract(media, "Page", "media")
    except AniListError as e:
        return error_response(terms, str(e))
    responses = [
        InlineQueryResultArticle(
            id=str(uuid4()),
            title=f"{'R' if len(media) else 'There were no r'}esults for query '{terms}'",
            input_message_content=InputTextMessageContent("Y u clickin' this?"),
        )
    ]

    def timefmt(t):
        if not t:
            return "???"
        if t < 3600:
            return "about an hour or so"
        if t < 24 * 3600:
            return f"about {t/3600} hours or so"
        return f"{int(t/(3600*24))} days"

    def nextEpisode(episodes: list):
        eps, time = None, None
        for ex in episodes:
            if ex["timeUntilAiring"] > 0:
                eps = ex["episode"]
                time = ex["timeUntilAiring"]
                break

        return f"episode {eps or '???'} in {timefmt(time)}"

    for m in media:
        desc = strip_tags(m["description"])
        responses.append(
            InlineQueryResultArticle(
                id=str(uuid4()),
                title=(
                    lambda t: f"{'[🌶] ' if m['isAdult'] else ''}[{m['format']}] {t['english'] or t['romaji']}"
                )(m["title"]),
                thumbnail_url=m["coverImage"]["medium"],
                input_message_content=InputTextMessageContent(
                    fit_message(
                        (
                            f"<b>{m['title']['english'] or m['title']['romaji']} ({m['startDate']['year']})</b>\n"
                            + f"Original name: {m['title']['native']}\n"
                            + f"Romaji name: {m['title']['romaji']}\n"
                            + f"Status: {m['status']}\n"
                            + f"Genres: {', '.join(m.get('genres', None) or ['Nothing'])}\n"
                            + f"Tags: {', '.join(i['name'] for i in (m.get('tags', []))) or 'Nothing'}\n"
                            + f"Total episode count: {m['episodes']}\n"
                            + (
                                f"Next episode: {nextEpisode(m['airingSchedule']['nodes'])}\n"
                                if m["status"] == "RELEASING"
                                else ""
                            )
                            + "\nHere be dragons\n"
                            + f"Description: {desc}\n"
                            + f"<a href=\"{m['coverImage']['large']}\"> Cover Image </a>"
                        ),
                        desc,
                    ),
                    parse_mode="HTML",
                ),
            )
        )
    return responses


def simple_query(terms=None, _query=None, litquery=None, page=1):
    if litquery:
        return aniquery(litquery, {})
    return aniquery(
        (
            """
        query($page: Int, $perPage: Int, $search: String) {
            Page (page: $page, perPage: $perPage) {
                media(search: $search) {
                    id
                    title {
                        romaji
                        native
                        english
                    }
                    type
                    format
                    status
                    description
                    season
                    startDate { year }
                    episodes
                    duration
                    coverImage {
                        medium
                        large
                    }
                    genres
                    isAdult
                    tags { name }
                    airingSchedule {
                        nodes {
                            timeUntilAiring
                            episode
                        }
                    }
                }
            }
        }
        """
            if _query is None
            else """
            query {
                Media("""
            + _query
            + """) {
                    id
                    title {
                        romaji
                        native
                        english
                    }
                    type
                    format
                    status
                    description
                    season
                    startDate { year }
                    episodes
                    duration
                    coverImage {
                        medium
                        large
                    }
                    genres
                    isAdult
                    tags { name }
                    airingSchedule {
                        nodes {
                            timeUntilAiring
                            episode
                        }
                    }
                }
            }
        """
        ),
        dict(search=terms, page=page, perPage=5) if _query is None else {},
    )
