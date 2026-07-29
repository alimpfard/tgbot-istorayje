import json, requests, textwrap
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
    terms = id
    media = simple_query(_query=s, page=extras.get("page", 1))
    print("Got result", media)
    try:
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
