import json, requests
from telegram import (
    InlineQueryResultArticle, ParseMode, InputTextMessageContent
)
from uuid import uuid4

url = 'https://graphql.anilist.co'

def query(qry: str, vars: dict):
    return requests.post(url, json={'query': qry, 'variables': vars}).json()

def squery_render(terms: str):
    media = simple_query(terms)
    print('Got result', media)
    media = media['data']['Page']['media']
    responses = [
        InlineQueryResultArticle(
            id=uuid4(),
            title=f"{'R' if len(media) else 'There were no r'}esults for query '{terms}'",
            input_message_content=InputTextMessageContent('Y u clickin\' this?')
        )
    ]
    def timefmt(t):
        if not t:
            return '???'
        return f'{t} seconds'
    def nextEpisode(episodes: list):
        eps, time = None, None
        for ex in episodes:
            if ex['timeUntilAiring'] > 0:
                eps = ex['episode']
                time = ex['timeUntilAiring']
                break
        return f"{eps} in {timefmt(time)}"

    for m in media:
        responses.append(
            InlineQueryResultArticle(
                id=uuid4(),
                title=(lambda t: f"{'[🌶] ' if m['isAdult'] else ''}{t['english'] or t['romaji']} ({t['native']})")(m['title']),
                thumb_url=m['coverImage']['medium'],
                input_message_content=InputTextMessageContent(
                    (f"<b>{m['title']['english'] or m['title']['romaji']} ({m['startDate']['year']})</b>\n" +
                     f"Original name: {m['title']['native']}\n" +
                     f"Status: {m['status']}\n" +
                     f"Genres: {', '.join(m['genres'])}\n" +
                     f"Total episode count: {m['episodes']}\n" +
                     (f"Next episode: {nextEpisode(m['airingSchedule'])}\n" if m['status'] == 'RELEASING' else '') +
                     '\n<Here be dragons>\n' +
                     f"Description: m['description']\n" +
                     f"<a href=\"{m['coverImage']['large']}\"> Cover Image </a>"
                    ),
                    parse_mode='HTML')
            )
        )
    return responses

def simple_query(terms: str):
    return query(
        '''
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
                    airingSchedule {
                        nodes {
                            timeUntilAiring
                            episode
                        }
                    }
                }
            }
        }
        ''',
        dict(search=terms, page=1, perPage=5)
    )