import json, requests
from telegram import (
    InlineQueryResultArticle, ParseMode, InputTextMessageContent
)
from uuid import uuid4
from html.parser import HTMLParser


class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    if not html:
        return '[nothing here]'
    s = MLStripper()
    s.feed(html)
    x = s.get_data()
    print('stripped:', x)
    return x.strip()

url = 'https://graphql.anilist.co'

def aniquery(qry: str, vars: dict):
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
        if t < 3600:
            return 'about an hour or so'
        if t < 24*3600:
            return f'about {t/3600} hours or so'
        return f'{int(t/(3600*24))} days'

    def nextEpisode(episodes: list):
        eps, time = None, None
        for ex in episodes:
            if ex['timeUntilAiring'] > 0:
                eps = ex['episode']
                time = ex['timeUntilAiring']
                break

        return f"episode {eps or '???'} in {timefmt(time)}"

    for m in media:
        responses.append(
            InlineQueryResultArticle(
                id=uuid4(),
                title=(lambda t: f"{'[🌶] ' if m['isAdult'] else ''}[{m['format']}] {t['english'] or t['romaji']}")(m['title']),
                thumb_url=m['coverImage']['medium'],
                input_message_content=InputTextMessageContent(
                    (f"<b>{m['title']['english'] or m['title']['romaji']} ({m['startDate']['year']})</b>\n" +
                     f"Original name: {m['title']['native']}\n" +
                     f"Status: {m['status']}\n" +
                     f"Genres: {', '.join(m['genres'])}\n" +
                     f"Total episode count: {m['episodes']}\n" +
                     (f"Next episode: {nextEpisode(m['airingSchedule']['nodes'])}\n" if m['status'] == 'RELEASING' else '') +
                     '\nHere be dragons\n' +
                     f"Description: {strip_tags(m['description'])}\n" +
                     f"<a href=\"{m['coverImage']['large']}\"> Cover Image </a>"
                    ),
                    parse_mode='HTML')
            )
        )
    return responses

def simple_query(terms: str):
    return aniquery(
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