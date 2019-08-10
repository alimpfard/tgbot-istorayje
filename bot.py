from telegram.ext import (
    Updater, CommandHandler, InlineQueryHandler,
    MessageHandler,
    Filters
)
from telegram import (
    InlineQueryResultArticle, ParseMode,
    InputTextMessageContent,
    InlineQueryResultCachedDocument, InlineQueryResultCachedPhoto, InlineQueryResultCachedGif,
    InlineQueryResultCachedMpeg4Gif, InlineQueryResultCachedSticker, Sticker
)
from telegram.ext import ChosenInlineResultHandler
# from googlecloud import getCloudAPIDetails
from googleimgsearch import searchGoogleImages
from trace import getTraceAPIDetails
from extern import pke_tagify

from db import DB
import re
import os
from io import BytesIO
from uuid import uuid4, UUID
import traceback
import json
import xxhash
import pickle
from threading import Event
from time import time
import random
from datetime import timedelta


def get_any(obj, lst):
    for prop in lst:
        if hasattr(obj, prop):
            mm = getattr(obj, prop, None)
            if mm is not None:
                return mm
    return None


class IstorayjeBot:
    def __init__(self, token, db: DB):
        self.random = random.Random()
        self.token = token
        self.updater = Updater(token)
        self.db = db
        for handler in self.create_handlers():
            self.register_handler(handler)
        self.updater.dispatcher.add_error_handler(self.error)

        if not self.restore_jobs():
            # TODO: fix save_jobs fucking up
            # self.updater.job_queue.run_repeating(
                # self.save_jobs, timedelta(minutes=5))
            self.updater.job_queue.run_repeating( # it will re-add itself based on load if more is required
                self.process_insertions, timedelta(minutes=1))

        self.context = {}

    def _dumpjobs(self, jq):
        if jq:
            job_tuples = jq._queue.queue
        else:
            job_tuples = []

        res_bins = []
        for next_t, job in job_tuples:
            # Back up objects
            _job_queue = job._job_queue
            _remove = job._remove
            _enabled = job._enabled

            # Replace un-pickleable threading primitives
            job._job_queue = None  # Will be reset in jq.put
            removed = job.removed  # Convert to boolean
            enabled = job.enabled  # Convert to boolean
            job._enabled = enabled
            job._remove = removed

            # Pickle the job
            res_bins.append(pickle.dumps((next_t, job)))

            # Restore objects
            job._job_queue = _job_queue
            job._remove = _remove
            job._enabled = _enabled

        return res_bins

    def save_jobs(self, *args, **kwargs):
        self.db.db.jobs.find_one_and_replace(
            {},
            {'data': pickle.dumps(self._dumpjobs(self.updater.job_queue))},
            upsert=True
        )

    def restore_jobs(self):
        jq = self.updater.job_queue
        now = time()
        jobs = None

        try:
            jobs = self.db.db.jobs.find_one_and_delete({})['data']
        except:
            jobs = []

        for picl in jobs:
            next_t, job = pickle.loads(picl)

            # Create threading primitives
            enabled = job._enabled
            removed = job._remove

            job._enabled = Event()
            job._remove = Event()

            if enabled:
                job._enabled.set()

            if removed:
                job._remove.set()

            next_t -= now  # Convert from absolute to relative time

            jq._put(job, next_t)

        return jobs is not None and len(jobs) > 0

    def error(self, bot, update, error):
        print(f'[Error] Update {update} caused error {error}')

    def register_handler(self, handler):
        self.updater.dispatcher.add_handler(handler)

    def start_polling(self):
        self.updater.start_polling()
        self.updater.idle()

    def start_webhook(self):
        PORT = int(os.environ.get("PORT", "8443"))
        HEROKU_APP_NAME = os.environ.get("HEROKU_APP_NAME")
        self.updater.start_webhook(
            listen="0.0.0.0", port=PORT, url_path=self.token)
        self.updater.bot.setWebhook(
            "https://{}.herokuapp.com/{}".format(HEROKU_APP_NAME, self.token))
        self.updater.idle()

    def create_handlers(self):
        return [
            CommandHandler('start', self.handle_start),
            CommandHandler('list_index', self.handle_list_index),
            CommandHandler('connect', self.start_option_set),
            CommandHandler('temp', self.set_temp),
            CommandHandler('set', self.set_option),
            CommandHandler('reverse', self.reverse_search),
            CommandHandler('reverse_fuzzy', self.reverse_search_fuzzy),
            CommandHandler('rehash', self.rehash_all),
            CommandHandler('help', self.help),
            ChosenInlineResultHandler(self.on_result_chosen),
            InlineQueryHandler(self.handle_query,
                               pass_user_data=True, pass_chat_data=True),
            MessageHandler(Filters.all, self.handle_possible_index_update)
        ]

    def handle_start(self, bot, update):
        update.message.reply_text(
            f'''Hello there General Kenobi - {update.message.from_user.id}@{update.message.chat.id}\nWe are live at {bot}'''
        )

    def on_result_chosen(self, bot, update):
        result = update.chosen_inline_result
        result_id = result.result_id
        try:
            UUID(result_id, version=4)
            return  # we don't want to cache these fuckers
        except:
            result_id = int(result_id)
        user = result.from_user.id
        coll, *_ = self.parse_query(result.query)
        print(
            f'> chosen result {result_id} for user {user} - collection {coll}')
        doc = self.db.db.storage.find_one_and_update({
            'user_id': user
        }, {
            '$inc': {
                'used_count': 1
            }
        }, return_document=True)
        last_used = doc['last_used'].get(coll, [])
        if result_id in last_used:
            return
        print(f"> {user}'s last_used: {last_used}")
        print(f'>> last used count', len(last_used))
        if len(last_used) > 5:
            count = self.db.db.storage.update_one({
                'user_id': user
            }, {
                '$pop': {f'last_used.{coll}': -1}
            }).modified_count
            print('>> evicted', count, 'entries')
        doc = self.db.db.storage.find_one_and_update({
            'user_id': user
        }, {
            '$addToSet': {
                f'last_used.{coll}': int(result_id)
            }
        }, return_document=True)
        print(f"> {user}'s last_used: {doc['last_used'][coll]}")

    def is_useful_tag(self, tag):
        print(tag)
        return re.sub(r'season|\s+', '', re.sub(r'[^A-z]', '', tag)) != ''

    def tagify_all(self, *tags):
        return list(filter(
            self.is_useful_tag,
            sum(list(list(set([x.replace(' ', '_').lower()] + x.lower().split(' ') + list(re.sub('\\W', '', y.lower()) for y in x.split(' '))))
                     for x in tags if x), [])
        ))

    def process_insertions(self, *args, timeout=150, **kwargs):
        stime = time()
        while True:
            if time() - stime >= timeout:
                break
            doc = self.db.db.tag_updates.find_one_and_delete({})
            if not doc:
                print('no more insertion')
                break
            instags = []

            if doc['service'] == 'google':
                print('google', doc)
                details = searchGoogleImages(self.updater.bot.get_file(file_id=doc['fileid'])._get_encoded_url())
                if not details:
                    resp = doc['response_id']
                    self.updater.bot.edit_message_text(
                        'Failed: google query had no results',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    continue
                req = details['descriptions']
                res = pke_tagify(req)
                if not res:
                    resp = doc['response_id']
                    self.updater.bot.edit_message_text(
                        'Failed: google query had no usable tags',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    continue
                instags = [x[0] for x in res if x[1] >= 100*doc['similarity_cap']]

            elif doc['service'] == 'anime':

                details = getTraceAPIDetails(doc['filecontent'])
                if not details:
                    resp = doc['response_id']
                    self.updater.bot.edit_message_text(
                        'Rejected: anime query had no results',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    print('no response')
                    continue

                docv = details['docs']
                if len(docv) < 1:
                    resp = doc['response_id']
                    self.updater.bot.edit_message_text(
                        'Failed: anime query had no results',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    print('no results')
                    continue
                doclist = []
                try:
                    # what the fuck?
                    doclist = sorted(filter(lambda x: x['similarity'] >= doc['similarity_cap'], docv), key=lambda x: x['similarity'], reverse=True)
                    docv = next(iter(doclist))
                except StopIteration:
                    resp = doc['response_id']
                    self.updater.bot.edit_message_text(
                        f'Completed: anime query results below set similarity ({doc["similarity_cap"]})',
                        chat_id=resp[1],
                        message_id=resp[0], 
                    )
                    print('similarity cap hit, just use first')
                    continue
                except:
                    traceback.print_exc()
                    continue

                if not docv:
                    resp = doc['response_id']
                    self.updater.bot.edit_message_text(
                        'Failed: unknown reason',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    print('invalid response, null document')
                    continue

                res = pke_tagify([
                    docv['filename'],
                    docv['anime'],
                    docv['title_english'],
                    docv['title_romaji'],
                    *docv['synonyms']
                ])

                instags = [x[0] for x in res if x[1] >= doc['similarity_cap']]
                
                if docv['is_adult']:
                    instags.push('nsfw')

            if len(instags):
                insps = list((x[0], a['index']) for x in doc['insertion_paths'] for a in self.db.db.storage.aggregate([
                    {'$match': {'user_id': {'$in': doc['users']}}},
                    {'$project': {'index': f'$collection.{x[0]}.index'}},
                    {'$unwind': {'path': '$index', 'includeArrayIndex': 'idx'}},
                    {'$match': {'index.id': x[1]}},
                    {'$project': {'index': '$idx'}}
                ]))
                print(insps)
                if len(insps):
                    ins = {'$addToSet': {f'collection.{p[0]}.index.{p[1]}.tags': {
                        '$each': instags} for p in insps}}
                    print(ins)
                    self.db.db.storage.update_many(
                        {'user_id': {'$in': doc['users']}}, ins)
                resp = doc['response_id']
                self.updater.bot.edit_message_text(
                    f'Completed:\nadded tags: {insps}',
                    chat_id=resp[1],
                    message_id=resp[0],
                )
            else:
                resp = doc['response_id']
                self.updater.bot.edit_message_text(
                    'Completed: query had no results',
                    chat_id=resp[1],
                    message_id=resp[0],
                )
        
        if self.db.db.tag_updates.count_documents({}) == 0:
            self.updater.job_queue.run_once(self.process_insertions, timedelta(seconds=30)) # todo: based on load

    def handle_magic_tags(self, tag: str, message: object, insertion_paths: list, early: bool, users: list):
        if tag.startswith('$'):
            if early:
                return None
            tag, *targs = tag[1:].split(':')
            print('magic tag', tag, 'with args', targs)
            insert = {
                'service': '',
                'filecontent': None,
                'fileid': None,
                'dlpath': None,
                'insertion_paths': insertion_paths,
                'users': users,
            }
            if tag in ['google', 'anime']:
                doc = get_any(message, ['document', 'sticker', 'photo'])
                mime = None 
                if isinstance(doc, list):
                    doc = self.random.choice(doc)
                    mime = 'image'
                if isinstance(doc, Sticker):
                    mime = 'image'

                if not doc:
                    print('doc is null', 'from', message)
                    return None
                if not mime:
                    mime = doc.mime_type
                print('got doc', doc)

                google = tag == 'google'
                insert['similarity_cap'] = int(
                    targs[0])/100 if len(targs) else 0.6

                if any(x in mime for x in ['gif', 'mp4']):
                    if google:
                        insert['fileid'] = doc.file_id
                    else:
                        insert['filecontent'] = bytes(self.updater.bot.get_file(
                            file_id=doc.thumb.file_id).download_as_bytearray())

                elif 'image' in mime:
                    if google:
                        insert['fileid'] = doc.file_id
                    else:
                        insert['filecontent'] = bytes(self.updater.bot.get_file(
                            file_id=doc.file_id).download_as_bytearray())

                else:
                    print(mime, 'is not supported')
                    return None  # shrug

                insert['service'] = tag
            else:
                return 'unsupported_magic:' + tag

            if not insert['service']:
                return None

            rmm = message.reply_text(f'will process {tag} and edit this message with the result in a bit.')
            insert['response_id'] = [rmm.message_id, rmm.chat_id]
            self.db.db.tag_updates.insert_one(insert)
            return None
        return tag

    def handle_possible_index_update(self, bot, update):
        print('<<<', update)
        reverse = self.context.get('do_reverse', [])
        fuzz = self.context.get('fuzz_reverse', {})
        try:
            if update.message.chat.id in reverse:
                fuzzy = False
                reverse.remove(update.message.chat.id)
                self.context['do_reverse'] = reverse
                if fuzz.get(update.message.chat.id, False):
                    fuzzy = True
                fuzz.pop(update.message.chat.id, None)
                self.context['fuzz_reverse'] = fuzz
                # do a reverse document to tag search
                try:
                    mfield = 'file_id'
                    mvalue = get_any(update.message, [
                                     'document', 'sticker', 'animation', 'audio', 'video', 'photo'])
                    assert (mvalue is not None)
                    if isinstance(mvalue, list):
                        mvalue = self.random.choice(mvalue)
                    print('found item', mvalue)
                    mvalue = mvalue.file_id
                    if fuzzy:
                        update.message.reply_text(
                            'Please wait, this might take a moment'
                        )
                        mvalue = xxhash.xxh64(self.updater.bot.get_file(
                            file_id=mvalue).download_as_bytearray()).digest()
                        mfield = 'xxhash'
                    print(f'going to look at {mfield} for {mvalue}')
                    wtf = list(self.db.db.message_cache.aggregate([
                        {'$match': {mfield: mvalue}},
                        {'$project': {'file_id': 0, 'type': 0}},
                        {'$lookup': {
                            'from': 'storage',
                            'let': {
                                'chatid': '$chatid',
                                'msgid': '$msg_id'
                            },
                            'pipeline': [
                                {'$project': {'collections': {
                                    '$objectToArray': '$collection'}}},
                                {'$unwind': '$collections'},
                                {'$project': {'_id': 0}},
                                {'$project': {'elem': '$collections.v.index'}},
                                {'$unwind': '$elem'},
                                {'$project': {'id': '$elem.id', 'tags': '$elem.tags'}},
                                {'$match': {
                                    '$expr': {
                                        '$eq': ['$id', '$$msgid']
                                    }
                                }},
                                {'$project': {'tags': 1}}
                            ],
                            'as': 'tag'
                        }},
                        {'$unwind': '$tag'},
                        {'$replaceRoot': {'newRoot': '$tag'}},
                        {'$group': {'_id': "$_id", 'tags': {'$addToSet': '$tags'}}},
                        {'$project': {
                            'result': {
                                '$reduce': {
                                    'input': '$tags',
                                    'initialValue': [],
                                    'in': {
                                        '$concatArrays': ['$$value', '$$this']
                                    }
                                }
                            }
                        }},
                        {'$project': {'_id': 0}}
                    ]))

                    if len(wtf) == 0:
                        # nothing matched
                        update.message.reply_text(
                            'no documents matching your query found')
                        return

                    wtf = wtf[0]['result']
                    print(wtf)
                    update.message.reply_text(
                        'Found these tags:\n' +
                        '    ' + ' '.join(wtf)
                    )
                except:
                    traceback.print_exc()
                    pass
                return
        except:
            pass

        # probably index update...or stray message
        try:
            username = '@' + update.message.chat.username
        except:
            try:
                username = '@' + update.channel_post.chat.username
            except:
                print('uhhhh...🤷‍♀️')
        users = [x['chat'] for x in self.db.db.cindex.aggregate([
            {'$match': {'index': {'$exists': username}}},
            {'$project': {'_id': 0, 'chat': '$index.' + username}},
            {'$unwind': '$chat'}
        ])]
        try:
            msg = update.message
            _ = msg.message_id
        except:
            msg = update.channel_post
            _ = msg.message_id

        move = False
        tags = None
        delete = False
        add = False
        reset = False
        remove = False
        query = False

        try:
            text = msg.text
            if text.startswith('set:'):
                move = True
                tags = re.split(self.reg, text[4:].strip())
            elif text.startswith('^delete'):
                delete = True
            elif text.startswith('^set:'):
                reset = True
                tags = re.split(self.reg, text[5:].strip())
            elif text.startswith('^add:'):
                add = True
                tags = re.split(self.reg, text[5:].strip())
            elif text.startswith('^remove:'):
                remove = True
                tags = re.split(self.reg, text[8:].strip())
            elif tet.startswith('^tags?'):
                query = True
                users = [msg.from_user.id]
            
        except Exception:
            pass
        for user in users:
            print('> processing', user)
            try:
                mtags = tags
                collections = list(x['collection']['k'] for x in
                                   self.db.db.storage.aggregate([
                                       {'$match': {'user_id': user}},
                                       {'$project': {'_id': 0, 'collection': {
                                           '$objectToArray': '$collection'}}},
                                       {'$unwind': '$collection'},
                                       {'$match': {'collection.v.id': username}}
                                   ])
                                   )
                print('>> collections:', collections)

                if mtags:
                    m_msgid = None
                    noreply = False
                    m_msg = None
                    try:
                        m_msgid = msg.reply_to_message.message_id
                        m_msg = msg.reply_to_message
                    except:
                        noreply = True
                    mtags = list(set(x for x in [
                        self.handle_magic_tags(
                            early=noreply,
                            tag=x,
                            message=m_msg,
                            insertion_paths=[(coll, m_msgid)
                                             for coll in collections],
                            users=[user]
                        )
                        for x in mtags
                    ] if x is not None))
                if not mtags and any([move, add, reset, remove]):
                    return
                filterop = {}
                if delete:
                    print(msg)
                    try:
                        msgid = msg.reply_to_message.message_id
                        updateop = {
                            '$pull': {
                                f'collection.{coll}.index': {'id': msgid}
                                for coll in collections
                            }
                        }
                    except Exception as e:
                        print(e)
                elif move:
                    updateop = {
                        '$push': {
                            f'collection.{coll}.index': {'id': msgid, 'tags': mtags}
                            for coll in collections
                            for msgid in self.db.db.storage.find_one({'user_id': user})['collection'][coll]['temp']
                        }
                    }
                    updateop.update({
                        '$set': {
                            f'collection.{coll}.temp': []
                            for coll in collections
                        }
                    })
                elif add:
                    filterop = {
                        f'collection.{coll}.index.id': msg.reply_to_message.message_id
                        for coll in collections
                    }
                    updateop = {
                        '$push': {
                            f'collection.{coll}.index.$.tags': {'$each': mtags}
                            for coll in collections
                        }
                    }
                elif remove:
                    filterop = {
                        f'collection.{coll}.index.id': msg.reply_to_message.message_id
                        for coll in collections
                    }
                    updateop = {
                        '$pullAll': {
                            f'collection.{coll}.index.$.tags': mtags
                            for coll in collections
                        }
                    }
                elif reset:
                    filterop = {
                        f'collection.{coll}.index.id': msg.reply_to_message.message_id
                        for coll in collections
                    }
                    updateop = {
                        '$set': {
                            f'collection.{coll}.index.$.tags': mtags
                            for coll in collections
                        }
                    }
                elif query:
                    res = sum(list(x['tags'] for x in self.db.db.storage.aggregate([
                        {'$match': {'user_id': user}},
                        {'$project': {'_id': 0, 'collection': {
                            '$objectToArray': '$collection'
                        }}},
                        {'$unwind': '$collection'},
                        {'$match': {'collection.v.id': username}},
                        {'$project': {'collection.v.index': 1}},
                        {'$unwind': '$collection.v.index'},
                        {'$match': {'collection.v.index.id': msg.reply_to_message.message_id}},
                        {'$project': {'tags': '$collection.v.index.tags'}}
                    ])), [])
                    msg.reply_text(f'tags: {" ".join(res)}')
                    return
                else:
                    updateop = {
                        '$addToSet': {
                            'collection.' + coll + '.temp': msg.message_id
                            for coll in collections
                        }
                    }
                print(updateop, filterop)
                filterop.update({
                    'user_id': user,
                })
                self.db.db.storage.update_one(filterop, updateop)
            except Exception as e:
                print(e)

    def handle_list_index(self, bot, update):
        coll = self.db.db.storage.find_one(filter={
            'user_id': update.message.from_user.id
        })
        s = str(coll['collection'])
        if len(s) < 4096:
            update.message.reply_text(s)
        else:
            update.message.reply_text('You should get a json file now...')
            update.message.reply_document(document=BytesIO(
                bytes(json.dumps(coll['collection']), 'utf8')), filename="collection.json")

    reg = re.compile(r'\s+')

    def parse_query(self, query):
        coll, *queries = re.split(self.reg, query)
        return (coll, queries)

    def clone_messaage_with_data(self, data, tags):
        ty = data['type']
        if ty == 'text':
            return InlineQueryResultArticle(
                id=data['msg_id'],
                title='> ' + ', '.join(tags) +
                ' (' + str(data['msg_id']) + ')',
                input_message_content=InputTextMessageContent(
                    data['text']
                )
            )
        elif ty == 'mp4':
            return InlineQueryResultCachedMpeg4Gif(
                data['msg_id'],
                data['file_id']
            )
        elif ty == 'gif':
            return InlineQueryResultCachedGif(
                data['msg_id'],
                data['file_id']
            )
        elif ty == 'img':
            return InlineQueryResultCachedPhoto(
                data['msg_id'],
                data['file_id'],
                title='> ' + ', '.join(tags) +
                ' (' + str(data['msg_id']) + ')',
                caption=data.get('caption', None)
            )
        elif ty == 'sticker':
            return InlineQueryResultCachedSticker(
                data['msg_id'],
                data['file_id']
            )
        elif ty == 'doc':
            return InlineQueryResultCachedDocument(
                data['msg_id'],
                '> ' + ', '.join(tags) + ' (' + str(data['msg_id']) + ')',
                data['file_id']
            )
        else:
            print('unhandled msg type', ty, 'for message', data)
            return None

    def try_clone_message(self, message, tags, id=None, chid=None):
        try:
            text = message.text
            assert (text is not None)
            print('> is text')
            data = {
                'type': 'text',
                'text': text,
                'chatid': chid,
                'msg_id': id
            }
            self.db.db.message_cache.insert_one(data)
            return self.clone_messaage_with_data(data, tags)
        except:
            try:
                document = get_any(message, ['document', 'animation', 'audio', 'video', 'photo'])
                assert (document is not None)
                mime = None
                if isinstance(document, list):
                    # photo list, we're gonna take a random one for fun
                    print (document)
                    assert (len(document) > 0)
                    document = self.random.choice(document)
                    mime = 'image'
                print('> is some sort of document')
                caption = message.caption
                if not mime:
                    mime = document.mime_type
                data = {
                    'file_id': document.file_id,
                    'chatid': chid,
                    'msg_id': id,
                    'xxhash': xxhash.xxh64(self.updater.bot.get_file(file_id=document.file_id).download_as_bytearray()).digest()
                }
                if 'mp4' in mime:
                    data['type'] = 'mp4'
                elif 'gif' in mime:
                    data['type'] = 'gif'
                elif 'image' in mime:
                    data['type'] = 'img'
                    data['caption'] = caption
                else:
                    data['type'] = 'doc'
                    data['caption'] = caption
                self.db.db.message_cache.insert_one(data)
                return self.clone_messaage_with_data(data, tags)
            except:
                try:
                    sticker = message.sticker
                    assert (sticker is not None)
                    print('> is a sticker')
                    data = {
                        'type': 'sticker',
                        'file_id': sticker.file_id,
                        'chatid': message.chat.id,
                        'msg_id': message.message_id,
                    }
                    self.db.db.message_cache.insert_one(data)
                    return self.clone_messaage_with_data(data, tags)
                except Exception as e:
                    traceback.print_exc()
                    print('exception occured while processing', message, 'with tags', tags)
                    return None
        return None

    def handle_query(self, bot, update, user_data=None, chat_data=None):
        try:
            coll, query = self.parse_query(update.inline_query.query)
            if not coll or coll == '':
                return

            print(coll, query)
            if any(x in coll for x in '$./[]'):
                update.inline_query.answer(
                    [InlineQueryResultArticle(id=uuid4(), title='Invalid collection name "' + coll + '"',
                                              input_message_content=InputTextMessageContent('This user is an actual idiot'))]
                )
                return
            colls = list((x['index']['id'], x['index']['tags']) for x in
                         self.db.db.storage.aggregate([
                             {'$match': {
                                 'user_id': update.inline_query.from_user.id
                             }
                             },
                             {'$project': {
                                 "index": '$collection.' + coll + '.index',
                                 '_id': 0
                             }
                             },
                             {'$unwind': '$index'},
                             {'$match': {
                                 'index.tags': {
                                     '$all': query
                                 }
                             }
                             },
                             {'$limit': 5}
                         ]))
            results = [InlineQueryResultArticle(
                id=uuid4(),
                title='>> ' +
                ' '.join(query or ['Your', 'Recent', 'Selections']),
                input_message_content=InputTextMessageContent(
                    'Search for `' +
                    ' '.join(
                        query) + '\' and more~' if len(query) else 'Yes, these are your recents'
                )
            )]
            userdata = self.db.db.storage.find_one(
                {'user_id': update.inline_query.from_user.id})
            chatid = userdata['collection'][coll]['id']
            cachetime = 300
            if len(query) == 0:
                last_used = self.db.db.storage.find_one({
                    'user_id': update.inline_query.from_user.id
                }, projection={
                    '_id': 0,
                    'collection': 0,
                })['last_used']
                print('>>>', last_used)
                last_used = last_used.get(coll, [])
                print('>>> || ', last_used, 'in', chatid)
                for msgid in last_used:
                    msgid = int(msgid)
                    cmsg = self.db.db.message_cache.find_one({'$and':
                                                              [{'chatid': chatid}, {
                                                                  'msg_id': msgid}]
                                                              })
                    if not cmsg:
                        print('> id', msgid, 'not found...?')
                    results.append(self.clone_messaage_with_data(
                        cmsg, ['last', 'used']))
            if len(results) > 1:
                update.inline_query.answer(
                    results,
                    cache_time=20,
                    is_personal=True
                )
                return
            if len(colls) < 1:
                results.append(
                    InlineQueryResultArticle(
                        id=uuid4(),
                        title='no result matching query found',
                        input_message_content=InputTextMessageContent(
                            f'<imaginary result matching {" ".join(query)}>')
                    )
                )
                cachetime = 60
            tempid = userdata.get('temp', None)
            print(userdata)

            if not tempid:
                results.append(
                    InlineQueryResultArticle(
                        id=uuid4(),
                        title='no temp set, "/temp <temp_chat_username>" in the bot chat',
                        input_message_content=InputTextMessageContent(
                            'This user is actually dumb')
                    )
                )
                cachetime = 0
                colls = []
            else:
                tempid = tempid['id']

            for col in colls:
                try:
                    print(tempid, chatid, col)
                    cmsg = self.db.db.message_cache.find_one({'$and':
                                                              [{'chatid': chatid}, {
                                                                  'msg_id': col[0]}]
                                                              })
                    if cmsg:
                        print('cache hit for message', col[0], ':', cmsg)
                        cloned_message = self.clone_messaage_with_data(
                            cmsg, col[1])
                    else:
                        print('cache miss for message', col[0], 'trying to load it')
                        msg = bot.forward_message(
                            chat_id=tempid,
                            from_chat_id=chatid,
                            message_id=col[0],
                            disable_notification=True,
                        )
                        cloned_message = self.try_clone_message(
                            msg, col[1], id=col[0], chid=chatid)
                        print('duplicated message found:', msg)

                        msg.delete()

                    if not cloned_message:
                        print('message clone failed for', col[0])
                        continue
                    results.append(cloned_message)
                except Exception as e:
                    cachetime = 10
                    results.append(
                        InlineQueryResultArticle(
                            id=uuid4(),
                            title=f'Exception <{e}> occured while processing {col}',
                            input_message_content=InputTextMessageContent(
                                f'This bot is actually dumb\nException: {e}')
                        )
                    )
            update.inline_query.answer(
                results,
                cache_time=cachetime,
                is_personal=True
            )
        except Exception as e:
            print(e)
            traceback.print_exc()
            update.inline_query.answer([
                InlineQueryResultArticle(
                    id=uuid4(),
                    title=f'Exception <{e}> occured while processing {query} in {coll}',
                    input_message_content=InputTextMessageContent(
                        f'This bot is actually dumb\n{e}\nHint: you might be searching a nonexistent collection')
                )
            ], cache_time=10)

    def start_option_set(self, bot, update):
        # update.message.reply_text('Add this bot to a group/channel (or use this chat) and give me its ID or username')
        txt = update.message.text[len('/connect '):].strip()
        if txt == '':
            update.message.reply_text(
                'connect what? (repeat command with argument)')
            return

        if any(x in txt for x in '$./[]'):
            update.message.reply_text('Invalid collection name "' + txt + '"')
            return

        update.message.reply_text('setting option ' + txt)
        context = {}
        context['option'] = txt
        self.context[update.message.from_user.id] = context

    def set_temp(self, bot, update):
        username = update.message.text[len('/temp '):]
        if username == '':
            update.message.reply_text(
                'set temp to what? (repeat command with argument)')
            return

        self.db.db.storage.update_one({
            'user_id': update.message.from_user.id
        }, {
            '$set': {
                'temp': {
                    'id': username,
                },
            }
        }, upsert=True)

        update.message.reply_text(
            'set temp storage to ' + username
        )

    def set_option(self, bot, update):
        context = self.context.get(update.message.from_user.id, None)
        if not context or not context['option']:
            update.message.reply_text(
                'set in what context? (/connect <context>)')
            return
        username = update.message.text[len('/set '):]
        if username == '':
            update.message.reply_text(
                'set what? (repeat command with argument)')
            return

        self.db.db.storage.update_one({
            'user_id': update.message.from_user.id
        }, {
            '$set': {
                'collection.' + context['option']: {
                    'id': username,
                    'index': [],
                    'temp': []
                },
                'last_used.' + context['option']: []
            }
        }, upsert=True)
        try:
            self.db.db.cindex.update_one({}, {
                '$addToSet': {'index.' + username: update.message.from_user.id}
            }, upsert=True)
        except Exception as e:
            print(e)
        del self.context[update.message.from_user.id]

    def help(self, bot, update):
        msg = update.message.reply_text(
            '*Message Tagger/Indexer*\n' +
            'To start working with this, follow the following steps:\n' +
            '    0. Create two channels (with usernames), hereon designated _temp_ and _storage_\n' +
            '    1. Add the bot to both of them\n' +
            '    2. (in this chat) /connect <name-of-collection>\n' +
            '    3. (in this chat) /set _@storage_\n' +
            '    4. (in this chat) /temp _@temp_\n' +
            'To tag and add a message (or a list of messages) to a collection, send the message(s) to _storage_, followed by "set: _tags_"\n' +
            '\nHere\'s an example of such interaction:',
            parse_mode=ParseMode.MARKDOWN
        )

        msg.reply_text(
            'This is a test message'
        )
        msg.reply_text(
            'set: test msg message useless example'
        )

        update.message.reply_text(
            'To search any given collection, use the bot\'s inline interface as such:\n' +
            '    @istorayjebot _collection_ _query_\n' +
            'for example:\n' +
            '    @istorayjebot gif misaka nah',
            parse_mode=ParseMode.MARKDOWN
        )
        update.message.reply_text(
            'Further modifications to entries are done through the provided operators, by replying to a tagged message\n' +
            '    `^set:` - set the tags, overwrites the previous values\n' +
            '    `^add:` - adds a tag to the previous set\n' +
            '    `^remove:` - removes a tag from the previous set\n' +
            '    `^delete` - deletes the entry from the index\n' +
            '\n' +
            'for example:',
            parse_mode=ParseMode.MARKDOWN
        )
        msg.reply_text(
            '^add: newtag another-new-tag'
        )

    def reverse_search(self, bot, update, fuzzy=False):
        update.message.reply_text(
            'Send the document/image/GIF (text will not be processed)'
        )
        ctx = self.context.get('do_reverse', [])
        fz = self.context.get('fuzz_reverse', {})
        ctx.append(update.message.from_user.id)
        self.context['do_reverse'] = ctx
        fz[update.message.from_user.id] = fuzzy
        self.context['fuzz_reverse'] = fz

    def reverse_search_fuzzy(self, bot, update):
        self.reverse_search(bot, update, fuzzy=True)

    def rehash_all(self, bot, update):
        index = list((x['_id'], x['file_id'])
                     for x in self.db.db.message_cache.find({'file_id': {'$ne': None}}))
        update.message.reply_text(f'found {len(index)} items, updating...')
        mod = 0
        for item in index:
            try:
                h = xxhash.xxh64(self.updater.bot.get_file(
                    file_id=item[1]).download_as_bytearray()).digest()
                mod += self.db.db.message_cache.update_one(
                    {'_id': item[0]}, {'$set': {'xxhash': h}}).modified_count
            except:
                traceback.print_exc()
        update.message.reply_text(f'Rehash done, updated {mod} entries')
