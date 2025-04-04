from telegram.ext import (
    Updater, CommandHandler, InlineQueryHandler,
    MessageHandler,
    Filters, Job
)
from telegram import (
    InlineQueryResultArticle, ParseMode,
    InputTextMessageContent,
    InlineQueryResultCachedDocument, InlineQueryResultCachedPhoto, InlineQueryResultCachedGif,
    InlineQueryResultCachedMpeg4Gif, InlineQueryResultCachedSticker, Sticker,
    InlineQueryResultCachedVoice,
)
from telegram.ext import ChosenInlineResultHandler
import telegram

# from googlecloud import getCloudAPIDetails
from googleimgsearch import searchGoogleImages
from saucenao import searchSauceNao
from iqdb import searchIqdb
from trace import getTraceAPIDetails
from extern import pke_tagify, store_image, get_some_frame, process_gifops
from anilist import *
from deepdan import deepdan
from pytimeparse.timeparse import timeparse

from bson.objectid import ObjectId

from db import DB
import re
import os
from io import BytesIO
from uuid import uuid4, UUID
import traceback
import json
import xxhash
import dill as pickle
from threading import Event
from time import time
import random
from datetime import timedelta
from nltk.corpus import stopwords
from nltk.corpus import wordnet as wn
from nltk.stem import PorterStemmer
from bson.json_util import dumps
from apihandler import APIHandler

def get_any(obj, lst):
    for prop in lst:
        if hasattr(obj, prop):
            mm = getattr(obj, prop, None)
            if mm is not None:
                return mm
    return None


class IstorayjeBot:
    def __init__(self, tokens: list[str], db: DB, dev=False):
        self.stemmer = PorterStemmer()
        self.stopwordset = stopwords.words('english')
        self.random = random.Random()
        self.tokens = tokens
        self.updaters = [Updater(token, use_context=False) for token in tokens]
        # Clear APScheduler jobs, they're weird
        for updater in self.updaters:
            updater.job_queue.scheduler.remove_all_jobs()

        self.db = db
        self.external_api_handler = APIHandler(self)
        for handler in self.create_handlers():
            self.register_handler(handler)

        for updater in self.updaters:
            updater.dispatcher.add_error_handler(self.error)
            self.restore_jobs(updater)
            updater.job_queue.run_repeating( # it will re-add itself based on load if more is required
                self.process_insertions, (timedelta(minutes=1) if not dev else timedelta(seconds=10)))

        self.primary_updater = self.updaters[0]

        self.context = {}

    def _transform_to_self_repeating_job(self, fn, job_id, period, args, kwargs, bot):
        def f(_, _1, fn = fn, args = args, kwargs = kwargs, bot = bot, job_id = job_id):
            try:
                print("Invoking", fn, type(fn))
                getattr(self, fn)(*args, **kwargs, bot=bot)
            except Exception as e:
                print(f'[Async Sched Error running {job_id}] {e}')
                traceback.print_exc()
            finally:
                self.db.db.jobs.update_one({'_id': job_id}, {'$set': {'next_run': time() + period}})
        return f

    def enqueue_repeating_task(self, bot, fn, period, *args, **kwargs):
        doc = self.db.db.jobs.insert_one({
            'fn': fn,
            'args': pickle.dumps(args),
            'kwargs': pickle.dumps(kwargs),
            'period': period,
            'start_time': time(),
            'next_run': time() + int(period),
        })
        self.primary_updater.job_queue.run_once(
            self._transform_to_self_repeating_job(fn, doc.inserted_id, period, args, kwargs, bot), period, name=str(doc.inserted_id))
        return doc.inserted_id

    def restore_jobs(self, updater):
        jq = updater.job_queue
        now = time()
        jobs = self.db.db.jobs.find({})
        for job in jobs:
            fn = job['fn']
            args = pickle.loads(job['args'])
            kwargs = pickle.loads(job['kwargs'])
            job_id = job['_id']
            next_run_from_now = job['next_run'] - now
            print("Next run for", job_id, "is", next_run_from_now, "later")
            if next_run_from_now <= 0:
                next_from_now = next_run_from_now % job['period']
                print("", "Which was overdue, and now is", next_from_now, "later")
            next_from_now = int(next_from_now)
            # Transform into a run_once job that will be re-added and will update the db entry
            jq.run_once(self._transform_to_self_repeating_job(fn, job_id, job['period'], args, kwargs, updater.bot), next_run_from_now, name = str(job_id))

    def cancel_repeating_task(self, task_id):
        print('canceling job', task_id)
        res = self.db.db.jobs.delete_one({'_id': ObjectId(task_id)})
        print('', 'deleted', res.deleted_count)
        for job in self.primary_updater.job_queue.get_jobs_by_name(task_id):
            job.schedule_removal()
            print('', "removed job", job)

    def error(self, bot, update, error):
        print(f'[Error] Update {update} caused error {error}')

    def register_handler(self, handler):
        for updater in self.updaters:
            updater.dispatcher.add_handler(handler)

    def start_polling(self):
        for updater in self.updaters:
            updater.start_polling()
        self.primary_updater.idle()

    def start_webhook(self):
        PORT = int(os.environ.get("PORT", "8443"))
        APP_URL = os.environ.get("APP_URL")
        for i, updater in enumerate(self.updaters):
            updater.start_webhook(
                listen="0.0.0.0", port=PORT, url_path=self.tokens[i],
                webhook_url="{}/{}".format(APP_URL, self.tokens[i]))
        self.primary_updater.idle()

    def create_handlers(self):
        return [
            CommandHandler('start', self.handle_start),
            CommandHandler('list_index', self.handle_list_index),
            CommandHandler('alias', self.handle_alias),
            CommandHandler('api', self.handle_api),
            CommandHandler('connect', self.start_option_set),
            CommandHandler('temp', self.set_temp),
            CommandHandler('set', self.set_option),
            CommandHandler('reverse', self.reverse_search),
            CommandHandler('reverse_fuzzy', self.reverse_search_fuzzy),
            CommandHandler('rehash', self.rehash_all),
            CommandHandler('help', self.help),
            CommandHandler('magics', self.help_magics),
            CommandHandler('share', self.share),
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

    def process_insertions(self, bot, *args, timeout=150, **kwargs):
        stime = time()
        while True:
            if time() - stime >= timeout:
                break
            doc = self.db.db.tag_updates.find_one_and_delete({})
            if not doc:
                break
            instags = []
            extra = []

            if doc['service'] == 'google':
                print('google', doc)
                details = searchGoogleImages(bot.get_file(doc['fileid'])._get_encoded_url())
                if not details:
                    resp = doc['response_id']
                    bot.edit_message_text(
                        'Failed: google query had no results',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    continue
                req = details['descriptions'] + ([details['best_guess']] * 4 if details['best_guess'] != '' else [])
                res = pke_tagify(req)
                if not res:
                    resp = doc['response_id']
                    bot.edit_message_text(
                        'Failed: google query had no usable tags',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    continue
                instags = [x[0] for x in res if x[1] >= 100*doc['similarity_cap']]
                extra = details['links']

            elif doc['service'] == 'sauce':
                sub = doc['sub_service']
                print(sub, doc)
                details = (searchIqdb if sub == 'iqdb' else searchSauceNao)(bot.get_file(doc['fileid'])._get_encoded_url())
                print(details)
                if not details:
                    resp = doc['response_id']
                    bot.edit_message_text(
                        'Failed: sauce query had no results',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    continue
                req = [x['title'] for x in details if x['similarity'] >= 100 * doc['similarity_cap']]
                res = pke_tagify(req)
                if not res:
                    resp = doc['response_id']
                    bot.edit_message_text(
                        'Failed: sauce query had no usable tags',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    continue
                instags = [x[0] for x in res if x[1] >= 100*doc['similarity_cap']]
                extra = [x['title'] + ':\n' + '\n\t'.join(x['content'].split('\n')) + '\n\n' for x in details]

            elif doc['service'] == 'dan':
                details = deepdan(doc['filecontent'], doc['mime'])
                if not details:
                    resp = doc['response_id']
                    bot.edit_message_text(
                        'Rejected: deepdan query had no results',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    print('no response')
                    continue

                # what the fuck?
                doclist = sorted(filter(lambda x: x[1] >= doc['similarity_cap'], details), key=lambda x: x[1], reverse=True)
                print(doclist, details)
                if not doclist:
                    resp = doc['response_id']
                    bot.edit_message_text(
                        f'Completed: deepdan query results below set similarity ({doc["similarity_cap"]})',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    print('similarity cap hit, just use first')
                    continue

                instags = [x[0] for x in doclist]

            elif doc['service'] == 'anime':
                details = getTraceAPIDetails(doc['filecontent'])
                if not details:
                    resp = doc['response_id']
                    bot.edit_message_text(
                        'Rejected: anime query had no results',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    print('no response')
                    continue

                docv = details['docs']
                if len(docv) < 1:
                    resp = doc['response_id']
                    bot.edit_message_text(
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
                    bot.edit_message_text(
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
                    bot.edit_message_text(
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
                instags = instags + docv['synonyms']
                extra = [docv['title_romaji'], docv['title_english'], docv['anime']]

                if docv['is_adult']:
                    instags.push('nsfw')

            elif doc['service'] == 'gifop':
                res = process_gifops(
                    url=bot.get_file(doc['fileid'])._get_encoded_url(),
                    ops={x:doc[x] for x in ['reverse', 'speed', 'skip', 'early', 'append', 'animate'] if x in doc},
                    format=doc['format']
                )
                resp = doc['response_id']
                if res == b'':
                    bot.edit_message_text(
                        f'Failed: operation failed',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    continue
                msgid = None
                try:
                    xrep = BytesIO(res)
                    xrep.name = 'gifopd.mp4'
                    msgid = bot.send_animation(
                        doc['response_id'][1],
                        xrep
                    ).message_id
                except Exception as e:
                    bot.edit_message_text(
                        f'Failed: operation failed: {e}',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    continue

                insps = list((x[0], a['index']) for x in doc['insertion_paths'] for a in self.db.db.storage.aggregate([
                    {'$match': {'user_id': {'$in': doc['users']}}},
                    {'$project': {'index': f'$collection.{x[0]}.index'}},
                    {'$unwind': {'path': '$index', 'includeArrayIndex': 'idx'}},
                    {'$match': {'index.id': x[1]}},
                    {'$project': {'index': '$idx'}}
                ]))
                if doc.get('replace', False):
                    ins = {'$set': {f'collection.{p[0]}.index.{p[1]}.id': msgid for p in insps}}
                    self.db.db.storage.update_many({'user_id': {'$in': doc['users']}}, ins)
                    bot.edit_message_text(
                        f'Completed: applied conversion and modified index',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                else:
                    document = self.db.db.storage.find_one({
                        '$and': [
                            {'user_id': {'$in': doc['users']}},
                            *[{f'collection.{p[0]}.index.{p[1]}.id': {'$not': None}} for p in insps]
                        ]
                    })
                    if not insps:
                        bot.edit_message_text(
                            f'Completed.\nWarning: nowhere to insert, new gif is unregistered',
                            chat_id=resp[1],
                            message_id=resp[0],
                        )
                        continue
                    if not document:
                        bot.edit_message_text(
                            f'Failed: not a single user whom has indexed the original message found',
                            chat_id=resp[1],
                            message_id=resp[0],
                        )
                        continue
                    ins = {
                        '$set': {
                            f'collection.{p[0]}.index.{p[1]}': document[f'collection.{p[0]}.index.{p[1]}']
                            for p in insps
                        }
                    }
                    self.db.db.storage.update_many({'user_id': {'$in': doc['users']}}, ins)
                    bot.edit_message_text(
                        f'Completed: applied conversion and added to index',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                continue
            instags = list(set(instags))

            if len(instags):
                instags = [tag.replace(' ', '_') for tag in instags]
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
                bot.edit_message_text(
                        f'Completed:\nadded tags: {" ".join(instags)}\nExtra data:\t{"    ".join(extra if extra else ["None"])}',
                    chat_id=resp[1],
                    message_id=resp[0],
                )
            else:
                resp = doc['response_id']
                bot.edit_message_text(
                    'Completed: query had no results',
                    chat_id=resp[1],
                    message_id=resp[0],
                )

        if self.db.db.tag_updates.count_documents({}) != 0:
            self.primary_updater.job_queue.run_once(lambda *args, **kwargs: self.process_insertions(bot, *args, **kwargs), timedelta(seconds=5)) # todo: based on load
    def sample(self, iterator, k):
        result = [next(iterator) for _ in range(k)]

        n = k - 1
        for item in iterator:
            n += 1
            s = self.random.randint(0, n)
            if s < k:
                result[s] = item

        return result

    def handle_magic_tags(self, tag: str, message: object, insertion_paths: list, early: bool, users: list):
        def aparse(st):
            ss = st.split('/')
            ss = [float(s.strip()) if not s.strip().startswith('<') else s.strip() for ssv in ss for s in ssv.split(';')]
            if len(ss) == 1:
                return ss[0] if isinstance(ss[0], list) else ss
            return ss

        tag, targs = tag
        if tag.startswith('$'):
            tag = tag[1:]
            print('magic tag', tag, 'with args', targs)
            insert = {
                'service': '',
                'filecontent': None,
                'fileid': None,
                'dlpath': None,
                'insertion_paths': insertion_paths,
                'users': users,
                'mime': None,
            }
            if tag in ['google', 'anime', 'dan', 'sauce']:
                if early:
                    return None
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

                google = tag in ['google', 'sauce']
                insert['similarity_cap'] = int(
                    targs[0])/100 if len(targs) else 0.6

                if any(x in mime for x in ['gif', 'mp4']):
                    if google:
                        content = get_some_frame(bot.get_file(file_id=doc.file_id)._get_encoded_url(), format='mp4' if 'mp4' in mime else 'gif')
                        # print(content)
                        # p = png.Reader(bytes=content).read()
                        # m = hashlib.md5()
                        # m.update(content)
                        tmp = self.db.db.storage.find_one({'$and': [{'user_id': {'$in': users}}, {'temp.id': {'$ne': None}}]})
                        if not tmp:
                            message.reply_text(f'none of the receiving users ({", ".join(users)}) has a temp set, impossible to process')
                            return None
                        insert['fileid'] = store_image(
                                            bot=self,
                                            file=BytesIO(content),
                                            # width=p[0],
                                            # height=p[1],
                                            # type='image/png',
                                            chat=tmp['temp']['id']
                                        )
                    else:
                        insert['filecontent'] = bytes(bot.get_file(
                            file_id=doc.thumb.file_id).download_as_bytearray())

                elif 'image' in mime:
                    if google:
                        insert['fileid'] = doc.file_id
                    else:
                        insert['filecontent'] = bytes(bot.get_file(
                            file_id=doc.file_id).download_as_bytearray())

                else:
                    print(mime, 'is not supported')
                    return None  # shrug

                insert['service'] = tag
                if tag == 'sauce':
                    insert['sub_service'] = 'saucenao' if len(targs) < 2 else targs[1]

            elif tag in ['caption', 'default_caption', 'defcap', 'defcaption', 'cap']:
                if early:
                    return None
                if len(targs) > 1:
                    return 'magic::$caption::requires(one or zero arguments)'
                def handle(collections, message, bot, arg):
                    return {
                        '$set': {
                            f'collection.{coll}.index.$.caption': arg if arg else None # clear
                            for coll in collections
                        }
                    }
                return InstantMagicTag('caption', argument=targs[0], handler=handle, can_handle=['add'])
            elif tag in ['syn', 'synonyms']:
                if len(targs) < 1:
                    return 'magic::$synonyms::requires(more than zero arguments)'
                words = []
                options = {
                    'hypernym-depth': 0,
                    'hyponyms': 0,
                    'count': 10,
                }
                for arg in targs:
                    if arg.startswith(':'):
                        arg = arg[1:]
                        opt, *arg = re.split(r'\s+', arg, maxsplit=1)
                        print(opt, arg)
                        if opt in options:
                            try:
                                options[opt] = int(arg[0] if len(arg) else 1)
                            except:
                                pass
                    else:
                        words.append(arg)
                synsets = set(sum((wn.synsets(word) for word in words), []))
                for depth in range(options['hypernym-depth']):
                    synsets.update([hyp for syn in synsets for hyp in syn.hypernyms()])

                if options['hyponyms']:
                    synsets.update([hyp for syn in synsets for hyp in syn.hyponyms()])
                try:
                    res0 = (lemma.name() for synset in synsets for lemma in synset.lemmas())
                    return self.sample(res0, options['count'])
                except Exception as e:
                    return f'magic::$synonyms::error(not enough synonyms present, set a lower :count, {e})'
            elif tag == 'gifop':
                if early:
                    return None
                insert['service'] = 'gifop'
                operations = {
                    'reverse': None,
                    'speed': None,
                    'skip': None,
                    'early': None,
                    'append': None,
                    'replace': None,
                    'animate': [],
                }
                for opt in targs:
                    if opt == 'reverse':
                        operations['reverse'] = True
                    elif opt == 'append':
                        operations['append'] = True
                    elif opt == 'replace':
                        operations['replace'] = True
                    else:
                        if opt.startswith('speed '):
                            operations['speed'] = float(opt[6:].strip())
                        elif opt.startswith('skip ') or opt.startswith('early '):
                            op = ['skip', 'early'][opt[0] == 'e']
                            value, unit = None, None
                            value, unit, *_ = opt[len(op):].strip().split(' ')
                            print(repr(opt[len(op):]), repr(value), repr(unit))
                            operations[op] = {'value': int(value), 'unit': unit}
                        elif opt.startswith('animate '):
                            print('animate ::', opt)
                            op = 'animate'
                            frame, funit = 0, 'fr'
                            length, lunit = 1, 'fr'
                            effect = None
                            mux = False
                            dx, dy = 0, 0
                            # animate frame:[number]/[unit] length:[number]/[unit] effect:[name] dx:[number] dy:[number] (multiplex) (word:word)
                            asv = [x.replace('\x04', ' ') for x in opt[8:].replace('^ ', '\x04').split(' ')]
                            extra = {}
                            for optv in asv:
                                if optv == 'multiplex':
                                    mux = True
                                if ':' not in optv:
                                    continue
                                k,v = optv.split(':', 1)
                                v = v.strip()
                                if k == 'frame':
                                    v, funit = v.split('/', 1)
                                    frame = float(v)
                                elif k == 'length':
                                    v, lunit = v.split('/', 1)
                                    length = float(v)
                                elif k == 'effect':
                                    effect = v
                                elif k == 'dx':
                                    dx = float(v)
                                elif k == 'dy':
                                    dy = float(v)
                                else:
                                    extra[k] = v
                            print('animate', effect, dx, dy, length, frame, extra)
                            if not effect:
                                continue

                            if effect == 'distort':
                                extra['arguments'] = aparse(extra.get('arguments', ''))

                            if effect == 'text':
                                for prop in ['text', 'font-size']:
                                    extra[prop] = extra.get(prop, None)

                            operations[op].append({
                                'frame': {
                                    'start': {
                                        'value': frame,
                                        'unit': funit
                                    },
                                    'length': {
                                        'value': length,
                                        'unit': lunit
                                    }
                                },
                                'effect': {
                                    'name': effect,
                                    'multiplex': mux,
                                    'dx': dx,
                                    'dy': dy,
                                    **extra
                                }
                            })
                print(operations)
                if not any(operations[x] for x in operations):
                    print ('nothing to do with this')
                    return None
                doc = get_any(message, ['document'])
                if not doc:
                    print ('no document for this')
                    return None
                mime = doc.mime_type
                if not mime or not any(x in mime for x in ['gif', 'mp4']):
                    print('not a gif')
                    return None
                # it's a GIF
                operations['format'] = 'gif' if 'gif' in mime else 'mp4'
                operations['message_id'] = message.message_id
                operations['chat_id'] = message.chat_id
                operations['fileid'] = doc.file_id
                insert.update({k:v for k,v in operations.items() if v})
            else:
                return 'unsupported_magic::$' + tag

            if not insert['service']:
                return None

            rmm = message.reply_text(f'will process {tag} and edit this message with the result in a bit.')
            insert['response_id'] = [rmm.message_id, rmm.chat_id]
            print(insert)
            self.db.db.tag_updates.insert_one(insert)
            return None
        return tag

    def parse_insertion_statement(self, stmt: str, rev={'(': ')', ')': '(', '{': '}', '}': '{', '<': '>', '>': '<'}):
        tags = []
        stmt = stmt.strip() + ' '
        open_paren = {
            '(': 0,
            '{': 0,
            '<': 0
        }
        tagbuf = ''
        argbuf = ''
        args = []
        escaped = False
        while len(stmt):
            c, stmt = stmt[0], stmt[1:]
            if c == ' ' and all(o == 0 for v,o in open_paren.items()):
                tags.append((tagbuf, args))
                tagbuf = ''
                argbuf = ''
                args = []
            elif not escaped and c in ['(', '{']:
                if open_paren[c] == 0:
                    argbuf = ''
                open_paren[c] += 1
            elif not escaped and c in [')', '}']:
                if open_paren[rev[c]] > 1:
                    open_paren[rev[c]] -= 1
                    argbuf += ')'
                elif open_paren[rev[c]] == 1:
                    open_paren[rev[c]] = 0
                    args.append(argbuf)
                else:
                    tagbuf += ')'
            elif c == '\\':
                if escaped:
                    escaped = False
                    if any(o != 0 for v,o in open_paren.items()):
                        argbuf += c
                    else:
                        tagbuf += c
                else:
                    escaped = True
            elif not escaped and c == ',' and any(o != 0 for v,o in open_paren.items()):
                args.append(argbuf)
                argbuf = ''
                while len(stmt) and stmt[0] == ' ':
                    stmt = stmt[1:]

            else:
                escaped = False
                if any(o != 0 for v,o in open_paren.items()):
                    argbuf += c
                else:
                    tagbuf += c

        return tags

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
                        mvalue = xxhash.xxh64(bot.get_file(
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
                try:
                    username = str(update.effective_user.id)
                except:
                    print('uhhhh...🤷‍♀️')
                    return
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
        extern = False
        extern_schedule = None
        extern_unschedule = None
        extern_query = None

        try:
            text = msg.text
            if text.startswith('set:'):
                move = True
                tags = self.parse_insertion_statement(text[4:].strip())
            elif text.startswith('^delete'):
                delete = True
            elif text.startswith('^set:'):
                reset = True
                tags = self.parse_insertion_statement(text[5:].strip())
            elif text.startswith('^add:'):
                add = True
                tags = self.parse_insertion_statement(text[5:].strip())
            elif text.startswith('^remove:'):
                remove = True
                tags = self.parse_insertion_statement(text[8:].strip())
            elif text.startswith('^tags?'):
                query = True
                users = [msg.from_user.id]
            elif text.startswith('.ext '):
                extern = True
                rest = text[5:]
                users = []
                if rest.startswith('.schedule '):
                    rest = rest[10:]
                    extern_schedule, extern_query = rest.split(' ', maxsplit=1)
                elif rest.startswith('.unschedule '):
                    extern_unschedule = rest[12:]
                else:
                    extern_query = rest

        except Exception:
            pass

        if extern:
            if extern_unschedule:
                self.cancel_repeating_task(extern_unschedule)
            elif extern_schedule:
                tp = timeparse(extern_schedule)
                if not tp:
                    msg.reply_text(f"Invalid time spec '{extern_schedule}'")
                else:
                    try:
                        job_id = self.enqueue_repeating_task(bot, 'process_extern_request', tp, extern_query, msg.to_dict())
                        msg.reply_text(f"Will repeat {extern_query} every {tp} seconds!\nuse {job_id} to refer to this job")
                    except Exception as e:
                        traceback.print_exc()
                        msg.reply_text(f"Error while processing request: {e}")
            else:
                self.process_extern_request(extern_query, msg.to_dict(), bot)

        for user in users:
            filterop = {}
            updateop = {}
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
                    has_instant = False
                    try:
                        m_msgid = msg.reply_to_message.message_id
                        m_msg = msg.reply_to_message
                    except:
                        noreply = True
                    ftags = set()
                    for x in mtags: # no arguments for non-magic tags
                        rtag = self.handle_magic_tags(
                            early=noreply,
                            tag=x,
                            message=m_msg,
                            insertion_paths=[(coll, m_msgid)
                                             for coll in collections],
                            users=[user]
                        )
                        if not rtag:
                            continue

                        if isinstance(rtag, InstantMagicTag):
                            has_instant = True
                            updateop.update(rtag.generate(collections=collections, message=msg, bot=self, set=reset, add=add, remove=remove))
                            continue

                        elif isinstance(rtag, list):
                            ftags.update(rtag)
                            continue

                        ftags.add(rtag)
                    mtags = list(ftags)
                print(mtags)
                if not mtags and any([move, add, reset, remove]) and not has_instant:
                    return
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
                        traceback.print_exc()
                        print(e)
                elif move:
                    updateop.update({
                        '$push': {
                            f'collection.{coll}.index': {'id': msgid, 'tags': mtags}
                            for coll in collections
                            for msgid in self.db.db.storage.find_one({'user_id': user})['collection'][coll]['temp']
                        }
                    })
                    updateop.update({
                        '$set': {
                            f'collection.{coll}.temp': []
                            for coll in collections
                        }
                    })
                elif add:
                    filterop.update({ '$or': [
                        {f'collection.{coll}.index.id': msg.reply_to_message.message_id}
                        for coll in collections
                    ]})
                    updateop.update({
                        '$push': {
                            f'collection.{coll}.index.$.tags': {'$each': mtags}
                            for coll in collections
                        }
                    })
                elif remove:
                    filterop.update({ '$or': [
                        {f'collection.{coll}.index.id': msg.reply_to_message.message_id}
                        for coll in collections
                    ]})
                    updateop.update({
                        '$pullAll': {
                            f'collection.{coll}.index.$.tags': mtags
                            for coll in collections
                        }
                    })
                elif reset:
                    filterop.update({ '$or': [
                        {f'collection.{coll}.index.id': msg.reply_to_message.message_id}
                        for coll in collections
                    ]})
                    updateop.update({
                        '$set': {
                            f'collection.{coll}.index.$.tags': mtags
                            for coll in collections
                        }
                    })
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
                traceback.print_exc()
                print(e)

    def process_extern_request(self, req: str, msg: str, bot):
        msg = telegram.Message.de_json(msg, bot)
        # @each (req) (lines:it)
        # query
        def parse(x: str):
            only_first = True
            if x.startswith('.first '):
                x = x[7:]
                only_first = True
            if x.startswith('@each '):
                req, *xs = x[6:].splitlines()
                if len(xs) == 0:
                    raise Exception("@each (req) expects a list of things afterwards")
                return ({"first": only_first}, [req.replace('(it)', x) for x in xs])
            return ({"first": only_first}, x)

        def process(*args, parsed_req=parse(req), **kwargs):
            first = parsed_req[0]['first']
            def do_respond(*_):
                def res(inline_query_results, **_):
                    # Extract raw results from the inline query results and reply with them
                    for result in inline_query_results:
                        if not hasattr(result, 'title'):
                            continue

                        content = result.input_message_content
                        bot.send_message(
                            chat_id=msg.chat.id,
                            text=content.message_text,
                            parse_mode=content.parse_mode,
                            reply_to_message_id=msg.message_id,
                        )
                        if first:
                            break
                return res

            for req in parsed_req[1]:
                self.handle_query(bot, (req, msg), respond=do_respond, read=lambda x: x[0], user=lambda x: x[1].from_user)

        return process()

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

    def parse_query(self, gquery):
        gquery = gquery.strip() + ' '
        coll = ''
        parsed_coll = False
        open_brak = 0
        escaped = False
        query = []
        qstack = []
        qbuf = ''
        extra = {
            'caption': None
        }
        if gquery.startswith("+"):
            # +n (pagination)
            gquery = gquery[1:]
            num = re.search(r'^(\d+)\s*(.*)', gquery)
            if num:
                extra['page'] = int(num.group(1))
                gquery = num.group(2)
            else:
                gquery = '+' + gquery
        while len(gquery):
            c, gquery = gquery[0], gquery[1:]
            if c == ' ' and not parsed_coll and open_brak == 0:
                if len(coll) > 0:
                    parsed_coll = True
                continue
            elif c == ' ' and open_brak == 1:
                qbuf += ' '
            elif c == ' ':
                if qbuf:
                    query.append(qbuf)
                    qbuf = ''
            elif c == '{' and not escaped and open_brak == 0:
                open_brak += 1
            elif c == '\\' and not escaped:
                escaped = True
            elif c == '}' and not escaped and open_brak == 1:
                open_brak = 0
                extra['caption'] = qbuf
                qbuf = ''
            elif c == '|' and not escaped and open_brak == 0:
                if qbuf != '':
                    query.append(qbuf)
                    qbuf = ''
                qstack.append(query)
                query = []
            else:
                escaped = False
                if parsed_coll:
                    qbuf += c
                else:
                    coll += c
        if query != []:
            qstack.append(query)

        return coll, qstack, extra

    def resolve_alias(self, alias, user_id, alias_map={}):
        aliases = self.db.db.aliases.find_one({'user_id': user_id})
        if not aliases:
            return alias
        return aliases.get('aliases', alias_map).get(alias, alias)

    def add_alias(self, alias, value, user_id):
        self.db.db.aliases.find_one_and_update({'user_id': user_id}, {'$set': {f'aliases.{alias}': value}}, upsert=True)

    def add_implicit_alias(self, bot_username, value, user_id):
        self.db.db.aliases.find_one_and_update({'user_id': user_id}, {'$set': {f'aliases.implicit${bot_username}': value}}, upsert=True)

    def clone_messaage_with_data(self, data, tags):
        ty = data['type']
        print('> got some', ty, ':', data)
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
                data['file_id'],
                caption=data.get('caption', None)
            )
        elif ty == 'gif':
            return InlineQueryResultCachedGif(
                data['msg_id'],
                data['file_id'],
                caption=data.get('caption', None)
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
                data['file_id'],
                caption=data.get('caption', None)
            )
        elif ty == 'voice':
            return InlineQueryResultCachedVoice(
                data['msg_id'],
                data['file_id'],
                '> ' + ', '.join(tags) + ' (' + str(data['msg_id']) + ')',
                caption=data.get('caption', None)
            )
        elif ty == 'audio':
            return InlineQueryResultCachedAudio(
                data['msg_id'],
                data['file_id'],
                '> ' + ', '.join(tags) + ' (' + str(data['msg_id']) + ')',
                caption=data.get('caption', None)
            )
        else:
            print('unhandled msg type', ty, 'for message', data)
            return None

    def try_clone_message(self, message, tags, dcaption, fcaption, id=None, chid=None):
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
            self.db.db.message_cache.find_one_and_replace({'$and': [{'msg_id': id}, {'chatid': chid}]}, {k:v for k,v in data.items() if k != 'caption'}, upsert=True)
            return self.clone_messaage_with_data(data, tags)
        except:
            try:
                document = get_any(message, ['document', 'animation', 'audio', 'video', 'photo'])
                assert (document is not None and [] != document)
                mime = None
                if isinstance(document, list):
                    # photo list, we're gonna take a random one for fun
                    print (document)
                    assert (len(document) > 0)
                    document = self.random.choice(document)
                    mime = 'image'
                print('> is some sort of document')
                caption = fcaption if fcaption not in ['$def', '$default', '$'] else dcaption
                if not mime:
                    mime = document.mime_type
                data = {
                    'file_id': document.file_id,
                    'chatid': chid,
                    'msg_id': id,
                    'xxhash': xxhash.xxh64(bot.get_file(file_id=document.file_id).download_as_bytearray()).digest(),
                    'caption': caption
                }
                if 'mp4' in mime:
                    data['type'] = 'mp4'
                elif 'gif' in mime:
                    data['type'] = 'gif'
                elif 'image' in mime:
                    data['type'] = 'img'
                elif 'audio' in mime:
                    data['type'] = 'audio'
                else:
                    data['type'] = 'doc'
                self.db.db.message_cache.find_one_and_replace({'$and': [{'msg_id': id}, {'chatid': chid}]}, {k:v for k,v in data.items() if k != 'caption'}, upsert=True)
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
                    self.db.db.message_cache.find_one_and_replace({'$and': [{'msg_id': data['msg_id']}, {'chatid': data['chatid']}]}, {k:v for k,v in data.items() if k != 'caption'}, upsert=True)
                    return self.clone_messaage_with_data(data, tags)
                except:
                    try:
                        voice = get_any(message, ['voice', 'audio'])
                        assert (voice is not None)
                        print('> is voice')
                        data = {
                            'type': 'voice',
                            'file_id': voice.file_id,
                            'chatid': chid,
                            'msg_id': id,
                            'xxhash': xxhash.xxh64(bot.get_file(file_id=voice.file_id).download_as_bytearray()).digest(),
                        }
                        self.db.db.message_cache.find_one_and_replace({'$and': [{'msg_id': id}, {'chatid': chid}]}, {k:v for k,v in data.items() if k != 'caption'}, upsert=True)
                        return self.clone_messaage_with_data(data, tags)
                    except Exception as e:
                        traceback.print_exc()
                        print('exception occured while processing', message, 'with tags', tags)
                        return None
        return None

    def process_search_query_further(self, query: list):
        qq = query[:]
        qs = set(tuple(x) for x in query)
        sw = self.stopwordset
        for terms in query:
            tts = [[]]
            for term in terms:
                pt = self.stemmer.stem(term)
                if pt != term:
                    for tt in tts[:]: # ouch
                        if term not in sw and pt not in sw:
                            tts.remove(tt)
                        tts.append(tt + [pt])
                        tts.append(tt + [term])
                else:
                    for tt in tts:
                        tt.append(pt)
            for tt in tts:
                tp = tuple(tt)
                if tp not in qs:
                    qq.append(tt)
                    qs.add(tp)
        return qq

    def invoke(self, api, reqs, query):
        return self.external_api_handler.invoke(api, query)

    def render_api(self, api, reqs, res):
        return self.external_api_handler.render(api, res)

    def has_api(self, user, api):
        return api in self.external_api_handler.apis


    def external_source_handler(self, data: dict, bot, update, user_data=None, chat_data=None, respond=lambda x: x.inline_query.answer, read=lambda x: x.inline_query.query, user=lambda x: x.inline_query.from_user):
        try:
            coll, *ireqs = data['source'].split(':')
            ireqs = ':'.join(ireqs)
            query = data['query']
            if coll == 'anilist':
                if ireqs == 'ql':
                    # raw query
                    respond(update)([
                        InlineQueryResultArticle(
                            id=uuid4(),
                            title="Raw request results for " + query,
                            input_message_content=InputTextMessageContent(json.dumps(aniquery(query, {})))
                        )
                    ])
                elif ireqs == 'aggql':
                    # aggregate query response
                    # parse query: `query` `aggregate`
                    match = re.search(r'^\`([^\`]+)\`\s*\`([^\`]+)\`$', query)
                    if match is None:
                        return
                    q0,q1 = match.group(1, 2)

                    coll = self.db.client[f'temp_{user(update).id}']
                    db = coll['temp']
                    db.insert_one(aniquery(q0, {}))
                    res = db.aggregate(json.loads(q1))
                    respond(update)([
                        InlineQueryResultArticle(
                            id=uuid4(),
                            title="Aggregate request results for " + q0,
                            input_message_content=InputTextMessageContent(dumps(list(res), indent=4))
                        )
                    ])
                    db.drop()
                elif ireqs == 'id':
                    respond(update)(iquery_render(query))
                elif ireqs == 'one':
                    respond(update)(qquery_render(query))
                elif ireqs == 'bychar':
                    # find result by character
                    respond(update)(cquery_render(query))
                elif ireqs == 'char':
                    respond(update)(charquery_render(query))
                elif ireqs == '':
                    # simple query
                    respond(update)(squery_render(data['query']))
                else:
                    raise Exception(f'Arguments to source {coll} not understood ({ireqs})')
            elif self.has_api(user(update).id, coll):
                try:
                    res = self.invoke(coll, ireqs, query)
                    respond(update)(self.render_api(coll, ireqs, res))
                except Exception as e:
                    raise Exception(f'Invalid API invocation: {e}')
            else:
                raise Exception(f'Undefined source {coll}')
        except Exception as e:
            print(e)
            traceback.print_exc()
            respond(update)([
                InlineQueryResultArticle(
                    id=uuid4(),
                    title=f'Exception <{e}> occured while processing {query} in external source {coll}',
                    input_message_content=InputTextMessageContent(
                        f'allow me to let you in on a secret,\nThis bot is actually dumb\n{e}')
                )
            ], cache_time=10)

    def handle_query(self, bot, update, user_data=None, chat_data=None, respond=lambda x: x.inline_query.answer, read=lambda x: x.inline_query.query, user=lambda x: x.inline_query.from_user):
        try:
            implicit_collection = self.resolve_alias(f'implicit${bot.username}', user(update).id)
            query = read(update)
            if not implicit_collection.startswith("implicit$"):
                query = implicit_collection + ' ' + query
            original_query = query
            coll, query, extra = self.parse_query(query)
            coll = self.resolve_alias(coll, user(update).id)
            possible_update = self.db.db.late_share.find_one_and_delete({'username': user(update).username})
            while possible_update:
                # someone has shared stuff with this guy
                shares = possible_update['shares']
                for share in shares:
                    for mshare in share:
                        self.db.db.storage.update_one(
                            {'user_id': user(update).id},
                            {
                                '$set': {'collection.' + mshare: share[mshare][0], # This is stored as an array
                                'last_used.' + mshare: []}
                            },
                            upsert=True)
                possible_update = self.db.db.late_share.find_one_and_delete({'username': user(update).username})

            if coll.startswith('@'):
                # external sources
                return self.external_source_handler({'source': coll[1:], 'query': ' '.join(original_query.strip().split(' ')[1:])}, bot, update, user_data, chat_data, respond, read, user)
            fcaption = extra.get('caption', None)
            print(read(update), '->', repr(coll), repr(query), extra)
            if not coll or coll == '':
                return

            print(coll, query)
            if any(x in coll for x in '$./[]'):
                respond(update)(
                    [InlineQueryResultArticle(id=uuid4(), title='Invalid collection name "' + coll + '"',
                                              input_message_content=InputTextMessageContent('This user is an actual idiot'))]
                )
                return
            query = self.process_search_query_further(query)

            colls = list((x['index']['id'], x['index']['tags'], x['index'].get('caption', None), x['index'].get('cache_stale', False)) for x in
                         self.db.db.storage.aggregate([
                             {'$match': {
                                 'user_id': user(update).id
                             }
                             },
                             {'$project': {
                                 "index": '$collection.' + coll + '.index',
                                 '_id': 0
                             }
                             },
                             {'$unwind': '$index'},
                             {'$match': {
                                 '$or': [
                                    {'index.tags': {
                                        '$all': q
                                    }}
                                    for q in query
                                 ]
                             }
                             },
                             {'$limit': 5}
                         ])) if query else []
            results = [InlineQueryResultArticle(
                id=uuid4(),
                title='>> ' +
                '|'.join((" ".join(q) for q in (query or [['Your Recent Selections']]))),

                input_message_content=InputTextMessageContent(
                    'Search for `' +
                    '|'.join(' '.join(q) for q in query) + '\' and more~' if len(query) else 'Yes, these are your recents'
                )
            )]
            userdata = self.db.db.storage.find_one(
                {'user_id': user(update).id})
            chatid = userdata['collection'][coll]['id']
            cachetime = 300
            if len(query) == 0:
                last_used = self.db.db.storage.find_one({
                    'user_id': user(update).id
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
                    cmsg['caption'] = fcaption
                    results.append(self.clone_messaage_with_data(
                        cmsg, ['last', 'used']))
            if len(results) > 1:
                respond(update)(
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
                            f'<imaginary result matching {"|".join(" ".join(q) for q in query)}>')
                    )
                )
                cachetime = 60
            tempid = userdata.get('temp', None)
            print(userdata)

            if not tempid:
                results.append(
                    InlineQueryResultArticle(
                        id=uuid4(),
                        title='no temp set, some results might not be available',
                        input_message_content=InputTextMessageContent(
                            'no temp is set, "/temp <temp_chat_username>" in the bot chat')
                    )
                )
            else:
                tempid = tempid['id']

            for col in colls:
                try:
                    print(tempid, chatid, col)
                    cmsg = self.db.db.message_cache.find_one({'$and':
                                                              [{'chatid': chatid}, {
                                                                  'msg_id': col[0]}]
                                                              })
                    if cmsg and not col[3]:
                        print('cache hit for message', col[0], ':', cmsg)
                        cmsg['caption'] = fcaption if fcaption not in ['$def', '$default', '$'] else col[2]
                        cloned_message = self.clone_messaage_with_data(
                            cmsg, col[1])
                    elif tempid:
                        print('cache miss for message', col[0], ('(cache was stale) ::' if col[3] else '::'), 'trying to load it')
                        msg = bot.forward_message(
                            chat_id=tempid,
                            from_chat_id=chatid,
                            message_id=col[0],
                            disable_notification=True,
                        )
                        cloned_message = self.try_clone_message(
                            msg, col[1], id=col[0], chid=chatid, fcaption=fcaption, dcaption=col[2])
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
            page = extra.get('page', 0)
            start_index = min(page * 20, len(results))
            end_index = min(start_index + 20, len(results))
            if start_index == end_index:
                start_index = 0
                end_index = 20
            respond(update)(
                results[start_index:end_index],
                cache_time=cachetime,
                is_personal=True
            )
        except Exception as e:
            print(e)
            traceback.print_exc()
            respond(update)([
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
    def share(self, bot, update):
        # share <collection> with <@username|id>
        command = update.message.text[len('/share '):]
        try:
            coll, _, user_id, *_ = command.split(' ')
            try:
                coll = self.resolve_alias(coll, update.message.from_user.id)
                colls = list(x['collection'] for x in
                         self.db.db.storage.aggregate([
                             {'$match': {
                                 'user_id': update.message.from_user.id
                             }
                             },
                             {'$project': {
                                 "collection": '$collection.' + coll,
                                 '_id': 0
                             }
                             },
                         ]))
                if not colls:
                    raise Exception('ENOTFOUND')
                # we have the collection to share
                # now to insert it under the target
                mcoll = (update.message.from_user.username or 'id' + update.message.from_user.id) + '-' + coll
                if user_id.startswith('@'):
                    # username, add it to a different db, and resolve it later
                    self.db.db.late_share.update_one(
                            {'username': user_id[1:]},
                            {'$addToSet': {'shares': {mcoll: colls}}},
                            upsert=True)
                    update.message.reply_text(f'Shared collection {coll} with username {user_id}, they will receive it as soon as they try to access it (under name {mcoll})')
                else:
                    # user id
                    self.db.db.storage.update_one(
                            {'user_id': int(user_id)},
                            {
                                '$set': {'collection.' + mcoll: colls[0], 'last_used.' + mcoll: []}
                            },
                            upsert=True)
                    update.message.reply_text(f'Shared collection {coll} with user {user_id} as {mcoll}')
            except Exception as e:
                print('cannot share', coll, 'because', e)
                update.message.reply_text(
                    f'Unknown collection {coll}, you can\'t share that which you do not have'
                )
        except:
            update.message.reply_text(
                'Invalid command format, use /share <collection> with <@username|id>'
            )

    def handle_api(self, bot, update):
        ssubcommand = update.message.text[len('/api '):]
        cmd, *args = ssubcommand.split(' ')
        try:
            if cmd == 'redeclare':
                # redeclare <name> <comm_type> <input> <output> <api_path>
                if len(args) != 5:
                    update.message.reply_text(f'invalid number of arguments, expected 5 (declare <name> <comm_type> <input> <output> <api_path>), got {len(args)}')
                    return
                name, comm_type, inp, out, path = args
                if comm_type not in self.external_api_handler.comms:
                    update.message.reply_text(f'invalid comm_type {comm_type}, valid types are: {self.external_api_handler.comms}')
                    return

                del self.external_api_handler.apis[name]
                self.external_api_handler.declare(name, comm_type, inp, out, path)
                update.message.reply_text(f'registered api {name} as {path}, with input {inp} and output {out} for you.\nnow define the IOs')
            if cmd == 'declare':
                # declare <name> <comm_type> <input> <output> <api_path>
                if len(args) != 5:
                    update.message.reply_text(f'invalid number of arguments, expected 5 (declare <name> <comm_type> <input> <output> <api_path>), got {len(args)}')
                    return
                name, comm_type, inp, out, path = args
                if comm_type not in self.external_api_handler.comms:
                    update.message.reply_text(f'invalid comm_type {comm_type}, valid types are: {self.external_api_handler.comms}')
                    return

                self.external_api_handler.declare(name, comm_type, inp, out, path)
                update.message.reply_text(f'registered api {name} as {path}, with input {inp} and output {out} for you.\nnow define the IOs')
            elif cmd == 'redefine':
                # redefine [input/output] <name> <type> <vname> ...request_body
                if len(args) < 4:
                    update.message.reply_text(f'invalid number of arguments, expected at least 4 arguments (define [input/output] <name> <type> <vname> ...request_body), but got {len(args)}')
                    return
                iotype, name, _type, vname, *req = args
                if iotype not in ['input', 'output']:
                    update.message.reply_text(f'invalid io type {iotype}, expected either `input` or `output`')
                    return
                del self.external_api_handler.ios[iotype][name]
                self.external_api_handler.define(iotype, name, vname, _type, ' '.join(req))
                update.message.reply_text(f'registered {iotype} adapter({_type}) {name} as `{" ".join(req)}`({vname})')
                return
            elif cmd == 'define':
                # define [input/output] <name> <type> <vname> ...request_body
                if len(args) < 4:
                    update.message.reply_text(f'invalid number of arguments, expected at least 4 arguments (define [input/output] <name> <type> <vname> ...request_body), but got {len(args)}')
                    return
                iotype, name, _type, vname, *req = args
                if iotype not in ['input', 'output']:
                    update.message.reply_text(f'invalid io type {iotype}, expected either `input` or `output`')
                    return
                self.external_api_handler.define(iotype, name, _type, vname, ' '.join(req))
                update.message.reply_text(f'registered {iotype} adapter({_type}) {name} as `{" ".join(req)}`({vname})')
                return
            elif cmd == 'list':
                if len(args) < 1:
                    update.message.reply_text(f'invalid number of arguments, expected 1 argument (list [api/io/input/output])')
                    return
                arg = args[0]
                if arg == "api":
                    update.message.reply_text('\n'.join(
                        f'{x}: ({v[0]}) input<{v[1]}> output<{v[2]}> = {v[3]}' for x,v in self.external_api_handler.apis.items()
                    ))
                    return
                elif arg == "input":
                    [update.message.reply_text(f'{x}\n{y}\n') for x,y in self.external_api_handler.input_adapters.items()]
                    return
                elif arg == "output":
                    [update.message.reply_text(f'{x}\n{y}\n') for x,y in self.external_api_handler.output_adapters.items()]
                    return
                else:
                    update.message.reply_text(f'unknown subcommand {arg}')
            else:
                update.message.reply_text('unknown command')
                return
        except Exception as e:
            update.message.reply_text(str(e))


    def handle_alias(self, bot, update):
        ssubcommand = update.message.text[len('/alias '):]
        cmd, *args = ssubcommand.split(' ')
        if cmd == 'set':
            if len(args) != 2:
                update.message.reply_text(f'invalid number of arguments, expected 2, got {len(args)}')
                return
            self.add_alias(args[0], args[1], update.message.from_user.id)
            update.message.reply_text(f'Added collection alias "{args[0]}" for "{args[1]}"')
            return
        elif cmd == 'get':
            update.message.reply_text(f'collection "{args[0]}" resolves to "{self.resolve_alias(args[0], update.message.from_user.id)}"')
            return
        elif cmd == 'implicit':
            if len(args) != 1:
                update.message.reply_text(f'invalid number of arguments, expected 1, got {len(args)}')
                return
            self.add_implicit_alias(bot.username, args[0], update.message.from_user.id)
            update.message.reply_text(f'Added implicit collection alias "{args[0]}" for bot "{bot.username}" (this one)')
            return
        else:
            update.message.reply_text(f'Unknown command')
            return

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
            traceback.print_exc()
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
            '    @istorayjebot _collection_ _query_ _{caption}_\n' +
            'You may want to alias a collection (external collections supported) to a different name, to do that, send\n' +
            '    /alias set <alias> <value>\n' +
            '  for example: /alias set ac @anilist:char\n' +
            'The caption can be omitted, or set as any of the following to get the "default" caption:\n' +
            '    `$def` or `$default` or `$`' +
            'for example:\n' +
            '    @istorayjebot gif misaka nah {$}\n' +
            'To match "misaka" and "nah" from the collection "gif" and give it the default (if set) caption\n' +
            'or another example:\n' +
            '    @istorayjebot gif lol\n' +
            'To match "lol" in collection "gif" and send no caption\n' +
            'or yet another example:\n' +
            '    @istorayjebot gif fish {Help, I am drowning!}\n' +
            'to match "fish" in collection "gif" and give it the caption "Help, I am drowning!"\n\n' +
            'External pseudo-collections (collections starting with \'@\')\n' +
            '    `@anilist`: query anilist for anime stuff\n' +
            '        takes an optional modifier in the form `@anilist:modifier` where modifier is:\n' +
            '        `ql`     - custom graphql search\n' +
            '        `bychar` - search anime by character name\n' +
            '        `char`   - search for character\n' +
            '        `aggql`  - aggregate operations on result of gql\n',
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
        update.message.reply_text(
            'in the later stage (when you would modify tags with ^<cmd>), you can use some "magic" tags that do different things\n' +
            'Here is a short description of some:\n' +
            '    $google(_minimum accepted accuracy_)\n' +
            '        reverse searches google and finds some matching tags\n' +
            '    $anime(_minimum accepted accuracy_)\n' +
            '        tries to find the name of the anime (cropped images/gifs will not work)\n' +
            '    $caption(_default caption_)\n' +
            '        sets a "default" caption for the message (not texts)\n' +
            'The default _minimum accepted accuracy_ is 60%, and *commas have to be escaped in captions*\n',
            parse_mode=ParseMode.MARKDOWN
        )

    def help_magics(self, bot, update):
        index = [
            'Magic Tags Index\n'
            '\n'
            'TODO: Add online documentation for this stuff\n'
            'This is a listing of all supported magic tags and their properties:\n',

            '`$google` - search google for relevant tags\n'
            '  stage 2\n'
            '  arguments:\n'
            '      - positional _minimum accepted accuracy_ <int>: tags with confidence less than this will be ignored\n'
            '      - optional literal _cloud_ <literal>: supposed to search with google Vision ML. currently ignored.\n'
            '  short forms:\n'
            '      None\n'
            '  document types:\n'
            '      media documents <image, video, GIF>\n'
            '  further notes:\n'
            '      None\n',

            '`$anime` - search for anime title\n'
            '  stage 2\n'
            '  arguments:\n'
            '       - position _minimum accepted accuracy_ <int>: results with confidence less than this will be ignored\n'
            '  short forms:\n'
            '      None\n'
            '  document types:\n'
            '      media documents <image, video, GIF>\n'
            '  further notes:\n'
            '      Cropped images of anime will likely yield incorrect results\n',


            '`$sauce` - search for image source (SauceNao)\n'
            '  stage 2\n'
            '  arguments:\n'
            '       - position _minimum accepted accuracy_ <int>: results with confidence less than this will be ignored\n'
            '  short forms:\n'
            '      None\n'
            '  document types:\n'
            '      media documents <image, video, GIF>\n'
            '  further notes:\n'
            '      May return multiple equal sources, and sources may have no links\n',


            '`$dan` - use a neural net to guess image contents (uses danbooru tags)\n'
            '  stage 2\n'
            '  arguments:\n'
            '       - position _minimum accepted accuracy_ <int>: results with confidence less than this will be ignored\n'
            '  short forms:\n'
            '      None\n'
            '  document types:\n'
            '      media documents <image, video, GIF>\n'
            '  further notes:\n'
            '      Geared towards animated/drawn images, but has shown to perform reasonably well on real images too\n',

            '`$synonyms` - find and add synonyms or related words\n'
            '  stage 1\n'
            '  arguments:\n'
            '      - mixed literal _word_ <literal> (multiple allowed): words to process\n'
            '      - optional opt mixed literal _:hypernym-depth {depth}_ <literal> (takes an int modifier "depth"): include words related by categories up to _depth_ categories\n'
            '      - optional opt mixed literal _:count {count}_ <literal> (takes an int modifier "count"): include this many results (default 10)\n'
            '      - optional opt literal _:hyponyms_ <literal>: includes words related in the same category\n'
            '  short forms:\n'
            '      `$syn`\n'
            '  document types:\n'
            '      all document types\n'
            '  further notes:\n'
            '      None\n',

            '`$caption` - add a default caption invokable by {$} in inline queries\n'
            '  stage 2\n'
            '  arguments:\n'
            '      - positional literal _caption_ <literal>: the would-be default caption (escape commas with a backslash "\\")\n'
            '  short forms:\n'
            '      `$cap`, `$defcap`\n'
            '  document types:\n'
            '      media documents <image, video, GIF>\n'
            '  further notes:\n'
            '      None\n',

            '`$gifop` - operations on indexed GIFs\n'
            '  stage 2\n'
            '  arguments:\n'
            '      - optional literal _reverse_ <literal>: reverses the GIF\n'
            '      - optional literal _append_ <literal>: (applies if _reverse_ is provided) appends the reverse to the end of original if provided\n'
            '      - optional literal _replace_ <literal>: replaces the original in the index if provided\n'
            '      - optional mixed literal _speed {speed}_ (takes a float modifier _speed_) <literal>: modifies the speed of the GIF\n'
            '      - optional mixed literal _skip {value} <ti|fr|%>_ (takes an int modifier _value_) <literal>: skips the provided {value} units from the start\n'
            '      - optional mixed literal _early {value} <ti|fr|%>_ (takes an int modifier _value_) <literal>: cuts off the provided {value} units from the end\n'
            '      - optional mixed literal _animate frame:[number]/[unit] length:[number]/[unit] effect:[name] dx:[number] dy:[number] (multiplex) (word:word)_\n'
            '           note: to escape spaces in e.g. \'text\', use `^ ` (that is, caret-space)\n'
            '           effects:\n'
            '           + scroll - scroll in direction (dx, dy)\n'
            '           + zoom - zoom in to (dx, dy)\n'
            '           + rotate - rotate around (dx, dy)\n'
            '           + text - place (prop: `text`) at position (dx, dy)\n'
            '             extras: `[color:<fill color>] [outline:<stroke color>] [background:<background color>] [shadow:<shadow color>]`\n'
            '             not specifying `shadow` and `background` will disable them\n'
            '           + overlay-points - overlay key point positions\n'
            '           + distort - apply various distortions (TODO)\n'
            '  short forms:\n'
            '      None\n'
            '  document types:\n'
            '      GIFs\n'
            '  further notes:\n'
            '      operation order is (first to last):\n'
            '          skip|early, reverse, speed\n',
            '''/api documentation

the available subcommands are

- list {api/input/output}
    List the required API handlers

- define {input/output} {name} {varname} ...request in API DSL
    Defines the named adapter with the provided varname,
    for input adapters, the variable refers to the query (as a string)
    for output adapters, the variable refers to the response, parsed if possible

- redefine (same args as define)
    Overwrites an existing adapter

- declare {name} {comm type} {input adapter} {output adapter} {api path}
    Declares an API endpoint, accessible via the @{name} external collection
    comm type will be defined later
    api path may refer to the result of {input adapter} as result

- redeclare (same args as declare)
    Overwrites a declared api

API handler documentation

Comm types

the available comm types are:
- http/link
    metavar in url: Yes
    response type: string
    behaviour: simply substitutes the metavar in the URL
    expected input adapter output: string

- json/post
    metavar in url: Yes, through input.pvalue
    response type: Dictionary
    behvaiour: substitutes the metavar, sends a post request, returns output
    expected input adapter output: {pvalue: string, value: T}

- html/xpath
    metavar in url: Yes
    response type: Dictionary
    behvaiour: substitutes the metavar, sends a get request, returns output
    expected input adapter output: string

- graphql
    metavar in url: No
    response type: T
    behaviour: sends the metavar as a gql query
    expected input adapter output: T'


DSL Documentation

A single python expression with the following extensions

- expr @json - jsonifies expr
- ${metavar} - replaced with the value of the metavar


Output Adapter Documentation

All output adapters must evaluate to an array of 2-tuples of the form (result name, result text), both of which should be strings.'''
        ]
        for i in index:
            update.message.reply_text(
                i,
                parse_mode=ParseMode.MARKDOWN
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
                     for x in self.db.db.message_cache.find({'$and': [{'file_id': {'$ne': None}}, {'cache_stale': None}]}))
        update.message.reply_text(f'found {len(index)} items, updating...')
        mod = 0
        for item in index:
            try:
                h = xxhash.xxh64(bot.get_file(
                    file_id=item[1]).download_as_bytearray()).digest()
                mod += self.db.db.message_cache.update_one(
                    {'_id': item[0]}, {'$set': {'xxhash': h}}).modified_count
            except:
                traceback.print_exc()
        update.message.reply_text(f'Rehash done, updated {mod} entries')


class InstantMagicTag:
    def __init__(self, name, argument=None, arguments=None, handler=None, can_handle=None):
        self.name = name
        self.handler = handler
        self.can_handle = can_handle or []
        if argument:
            self.arguments = [argument]
            self.kind = 'single'
        elif arguments:
            self.arguments = arguments
            self.kind = 'multiple'
        else:
            self.arguments = [None]
            self.kind = 'single'

    def generate(self, collections, message, bot, **kwargs):
        if not self.handler or not all(x in self.can_handle for x in kwargs if kwargs[x]):
            return {}
        return self.handler(collections, message, bot, self.arg)

    @property
    def arg(self):
        if self.kind == 'single':
            return self.arguments[0]
        else:
            return self.arguments

