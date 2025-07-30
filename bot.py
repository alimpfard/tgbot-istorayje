import asyncio
from telegram.ext import (
    Updater,
    CommandHandler,
    InlineQueryHandler,
    MessageHandler,
    filters,
    ApplicationBuilder,
    Application,
    ContextTypes,
)
from telegram import (
    InlineQueryResult,
    InlineQueryResultArticle,
    InputTextMessageContent,
    InlineQueryResultCachedDocument,
    InlineQueryResultCachedPhoto,
    InlineQueryResultCachedGif,
    InlineQueryResultCachedMpeg4Gif,
    InlineQueryResultCachedSticker,
    Sticker,
    InlineQueryResultCachedVoice,
    InlineQueryResultCachedAudio,
)
from telegram.ext import ChosenInlineResultHandler
import telegram
from telegram import Bot
from telegram.constants import ParseMode
import signal

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

from typing import Awaitable, Callable, Optional, Coroutine
from type import T

from help_texts import HELP_TEXTS, MAGIC_HELP_TEXTS


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
        self.stopwordset = stopwords.words("english")
        self.random = random.Random()
        self.tokens = tokens
        self.apps = [ApplicationBuilder().token(token).build() for token in tokens]

        self.db = db
        self.external_api_handler = APIHandler(self)
        for handler in self.create_handlers():
            self.register_handler(handler)

        for app in self.apps:
            app.add_error_handler(self.error)
            self.restore_jobs(app)
            app.job_queue.run_repeating(  # it will re-add itself based on load if more is required
                self.process_insertions,
                (timedelta(minutes=1) if not dev else timedelta(seconds=10)),
            )

        self.primary_app = self.apps[0]

        self.context = {}

    def _transform_to_self_repeating_job(
        self, fn: str, job_id, period: float, args, kwargs
    ) -> Callable[[ContextTypes.DEFAULT_TYPE], Coroutine]:
        async def f(
            context: ContextTypes.DEFAULT_TYPE,
            fn=fn,
            args=args,
            kwargs=kwargs,
            job_id=job_id,
        ):
            try:
                print("Invoking", fn, type(fn))
                await getattr(self, fn)(*args, **kwargs, context=context)
            except Exception as e:
                print(f"[Async Sched Error running {job_id}] {e}")
                traceback.print_exc()
            finally:
                self.db.db.jobs.update_one(
                    {"_id": job_id}, {"$set": {"next_run": time() + period}}
                )

        return f

    def enqueue_repeating_task(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        fn: str,
        period: float,
        *args,
        **kwargs,
    ):
        doc = self.db.db.jobs.insert_one(
            {
                "fn": fn,
                "args": pickle.dumps(args),
                "kwargs": pickle.dumps(kwargs),
                "period": period,
                "start_time": time(),
                "next_run": time() + int(period),
            }
        )
        self.primary_app.job_queue.run_once(
            self._transform_to_self_repeating_job(
                fn, doc.inserted_id, period, args, kwargs
            ),
            period,
            name=str(doc.inserted_id),
        )

        return doc.inserted_id

    def restore_jobs(self, app: Application):
        jq = app.job_queue
        now = time()
        jobs = self.db.db.jobs.find({})
        for job in jobs:
            fn = job["fn"]
            args = pickle.loads(job["args"])
            kwargs = pickle.loads(job["kwargs"])
            job_id = job["_id"]
            next_run_from_now = job["next_run"] - now
            print("Next run for", job_id, "is", next_run_from_now, "later")
            # Calculate delay until the next run, adjust if overdue
            delay = next_run_from_now
            if delay <= 0:
                delay = delay % job["period"]
                print("", "Which was overdue, and now is", delay, "later")
            delay = int(delay)
            # Transform into a run_once job that will be re-added and will update the db entry
            jq.run_once(
                self._transform_to_self_repeating_job(
                    fn, job_id, job["period"], args, kwargs
                ),
                delay,
                name=str(job_id),
            )

    def cancel_repeating_task(self, task_id):
        print("canceling job", task_id)
        res = self.db.db.jobs.delete_one({"_id": ObjectId(task_id)})
        print("", "deleted", res.deleted_count)
        for job in self.primary_app.job_queue.get_jobs_by_name(task_id):
            job.schedule_removal()
            print("", "removed job", job)

    async def error(self, update: object, context: ContextTypes.DEFAULT_TYPE):
        print(f"[Error] Update {update} caused error {context.error}")

    def register_handler(self, handler):
        for app in self.apps:
            app.add_handler(handler)

    def start_polling(self):
        loop = asyncio.get_event_loop()
        stop_signals = (
            signal.SIGINT,
            signal.SIGTERM,
            signal.SIGABRT,
        )
        for sig in stop_signals:
            loop.add_signal_handler(sig, self.primary_app._raise_system_exit)

        try:
            for app in self.apps:
                try:

                    def error_callback(exc) -> None:
                        app.create_task(app.process_error(error=exc, update=None))

                    loop.run_until_complete(app._bootstrap_initialize(max_retries=0))
                    if app.post_init:
                        loop.run_until_complete(app.post_init(app))
                    if app._Application__stop_running_marker.is_set():
                        print("App is already stopped, skipping polling start")
                        continue
                    updater_coro = app.updater.start_polling(
                        error_callback=error_callback
                    )
                    loop.run_until_complete(updater_coro)
                    loop.run_until_complete(app.start())
                except (KeyboardInterrupt, SystemExit):
                    print("Received shutdown signal, stopping polling")
            loop.run_forever()
        finally:
            # Ensure all apps are stopped gracefully
            for app in self.apps:
                try:
                    if app.updater.running:
                        loop.run_until_complete(app.updater.stop())
                    if app.running:
                        loop.run_until_complete(app.stop())
                        if app.post_stop:
                            loop.run_until_complete(app.post_stop(app))
                    loop.run_until_complete(app.shutdown())
                    if app.post_shutdown:
                        loop.run_until_complete(app.post_shutdown(app))
                except:
                    continue
            loop.close()

    def start_webhook(self):
        PORT = int(os.environ.get("PORT", "8443"))
        APP_URL = os.environ.get("APP_URL")

        # If there's only one updater, use the simple approach
        if len(self.apps) == 1:
            self.apps[0].run_webhook(
                listen="0.0.0.0",
                port=PORT,
                url_path=self.tokens[0],
                webhook_url="{}/{}".format(APP_URL, self.tokens[0]),
            )
            return

        # For multiple updaters, we need a router
        from flask import Flask, request, jsonify
        import threading
        import logging

        app = Flask(__name__)

        @app.route("/<path:token>", methods=["POST"])
        def webhook(token):
            for i, t in enumerate(self.tokens):
                if token == t:
                    updater_port = PORT + i + 1
                    from requests import post

                    response = post(
                        f"http://127.0.0.1:{updater_port}/",
                        data=request.data,
                        headers=dict(request.headers),
                    )
                    return (
                        response.content,
                        response.status_code,
                        dict(response.headers),
                    )

            return jsonify({"error": "Invalid token"}), 404

        threading.Thread(
            target=lambda: app.run(
                host="0.0.0.0", port=PORT, debug=False, use_reloader=False
            ),
            daemon=True,
        ).start()

        print(f"Router started on port {PORT}")

        loop = asyncio.get_event_loop()
        stop_signals = (signal.SIGINT, signal.SIGTERM, signal.SIGABRT)
        for s in stop_signals:
            loop.add_signal_handler(s, self.primary_app._raise_system_exit)

        try:
            for i, tapp in enumerate(self.apps):
                try:

                    def error_callback(exc) -> None:
                        tapp.create_task(tapp.process_error(error=exc, update=None))

                    loop.run_until_complete(tapp._bootstrap_initialize(max_retries=0))
                    if tapp.post_init:
                        loop.run_until_complete(tapp.post_init(tapp))
                    if tapp._Application__stop_running_marker.is_set():
                        print("App is already stopped, skipping webhook start")
                        continue
                    updater_coro = tapp.updater.start_webhook(
                        listen="127.0.0.1",
                        port=PORT + i + 1,
                        url_path="",
                        webhook_url=f"{APP_URL}/{self.tokens[i]}",
                    )
                    loop.run_until_complete(updater_coro)
                    loop.run_until_complete(tapp.start())
                except (KeyboardInterrupt, SystemExit):
                    print("Received shutdown signal, stopping webhook")
            loop.run_forever()
        finally:
            # Ensure all apps are stopped gracefully
            for tapp in self.apps:
                try:
                    if tapp.updater.running:
                        loop.run_until_complete(tapp.updater.stop())
                    if tapp.running:
                        loop.run_until_complete(tapp.stop())
                        if tapp.post_stop:
                            loop.run_until_complete(tapp.post_stop(tapp))
                    loop.run_until_complete(tapp.shutdown())
                    if tapp.post_shutdown:
                        loop.run_until_complete(tapp.post_shutdown(tapp))
                except:
                    continue
            loop.close()

    def create_handlers(self):
        return [
            CommandHandler("start", self.handle_start),
            CommandHandler("list_index", self.handle_list_index),
            CommandHandler("alias", self.handle_alias),
            CommandHandler("api", self.handle_api),
            CommandHandler("connect", self.start_option_set),
            CommandHandler("temp", self.set_temp),
            CommandHandler("set", self.set_option),
            CommandHandler("reverse", self.reverse_search),
            CommandHandler("reverse_fuzzy", self.reverse_search_fuzzy),
            CommandHandler("rehash", self.rehash_all),
            CommandHandler("help", self.help),
            CommandHandler("magics", self.help_magics),
            CommandHandler("share", self.share),
            ChosenInlineResultHandler(self.on_result_chosen),
            InlineQueryHandler(self.handle_query),
            MessageHandler(filters.ALL, self.handle_possible_index_update),
        ]

    async def handle_start(
        self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE
    ):
        await context.bot.send_message(
            chat_id=update.message.chat.id,
            text=f"""Hello there General Kenobi - {update.message.from_user.id}@{update.message.chat.id}\nWe are live at {context.bot}""",
        )

    async def on_result_chosen(
        self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE
    ):
        result = update.chosen_inline_result
        result_id = result.result_id
        try:
            UUID(result_id, version=4)
            return  # we don't want to cache these fuckers
        except:
            result_id = int(result_id)
        user = result.from_user.id
        coll, *_ = self.parse_query(result.query)
        print(f"> chosen result {result_id} for user {user} - collection {coll}")
        doc = self.db.db.storage.find_one_and_update(
            {"user_id": user}, {"$inc": {"used_count": 1}}, return_document=True
        )
        last_used = doc["last_used"].get(coll, [])
        if result_id in last_used:
            return
        print(f"> {user}'s last_used: {last_used}")
        print(f">> last used count", len(last_used))
        if len(last_used) > 5:
            count = self.db.db.storage.update_one(
                {"user_id": user}, {"$pop": {f"last_used.{coll}": -1}}
            ).modified_count
            print(">> evicted", count, "entries")
        doc = self.db.db.storage.find_one_and_update(
            {"user_id": user},
            {"$addToSet": {f"last_used.{coll}": int(result_id)}},
            return_document=True,
        )
        print(f"> {user}'s last_used: {doc['last_used'][coll]}")

    def is_useful_tag(self, tag):
        print(tag)
        return re.sub(r"season|\s+", "", re.sub(r"[^A-z]", "", tag)) != ""

    def tagify_all(self, *tags):
        return list(
            filter(
                self.is_useful_tag,
                sum(
                    list(
                        list(
                            set(
                                [x.replace(" ", "_").lower()]
                                + x.lower().split(" ")
                                + list(
                                    re.sub("\\W", "", y.lower()) for y in x.split(" ")
                                )
                            )
                        )
                        for x in tags
                        if x
                    ),
                    [],
                ),
            )
        )

    async def process_insertions(
        self, context: ContextTypes.DEFAULT_TYPE, *args, timeout=150, **kwargs
    ):
        stime = time()
        while True:
            if time() - stime >= timeout:
                break
            doc = self.db.db.tag_updates.find_one_and_delete({})
            if not doc:
                break
            instags = []
            extra = []

            if doc["service"] == "google":
                print("google", doc)
                details = searchGoogleImages(
                    (await context.bot.get_file(doc["fileid"]))._get_encoded_url()
                )
                if not details:
                    resp = doc["response_id"]
                    await context.bot.edit_message_text(
                        "Failed: google query had no results",
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    continue
                req = details["descriptions"] + (
                    [details["best_guess"]] * 4 if details["best_guess"] != "" else []
                )
                res = pke_tagify(req)
                if not res:
                    resp = doc["response_id"]
                    await context.bot.edit_message_text(
                        "Failed: google query had no usable tags",
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    continue
                instags = [x[0] for x in res if x[1] >= 100 * doc["similarity_cap"]]
                extra = details["links"]

            elif doc["service"] == "sauce":
                sub = doc["sub_service"]
                print(sub, doc)
                details = (searchIqdb if sub == "iqdb" else searchSauceNao)(
                    (await context.bot.get_file(doc["fileid"]))._get_encoded_url()
                )
                print(details)
                if not details:
                    resp = doc["response_id"]
                    await context.bot.edit_message_text(
                        "Failed: sauce query had no results",
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    continue
                req = [
                    x["title"]
                    for x in details
                    if x["similarity"] >= 100 * doc["similarity_cap"]
                ]
                res = pke_tagify(req)
                if not res:
                    resp = doc["response_id"]
                    await context.bot.edit_message_text(
                        "Failed: sauce query had no usable tags",
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    continue
                instags = [x[0] for x in res if x[1] >= 100 * doc["similarity_cap"]]
                extra = [
                    x["title"] + ":\n" + "\n\t".join(x["content"].split("\n")) + "\n\n"
                    for x in details
                ]

            elif doc["service"] == "dan":
                details = deepdan(doc["filecontent"], doc["mime"])
                if not details:
                    resp = doc["response_id"]
                    await context.bot.edit_message_text(
                        "Rejected: deepdan query had no results",
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    print("no response")
                    continue

                # what the fuck?
                doclist = sorted(
                    filter(lambda x: x[1] >= doc["similarity_cap"], details),
                    key=lambda x: x[1],
                    reverse=True,
                )
                print(doclist, details)
                if not doclist:
                    resp = doc["response_id"]
                    await context.bot.edit_message_text(
                        f'Completed: deepdan query results below set similarity ({doc["similarity_cap"]})',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    print("similarity cap hit, just use first")
                    continue

                instags = [x[0] for x in doclist]

            elif doc["service"] == "anime":
                details = getTraceAPIDetails(doc["filecontent"])
                if not details:
                    resp = doc["response_id"]
                    await context.bot.edit_message_text(
                        "Rejected: anime query had no results",
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    print("no response")
                    continue

                docv = details["docs"]
                if len(docv) < 1:
                    resp = doc["response_id"]
                    await context.bot.edit_message_text(
                        "Failed: anime query had no results",
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    print("no results")
                    continue
                doclist = []
                try:
                    # what the fuck?
                    doclist = sorted(
                        filter(
                            lambda x: x["similarity"] >= doc["similarity_cap"], docv
                        ),
                        key=lambda x: x["similarity"],
                        reverse=True,
                    )
                    docv = next(iter(doclist))
                except StopIteration:
                    resp = doc["response_id"]
                    await context.bot.edit_message_text(
                        f'Completed: anime query results below set similarity ({doc["similarity_cap"]})',
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    print("similarity cap hit, just use first")
                    continue
                except:
                    traceback.print_exc()
                    continue

                if not docv:
                    resp = doc["response_id"]
                    await context.bot.edit_message_text(
                        "Failed: unknown reason",
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    print("invalid response, null document")
                    continue

                res = pke_tagify(
                    [
                        docv["filename"],
                        docv["anime"],
                        docv["title_english"],
                        docv["title_romaji"],
                        *docv["synonyms"],
                    ]
                )

                instags = [x[0] for x in res if x[1] >= doc["similarity_cap"]]
                instags = instags + docv["synonyms"]
                extra = [docv["title_romaji"], docv["title_english"], docv["anime"]]

                if docv["is_adult"]:
                    instags.push("nsfw")

            elif doc["service"] == "gifop":
                res = process_gifops(
                    url=(await context.bot.get_file(doc["fileid"]))._get_encoded_url(),
                    ops={
                        x: doc[x]
                        for x in [
                            "reverse",
                            "speed",
                            "skip",
                            "early",
                            "append",
                            "animate",
                        ]
                        if x in doc
                    },
                    format=doc["format"],
                )
                resp = doc["response_id"]
                if res == b"":
                    await context.bot.edit_message_text(
                        f"Failed: operation failed",
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    continue
                msgid = None
                try:
                    xrep = BytesIO(res)
                    xrep.name = "gifopd.mp4"
                    msgid = (
                        await context.bot.send_animation(doc["response_id"][1], xrep)
                    ).message_id
                except Exception as e:
                    await context.bot.edit_message_text(
                        f"Failed: operation failed: {e}",
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                    continue

                insps = list(
                    (x[0], a["index"])
                    for x in doc["insertion_paths"]
                    for a in self.db.db.storage.aggregate(
                        [
                            {"$match": {"user_id": {"$in": doc["users"]}}},
                            {"$project": {"index": f"$collection.{x[0]}.index"}},
                            {"$unwind": {"path": "$index", "includeArrayIndex": "idx"}},
                            {"$match": {"index.id": x[1]}},
                            {"$project": {"index": "$idx"}},
                        ]
                    )
                )
                if doc.get("replace", False):
                    ins = {
                        "$set": {
                            f"collection.{p[0]}.index.{p[1]}.id": msgid for p in insps
                        }
                    }
                    self.db.db.storage.update_many(
                        {"user_id": {"$in": doc["users"]}}, ins
                    )
                    await context.bot.edit_message_text(
                        f"Completed: applied conversion and modified index",
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                else:
                    document = self.db.db.storage.find_one(
                        {
                            "$and": [
                                {"user_id": {"$in": doc["users"]}},
                                *[
                                    {
                                        f"collection.{p[0]}.index.{p[1]}.id": {
                                            "$not": None
                                        }
                                    }
                                    for p in insps
                                ],
                            ]
                        }
                    )
                    if not insps:
                        await context.bot.edit_message_text(
                            f"Completed.\nWarning: nowhere to insert, new gif is unregistered",
                            chat_id=resp[1],
                            message_id=resp[0],
                        )
                        continue
                    if not document:
                        await context.bot.edit_message_text(
                            f"Failed: not a single user whom has indexed the original message found",
                            chat_id=resp[1],
                            message_id=resp[0],
                        )
                        continue
                    ins = {
                        "$set": {
                            f"collection.{p[0]}.index.{p[1]}": document[
                                f"collection.{p[0]}.index.{p[1]}"
                            ]
                            for p in insps
                        }
                    }
                    self.db.db.storage.update_many(
                        {"user_id": {"$in": doc["users"]}}, ins
                    )
                    await context.bot.edit_message_text(
                        f"Completed: applied conversion and added to index",
                        chat_id=resp[1],
                        message_id=resp[0],
                    )
                continue
            instags = list(set(instags))

            if len(instags):
                instags = [tag.replace(" ", "_") for tag in instags]
                insps = list(
                    (x[0], a["index"])
                    for x in doc["insertion_paths"]
                    for a in self.db.db.storage.aggregate(
                        [
                            {"$match": {"user_id": {"$in": doc["users"]}}},
                            {"$project": {"index": f"$collection.{x[0]}.index"}},
                            {"$unwind": {"path": "$index", "includeArrayIndex": "idx"}},
                            {"$match": {"index.id": x[1]}},
                            {"$project": {"index": "$idx"}},
                        ]
                    )
                )
                print(insps)
                if len(insps):
                    ins = {
                        "$addToSet": {
                            f"collection.{p[0]}.index.{p[1]}.tags": {"$each": instags}
                            for p in insps
                        }
                    }
                    print(ins)
                    self.db.db.storage.update_many(
                        {"user_id": {"$in": doc["users"]}}, ins
                    )
                resp = doc["response_id"]
                await context.bot.edit_message_text(
                    f'Completed:\nadded tags: {" ".join(instags)}\nExtra data:\t{"    ".join(extra if extra else ["None"])}',
                    chat_id=resp[1],
                    message_id=resp[0],
                )
            else:
                resp = doc["response_id"]
                await context.bot.edit_message_text(
                    "Completed: query had no results",
                    chat_id=resp[1],
                    message_id=resp[0],
                )

        if self.db.db.tag_updates.count_documents({}) != 0:
            self.primary_app.job_queue.run_once(
                lambda *args, **kwargs: self.process_insertions(
                    context, *args, **kwargs
                ),
                timedelta(seconds=5),
            )  # todo: based on load

    def sample(self, iterator, k):
        result = [next(iterator) for _ in range(k)]

        n = k - 1
        for item in iterator:
            n += 1
            s = self.random.randint(0, n)
            if s < k:
                result[s] = item

        return result

    async def handle_magic_tags(
        self,
        tag: str,
        message: telegram.Message | None,
        insertion_paths: list,
        early: bool,
        users: list,
        context: ContextTypes.DEFAULT_TYPE,
    ):
        def aparse(st):
            ss = st.split("/")
            ss = [
                float(s.strip()) if not s.strip().startswith("<") else s.strip()
                for ssv in ss
                for s in ssv.split(";")
            ]
            if len(ss) == 1:
                return ss[0] if isinstance(ss[0], list) else ss
            return ss

        tag, targs = tag
        if tag.startswith("$"):
            tag = tag[1:]
            print("magic tag", tag, "with args", targs)
            insert = {
                "service": "",
                "filecontent": None,
                "fileid": None,
                "dlpath": None,
                "insertion_paths": insertion_paths,
                "users": users,
                "mime": None,
            }
            if tag in ["google", "anime", "dan", "sauce"]:
                if early:
                    return None
                doc = get_any(message, ["document", "sticker", "photo"])
                mime = None
                if isinstance(doc, list):
                    doc = self.random.choice(doc)
                    mime = "image"
                if isinstance(doc, Sticker):
                    mime = "image"

                if not doc:
                    print("doc is null", "from", message)
                    return None
                if not mime:
                    mime = doc.mime_type  # type: ignore (not hit for sticker)
                print("got doc", doc)

                google = tag in ["google", "sauce"]
                insert["similarity_cap"] = int(targs[0]) / 100 if len(targs) else 0.6

                if any(x in mime for x in ["gif", "mp4"]):
                    if google:
                        content = get_some_frame(
                            (
                                await context.bot.get_file(file_id=doc.file_id)
                            )._get_encoded_url(),
                            format="mp4" if "mp4" in mime else "gif",
                        )
                        # print(content)
                        # p = png.Reader(bytes=content).read()
                        # m = hashlib.md5()
                        # m.update(content)
                        tmp = self.db.db.storage.find_one(
                            {
                                "$and": [
                                    {"user_id": {"$in": users}},
                                    {"temp.id": {"$ne": None}},
                                ]
                            }
                        )
                        if not tmp:
                            await message.reply_text(
                                f'none of the receiving users ({", ".join(users)}) has a temp set, impossible to process'
                            )
                            return None
                        insert["fileid"] = store_image(
                            bot=self,
                            file=BytesIO(content),
                            # width=p[0],
                            # height=p[1],
                            # type='image/png',
                            chat=tmp["temp"]["id"],
                        )
                    else:
                        insert["filecontent"] = bytes(
                            await (
                                await context.bot.get_file(
                                    file_id=doc.thumb.file_id  # type: ignore (not hit for sticker, photo, etc.)
                                )
                            ).download_as_bytearray()
                        )

                elif "image" in mime:
                    if google:
                        insert["fileid"] = doc.file_id
                    else:
                        insert["filecontent"] = bytes(
                            await (
                                await context.bot.get_file(file_id=doc.file_id)
                            ).download_as_bytearray()
                        )

                else:
                    print(mime, "is not supported")
                    return None  # shrug

                insert["service"] = tag
                if tag == "sauce":
                    insert["sub_service"] = "saucenao" if len(targs) < 2 else targs[1]

            elif tag in ["caption", "default_caption", "defcap", "defcaption", "cap"]:
                if early:
                    return None
                if len(targs) > 1:
                    return "magic::$caption::requires(one or zero arguments)"

                def handle(collections, message, bot, arg):
                    return {
                        "$set": {
                            f"collection.{coll}.index.$.caption": (
                                arg if arg else None
                            )  # clear
                            for coll in collections
                        }
                    }

                return InstantMagicTag(
                    "caption", argument=targs[0], handler=handle, can_handle=["add"]
                )
            elif tag in ["syn", "synonyms"]:
                if len(targs) < 1:
                    return "magic::$synonyms::requires(more than zero arguments)"
                words = []
                options = {
                    "hypernym-depth": 0,
                    "hyponyms": 0,
                    "count": 10,
                }
                for arg in targs:
                    if arg.startswith(":"):
                        arg = arg[1:]
                        opt, *arg = re.split(r"\s+", arg, maxsplit=1)
                        print(opt, arg)
                        if opt in options:
                            try:
                                options[opt] = int(arg[0] if len(arg) else 1)
                            except:
                                pass
                    else:
                        words.append(arg)
                synsets = set(sum((wn.synsets(word) for word in words), []))
                for depth in range(options["hypernym-depth"]):
                    synsets.update([hyp for syn in synsets for hyp in syn.hypernyms()])

                if options["hyponyms"]:
                    synsets.update([hyp for syn in synsets for hyp in syn.hyponyms()])
                try:
                    res0 = (
                        lemma.name() for synset in synsets for lemma in synset.lemmas()
                    )
                    return self.sample(res0, options["count"])
                except Exception as e:
                    return f"magic::$synonyms::error(not enough synonyms present, set a lower :count, {e})"
            elif tag == "gifop":
                if early:
                    return None
                insert["service"] = "gifop"
                operations = {
                    "reverse": None,
                    "speed": None,
                    "skip": None,
                    "early": None,
                    "append": None,
                    "replace": None,
                    "animate": [],
                }
                for opt in targs:
                    if opt == "reverse":
                        operations["reverse"] = True
                    elif opt == "append":
                        operations["append"] = True
                    elif opt == "replace":
                        operations["replace"] = True
                    else:
                        if opt.startswith("speed "):
                            operations["speed"] = float(opt[6:].strip())
                        elif opt.startswith("skip ") or opt.startswith("early "):
                            op = ["skip", "early"][opt[0] == "e"]
                            value, unit = None, None
                            value, unit, *_ = opt[len(op) :].strip().split(" ")
                            print(repr(opt[len(op) :]), repr(value), repr(unit))
                            operations[op] = {"value": int(value), "unit": unit}
                        elif opt.startswith("animate "):
                            print("animate ::", opt)
                            op = "animate"
                            frame, funit = 0, "fr"
                            length, lunit = 1, "fr"
                            effect = None
                            mux = False
                            dx, dy = 0, 0
                            # animate frame:[number]/[unit] length:[number]/[unit] effect:[name] dx:[number] dy:[number] (multiplex) (word:word)
                            asv = [
                                x.replace("\x04", " ")
                                for x in opt[8:].replace("^ ", "\x04").split(" ")
                            ]
                            extra = {}
                            for optv in asv:
                                if optv == "multiplex":
                                    mux = True
                                if ":" not in optv:
                                    continue
                                k, v = optv.split(":", 1)
                                v = v.strip()
                                if k == "frame":
                                    v, funit = v.split("/", 1)
                                    frame = float(v)
                                elif k == "length":
                                    v, lunit = v.split("/", 1)
                                    length = float(v)
                                elif k == "effect":
                                    effect = v
                                elif k == "dx":
                                    dx = float(v)
                                elif k == "dy":
                                    dy = float(v)
                                else:
                                    extra[k] = v
                            print("animate", effect, dx, dy, length, frame, extra)
                            if not effect:
                                continue

                            if effect == "distort":
                                extra["arguments"] = aparse(extra.get("arguments", ""))

                            if effect == "text":
                                for prop in ["text", "font-size"]:
                                    extra[prop] = extra.get(prop, None)

                            operations[op].append(
                                {
                                    "frame": {
                                        "start": {"value": frame, "unit": funit},
                                        "length": {"value": length, "unit": lunit},
                                    },
                                    "effect": {
                                        "name": effect,
                                        "multiplex": mux,
                                        "dx": dx,
                                        "dy": dy,
                                        **extra,
                                    },
                                }
                            )
                print(operations)
                if not any(operations[x] for x in operations):
                    print("nothing to do with this")
                    return None
                doc = get_any(message, ["document"])
                if not doc:
                    print("no document for this")
                    return None
                mime = doc.mime_type
                if not mime or not any(x in mime for x in ["gif", "mp4"]):
                    print("not a gif")
                    return None
                # it's a GIF
                operations["format"] = "gif" if "gif" in mime else "mp4"
                operations["message_id"] = message.message_id
                operations["chat_id"] = message.chat_id
                operations["fileid"] = doc.file_id
                insert.update({k: v for k, v in operations.items() if v})
            else:
                return "unsupported_magic::$" + tag

            if not insert["service"]:
                return None

            rmm = await message.reply_text(
                f"will process {tag} and edit this message with the result in a bit."
            )
            insert["response_id"] = [rmm.message_id, rmm.chat_id]
            print(insert)
            self.db.db.tag_updates.insert_one(insert)
            return None
        return tag

    def parse_insertion_statement(
        self,
        stmt: str,
        rev={"(": ")", ")": "(", "{": "}", "}": "{", "<": ">", ">": "<"},
    ):
        tags = []
        stmt = stmt.strip() + " "
        open_paren = {"(": 0, "{": 0, "<": 0}
        tagbuf = ""
        argbuf = ""
        args = []
        escaped = False
        while len(stmt):
            c, stmt = stmt[0], stmt[1:]
            if c == " " and all(o == 0 for v, o in open_paren.items()):
                tags.append((tagbuf, args))
                tagbuf = ""
                argbuf = ""
                args = []
            elif not escaped and c in ["(", "{"]:
                if open_paren[c] == 0:
                    argbuf = ""
                open_paren[c] += 1
            elif not escaped and c in [")", "}"]:
                if open_paren[rev[c]] > 1:
                    open_paren[rev[c]] -= 1
                    argbuf += ")"
                elif open_paren[rev[c]] == 1:
                    open_paren[rev[c]] = 0
                    args.append(argbuf)
                else:
                    tagbuf += ")"
            elif c == "\\":
                if escaped:
                    escaped = False
                    if any(o != 0 for v, o in open_paren.items()):
                        argbuf += c
                    else:
                        tagbuf += c
                else:
                    escaped = True
            elif (
                not escaped and c == "," and any(o != 0 for v, o in open_paren.items())
            ):
                args.append(argbuf)
                argbuf = ""
                while len(stmt) and stmt[0] == " ":
                    stmt = stmt[1:]

            else:
                escaped = False
                if any(o != 0 for v, o in open_paren.items()):
                    argbuf += c
                else:
                    tagbuf += c

        return tags

    async def handle_possible_index_update(
        self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE
    ):
        print("<<<", update)
        reverse = self.context.get("do_reverse", [])
        fuzz = self.context.get("fuzz_reverse", {})
        try:
            if update.message.chat.id in reverse:
                fuzzy = False
                reverse.remove(update.message.chat.id)
                self.context["do_reverse"] = reverse
                if fuzz.get(update.message.chat.id, False):
                    fuzzy = True
                fuzz.pop(update.message.chat.id, None)
                self.context["fuzz_reverse"] = fuzz
                # do a reverse document to tag search
                try:
                    mfield = "file_id"
                    mvalue = get_any(
                        update.message,
                        ["document", "sticker", "animation", "audio", "video", "photo"],
                    )
                    assert mvalue is not None
                    if isinstance(mvalue, list):
                        mvalue = self.random.choice(mvalue)
                    print("found item", mvalue)
                    mvalue = mvalue.file_id
                    if fuzzy:
                        await update.message.reply_text(
                            "Please wait, this might take a moment"
                        )
                        mvalue = xxhash.xxh64(
                            await (
                                await context.bot.get_file(file_id=mvalue)
                            ).download_as_bytearray()
                        ).digest()
                        mfield = "xxhash"
                    print(f"going to look at {mfield} for {mvalue}")
                    wtf = list(
                        self.db.db.message_cache.aggregate(
                            [
                                {"$match": {mfield: mvalue}},
                                {"$project": {"file_id": 0, "type": 0}},
                                {
                                    "$lookup": {
                                        "from": "storage",
                                        "let": {
                                            "chatid": "$chatid",
                                            "msgid": "$msg_id",
                                        },
                                        "pipeline": [
                                            {
                                                "$project": {
                                                    "collections": {
                                                        "$objectToArray": "$collection"
                                                    }
                                                }
                                            },
                                            {"$unwind": "$collections"},
                                            {"$project": {"_id": 0}},
                                            {
                                                "$project": {
                                                    "elem": "$collections.v.index"
                                                }
                                            },
                                            {"$unwind": "$elem"},
                                            {
                                                "$project": {
                                                    "id": "$elem.id",
                                                    "tags": "$elem.tags",
                                                }
                                            },
                                            {
                                                "$match": {
                                                    "$expr": {"$eq": ["$id", "$$msgid"]}
                                                }
                                            },
                                            {"$project": {"tags": 1}},
                                        ],
                                        "as": "tag",
                                    }
                                },
                                {"$unwind": "$tag"},
                                {"$replaceRoot": {"newRoot": "$tag"}},
                                {
                                    "$group": {
                                        "_id": "$_id",
                                        "tags": {"$addToSet": "$tags"},
                                    }
                                },
                                {
                                    "$project": {
                                        "result": {
                                            "$reduce": {
                                                "input": "$tags",
                                                "initialValue": [],
                                                "in": {
                                                    "$concatArrays": [
                                                        "$$value",
                                                        "$$this",
                                                    ]
                                                },
                                            }
                                        }
                                    }
                                },
                                {"$project": {"_id": 0}},
                            ]
                        )
                    )

                    if len(wtf) == 0:
                        # nothing matched
                        await update.message.reply_text(
                            "no documents matching your query found"
                        )
                        return

                    wtf = wtf[0]["result"]
                    print(wtf)
                    await update.message.reply_text(
                        "Found these tags:\n" + "    " + " ".join(wtf)
                    )
                except:
                    traceback.print_exc()
                    pass
                return
        except:
            pass

        # probably index update...or stray message
        username = None
        if update.message:
            if update.message.chat.username:
                username = "@" + update.message.chat.username
        if not username and update.channel_post:
            if update.channel_post.chat.username:
                username = "@" + update.channel_post.chat.username
        if not username and update.effective_user:
            username = str(update.effective_user.id)
        if not username:
            print("uhhhh.... no username?")
            return

        users = [
            x["chat"]
            for x in self.db.db.cindex.aggregate(
                [
                    {"$match": {"index": {"$exists": username}}},
                    {"$project": {"_id": 0, "chat": "$index." + username}},
                    {"$unwind": "$chat"},
                ]
            )
        ]
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
            if text.startswith("set:"):
                move = True
                tags = self.parse_insertion_statement(text[4:].strip())
            elif text.startswith("^delete"):
                delete = True
            elif text.startswith("^set:"):
                reset = True
                tags = self.parse_insertion_statement(text[5:].strip())
            elif text.startswith("^add:"):
                add = True
                tags = self.parse_insertion_statement(text[5:].strip())
            elif text.startswith("^remove:"):
                remove = True
                tags = self.parse_insertion_statement(text[8:].strip())
            elif text.startswith("^tags?"):
                query = True
                users = [msg.from_user.id]
            elif text.startswith(".ext "):
                extern = True
                rest = text[5:]
                users = []
                if rest.startswith(".schedule "):
                    rest = rest[10:]
                    extern_schedule, extern_query = rest.split(" ", maxsplit=1)
                elif rest.startswith(".unschedule "):
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
                    await msg.reply_text(f"Invalid time spec '{extern_schedule}'")
                else:
                    try:
                        job_id = self.enqueue_repeating_task(
                            context,
                            "process_extern_request",
                            tp,
                            extern_query,
                            msg.to_dict(),
                        )
                        await msg.reply_text(
                            f"Will repeat {extern_query} every {tp} seconds!\nuse {job_id} to refer to this job"
                        )
                    except Exception as e:
                        traceback.print_exc()
                        await msg.reply_text(f"Error while processing request: {e}")
            else:
                await self.process_extern_request(extern_query, msg.to_dict(), context)

        for user in users:
            filterop = {}
            updateop = {}
            print("> processing", user)
            try:
                mtags = tags
                collections = list(
                    x["collection"]["k"]
                    for x in self.db.db.storage.aggregate(
                        [
                            {"$match": {"user_id": user}},
                            {
                                "$project": {
                                    "_id": 0,
                                    "collection": {"$objectToArray": "$collection"},
                                }
                            },
                            {"$unwind": "$collection"},
                            {"$match": {"collection.v.id": username}},
                        ]
                    )
                )
                print(">> collections:", collections)

                has_instant = False
                if mtags:
                    m_msgid = None
                    noreply = False
                    m_msg = None
                    try:
                        m_msgid = msg.reply_to_message.message_id
                        m_msg = msg.reply_to_message
                    except:
                        noreply = True
                    ftags = set()
                    for x in mtags:  # no arguments for non-magic tags
                        rtag = await self.handle_magic_tags(
                            early=noreply,
                            tag=x,
                            message=m_msg,
                            insertion_paths=[(coll, m_msgid) for coll in collections],
                            users=[user],
                            context=context,
                        )
                        if not rtag:
                            continue

                        if isinstance(rtag, InstantMagicTag):
                            has_instant = True
                            updateop.update(
                                rtag.generate(
                                    collections=collections,
                                    message=msg,
                                    bot=self,
                                    set=reset,
                                    add=add,
                                    remove=remove,
                                )
                            )
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
                            "$pull": {
                                f"collection.{coll}.index": {"id": msgid}
                                for coll in collections
                            }
                        }
                    except Exception as e:
                        traceback.print_exc()
                        print(e)
                elif move:
                    updateop.update(
                        {
                            "$push": {
                                f"collection.{coll}.index": {"id": msgid, "tags": mtags}
                                for coll in collections
                                for msgid in self.db.db.storage.find_one(
                                    {"user_id": user}
                                )["collection"][coll]["temp"]
                            }
                        }
                    )
                    updateop.update(
                        {
                            "$set": {
                                f"collection.{coll}.temp": [] for coll in collections
                            }
                        }
                    )
                elif add:
                    filterop.update(
                        {
                            "$or": [
                                {
                                    f"collection.{coll}.index.id": msg.reply_to_message.message_id
                                }
                                for coll in collections
                            ]
                        }
                    )
                    updateop.update(
                        {
                            "$push": {
                                f"collection.{coll}.index.$.tags": {"$each": mtags}
                                for coll in collections
                            }
                        }
                    )
                elif remove:
                    filterop.update(
                        {
                            "$or": [
                                {
                                    f"collection.{coll}.index.id": msg.reply_to_message.message_id
                                }
                                for coll in collections
                            ]
                        }
                    )
                    updateop.update(
                        {
                            "$pullAll": {
                                f"collection.{coll}.index.$.tags": mtags
                                for coll in collections
                            }
                        }
                    )
                elif reset:
                    filterop.update(
                        {
                            "$or": [
                                {
                                    f"collection.{coll}.index.id": msg.reply_to_message.message_id
                                }
                                for coll in collections
                            ]
                        }
                    )
                    updateop.update(
                        {
                            "$set": {
                                f"collection.{coll}.index.$.tags": mtags
                                for coll in collections
                            }
                        }
                    )
                elif query:
                    res = sum(
                        list(
                            x["tags"]
                            for x in self.db.db.storage.aggregate(
                                [
                                    {"$match": {"user_id": user}},
                                    {
                                        "$project": {
                                            "_id": 0,
                                            "collection": {
                                                "$objectToArray": "$collection"
                                            },
                                        }
                                    },
                                    {"$unwind": "$collection"},
                                    {"$match": {"collection.v.id": username}},
                                    {"$project": {"collection.v.index": 1}},
                                    {"$unwind": "$collection.v.index"},
                                    {
                                        "$match": {
                                            "collection.v.index.id": msg.reply_to_message.message_id
                                        }
                                    },
                                    {"$project": {"tags": "$collection.v.index.tags"}},
                                ]
                            )
                        ),
                        [],
                    )
                    await msg.reply_text(f'tags: {" ".join(res)}')
                    return
                else:
                    updateop = {
                        "$addToSet": {
                            "collection." + coll + ".temp": msg.message_id
                            for coll in collections
                        }
                    }
                print(updateop, filterop)
                filterop.update(
                    {
                        "user_id": user,
                    }
                )
                self.db.db.storage.update_one(filterop, updateop)
            except Exception as e:
                traceback.print_exc()
                print(e)

    async def process_extern_request(
        self, req, json_msg: dict, context: ContextTypes.DEFAULT_TYPE
    ):
        if isinstance(json_msg, str):
            # migration from old code
            json_msg = json.loads(json_msg)
        msg = telegram.Message.de_json(json_msg, context.bot)

        # @each (req) (lines:it)
        # query
        def parse(x: str):
            only_first = True
            if x.startswith(".first "):
                x = x[7:]
                only_first = True
            if x.startswith("@each "):
                req, *xs = x[6:].splitlines()
                if len(xs) == 0:
                    raise Exception("@each (req) expects a list of things afterwards")
                return ({"first": only_first}, [req.replace("(it)", x) for x in xs])
            return ({"first": only_first}, x)

        async def process(*args, parsed_req=parse(req), **kwargs):
            first = parsed_req[0]["first"]

            def do_respond(*_):
                async def res(inline_query_results, **_):
                    # Extract raw results from the inline query results and reply with them
                    for result in inline_query_results:
                        if not hasattr(result, "title"):
                            continue

                        content = result.input_message_content
                        await context.bot.send_message(
                            chat_id=msg.chat.id,
                            text=content.message_text,
                            parse_mode=content.parse_mode,
                            reply_to_message_id=msg.message_id,
                        )
                        if first:
                            break

                return res

            queues = []
            for req in parsed_req[1]:
                queues.append(
                    self.handle_query(
                        (req, msg),
                        context,
                        respond=do_respond,
                        read=lambda x: x[0],
                        user=lambda x: x[1].from_user,
                    )
                )

            await asyncio.gather(*queues)

        return await process()

    async def handle_list_index(
        self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE
    ):
        coll = self.db.db.storage.find_one(
            filter={"user_id": update.message.from_user.id}
        )
        assert coll is not None, "No collection found for user"
        s = str(coll["collection"])
        if len(s) < 4096:
            await update.message.reply_text(s)
        else:
            await update.message.reply_text("You should get a json file now...")
            await update.message.reply_document(
                document=BytesIO(bytes(json.dumps(coll["collection"]), "utf8")),
                filename="collection.json",
            )

    reg = re.compile(r"\s+")

    def parse_query(self, gquery):
        gquery = gquery.strip() + " "
        coll = ""
        parsed_coll = False
        open_brak = 0
        escaped = False
        query = []
        qstack = []
        qbuf = ""
        extra: dict = {"caption": None}
        if gquery.startswith("+"):
            # +n (pagination)
            gquery = gquery[1:]
            num = re.search(r"^(\d+)\s*(.*)", gquery)
            if num:
                extra["page"] = int(num.group(1))
                gquery = num.group(2)
            else:
                gquery = "+" + gquery
        while len(gquery):
            c, gquery = gquery[0], gquery[1:]
            if c == " " and not parsed_coll and open_brak == 0:
                if len(coll) > 0:
                    parsed_coll = True
                continue
            elif c == " " and open_brak == 1:
                qbuf += " "
            elif c == " ":
                if qbuf:
                    query.append(qbuf)
                    qbuf = ""
            elif c == "{" and not escaped and open_brak == 0:
                open_brak += 1
            elif c == "\\" and not escaped:
                escaped = True
            elif c == "}" and not escaped and open_brak == 1:
                open_brak = 0
                extra["caption"] = qbuf
                qbuf = ""
            elif c == "|" and not escaped and open_brak == 0:
                if qbuf != "":
                    query.append(qbuf)
                    qbuf = ""
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
        aliases = self.db.db.aliases.find_one({"user_id": user_id})
        if not aliases:
            return alias
        return aliases.get("aliases", alias_map).get(alias, alias)

    def add_alias(self, alias, value, user_id):
        self.db.db.aliases.find_one_and_update(
            {"user_id": user_id}, {"$set": {f"aliases.{alias}": value}}, upsert=True
        )

    def add_implicit_alias(self, bot_username, value, user_id):
        self.db.db.aliases.find_one_and_update(
            {"user_id": user_id},
            {"$set": {f"aliases.implicit${bot_username}": value}},
            upsert=True,
        )

    def clone_messaage_with_data(self, data, tags):
        ty = data["type"]
        print("> got some", ty, ":", data)
        if ty == "text":
            return InlineQueryResultArticle(
                id=data["msg_id"],
                title="> " + ", ".join(tags) + " (" + str(data["msg_id"]) + ")",
                input_message_content=InputTextMessageContent(data["text"]),
            )
        elif ty == "mp4":
            return InlineQueryResultCachedMpeg4Gif(
                data["msg_id"], data["file_id"], caption=data.get("caption", None)
            )
        elif ty == "gif":
            return InlineQueryResultCachedGif(
                data["msg_id"], data["file_id"], caption=data.get("caption", None)
            )
        elif ty == "img":
            return InlineQueryResultCachedPhoto(
                data["msg_id"],
                data["file_id"],
                title="> " + ", ".join(tags) + " (" + str(data["msg_id"]) + ")",
                caption=data.get("caption", None),
            )
        elif ty == "sticker":
            return InlineQueryResultCachedSticker(data["msg_id"], data["file_id"])
        elif ty == "doc":
            return InlineQueryResultCachedDocument(
                data["msg_id"],
                "> " + ", ".join(tags) + " (" + str(data["msg_id"]) + ")",
                data["file_id"],
                caption=data.get("caption", None),
            )
        elif ty == "voice":
            return InlineQueryResultCachedVoice(
                data["msg_id"],
                data["file_id"],
                "> " + ", ".join(tags) + " (" + str(data["msg_id"]) + ")",
                caption=data.get("caption", None),
            )
        elif ty == "audio":
            return InlineQueryResultCachedAudio(
                data["msg_id"],
                data["file_id"],
                "> " + ", ".join(tags) + " (" + str(data["msg_id"]) + ")",
                # FIXME: caption...?
                # caption=data.get("caption", None),
            )
        else:
            print("unhandled msg type", ty, "for message", data)
            return None

    async def try_clone_message(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        message,
        tags,
        dcaption,
        fcaption,
        id=None,
        chid=None,
    ):
        try:
            text = message.text
            assert text is not None
            print("> is text")
            data = {"type": "text", "text": text, "chatid": chid, "msg_id": id}
            self.db.db.message_cache.find_one_and_replace(
                {"$and": [{"msg_id": id}, {"chatid": chid}]},
                {k: v for k, v in data.items() if k != "caption"},
                upsert=True,
            )
            return self.clone_messaage_with_data(data, tags)
        except:
            try:
                document = get_any(
                    message, ["document", "animation", "audio", "video", "photo"]
                )
                assert document is not None and [] != document
                mime = None
                if isinstance(document, list):
                    # photo list, we're gonna take a random one for fun
                    print(document)
                    assert len(document) > 0
                    document = self.random.choice(document)
                    mime = "image"
                print("> is some sort of document")
                caption = (
                    fcaption if fcaption not in ["$def", "$default", "$"] else dcaption
                )
                if not mime:
                    mime = document.mime_type
                data = {
                    "file_id": document.file_id,
                    "chatid": chid,
                    "msg_id": id,
                    "xxhash": xxhash.xxh64(
                        await (
                            await context.bot.get_file(file_id=document.file_id)
                        ).download_as_bytearray()
                    ).digest(),
                    "caption": caption,
                }
                if "mp4" in mime:
                    data["type"] = "mp4"
                elif "gif" in mime:
                    data["type"] = "gif"
                elif "image" in mime:
                    data["type"] = "img"
                elif "audio" in mime:
                    data["type"] = "audio"
                else:
                    data["type"] = "doc"
                self.db.db.message_cache.find_one_and_replace(
                    {"$and": [{"msg_id": id}, {"chatid": chid}]},
                    {k: v for k, v in data.items() if k != "caption"},
                    upsert=True,
                )
                return self.clone_messaage_with_data(data, tags)
            except:
                try:
                    sticker = message.sticker
                    assert sticker is not None
                    print("> is a sticker")
                    data = {
                        "type": "sticker",
                        "file_id": sticker.file_id,
                        "chatid": message.chat.id,
                        "msg_id": message.message_id,
                    }
                    self.db.db.message_cache.find_one_and_replace(
                        {
                            "$and": [
                                {"msg_id": data["msg_id"]},
                                {"chatid": data["chatid"]},
                            ]
                        },
                        {k: v for k, v in data.items() if k != "caption"},
                        upsert=True,
                    )
                    return self.clone_messaage_with_data(data, tags)
                except:
                    try:
                        voice = get_any(message, ["voice", "audio"])
                        assert voice is not None
                        print("> is voice")
                        data = {
                            "type": "voice",
                            "file_id": voice.file_id,
                            "chatid": chid,
                            "msg_id": id,
                            "xxhash": xxhash.xxh64(
                                await (
                                    await context.bot.get_file(file_id=voice.file_id)
                                ).download_as_bytearray()
                            ).digest(),
                        }
                        self.db.db.message_cache.find_one_and_replace(
                            {"$and": [{"msg_id": id}, {"chatid": chid}]},
                            {k: v for k, v in data.items() if k != "caption"},
                            upsert=True,
                        )
                        return self.clone_messaage_with_data(data, tags)
                    except Exception as e:
                        traceback.print_exc()
                        print(
                            "exception occured while processing",
                            message,
                            "with tags",
                            tags,
                        )
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
                    for tt in tts[:]:  # ouch
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

    async def external_source_handler(
        self,
        data: dict,
        context: ContextTypes.DEFAULT_TYPE,
        update: T,
        user_data=None,
        chat_data=None,
        respond: Callable[
            [T], Callable[..., Awaitable]
        ] = lambda x: x.inline_query.answer,  # type: ignore
        read: Callable[[T], str] = lambda x: x.inline_query.query,  # type: ignore
        user: Callable[
            [T], Optional[telegram.User]
        ] = lambda x: x.inline_query.from_user,  # type: ignore
    ):
        coll = None
        query = None

        try:
            coll, *ireqs = data["source"].split(":")
            ireqs = ":".join(ireqs)
            query = data["query"]
            if coll == "anilist":
                if ireqs == "ql":
                    # raw query
                    await respond(update)(
                        [
                            InlineQueryResultArticle(
                                id=str(uuid4()),
                                title="Raw request results for " + query,
                                input_message_content=InputTextMessageContent(
                                    json.dumps(aniquery(query, {}))
                                ),
                            )
                        ]
                    )
                elif ireqs == "aggql":
                    # aggregate query response
                    # parse query: `query` `aggregate`
                    match = re.search(r"^\`([^\`]+)\`\s*\`([^\`]+)\`$", query)
                    if match is None:
                        return
                    q0, q1 = match.group(1, 2)

                    coll = self.db.client[f"temp_{user(update).id}"]
                    db = coll["temp"]
                    db.insert_one(aniquery(q0, {}))
                    res = db.aggregate(json.loads(q1))
                    await respond(update)(
                        [
                            InlineQueryResultArticle(
                                id=str(uuid4()),
                                title="Aggregate request results for " + q0,
                                input_message_content=InputTextMessageContent(
                                    dumps(list(res), indent=4)
                                ),
                            )
                        ]
                    )
                    db.drop()
                elif ireqs == "id":
                    await respond(update)(iquery_render(query))
                elif ireqs == "one":
                    await respond(update)(qquery_render(query))
                elif ireqs == "bychar":
                    # find result by character
                    await respond(update)(cquery_render(query))
                elif ireqs == "char":
                    await respond(update)(charquery_render(query))
                elif ireqs == "":
                    # simple query
                    await respond(update)(squery_render(data["query"]))
                else:
                    raise Exception(
                        f"Arguments to source {coll} not understood ({ireqs})"
                    )
            elif self.has_api(user(update).id, coll):
                try:
                    res = self.invoke(coll, ireqs, query)
                    await respond(update)(self.render_api(coll, ireqs, res))
                except Exception as e:
                    raise Exception(f"Invalid API invocation: {e}")
            else:
                raise Exception(f"Undefined source {coll}")
        except Exception as e:
            print(e)
            traceback.print_exc()
            await respond(update)(
                [
                    InlineQueryResultArticle(
                        id=str(uuid4()),
                        title=f"Exception <{e}> occured while processing {query} in external source {coll}",
                        input_message_content=InputTextMessageContent(
                            f"allow me to let you in on a secret,\nThis bot is actually dumb\n{e}"
                        ),
                    )
                ],
                cache_time=10,
            )

    async def handle_query(
        self,
        update: T,
        context: ContextTypes.DEFAULT_TYPE,
        user_data=None,
        chat_data=None,
        respond: Callable[[T], Callable[..., Awaitable]] = lambda x: x.inline_query.answer,  # type: ignore
        read: Callable[[T], str] = lambda x: x.inline_query.query,  # type: ignore
        user: Callable[[T], Optional[telegram.User]] = lambda x: x.inline_query.from_user,  # type: ignore
    ):
        query = None
        coll = None

        try:
            implicit_collection = self.resolve_alias(
                f"implicit${context.bot.username}", user(update).id
            )
            query = read(update)
            if not implicit_collection.startswith("implicit$"):
                query = implicit_collection + " " + query
            original_query = query
            coll, query, extra = self.parse_query(query)
            coll = self.resolve_alias(coll, user(update).id)
            possible_update = self.db.db.late_share.find_one_and_delete(
                {"username": user(update).username}
            )
            while possible_update:
                # someone has shared stuff with this guy
                shares = possible_update["shares"]
                for share in shares:
                    for mshare in share:
                        self.db.db.storage.update_one(
                            {"user_id": user(update).id},
                            {
                                "$set": {
                                    "collection."
                                    + mshare: share[mshare][
                                        0
                                    ],  # This is stored as an array
                                    "last_used." + mshare: [],
                                }
                            },
                            upsert=True,
                        )
                possible_update = self.db.db.late_share.find_one_and_delete(
                    {"username": user(update).username}
                )

            if coll.startswith("@"):
                # external sources
                return await self.external_source_handler(
                    {
                        "source": coll[1:],
                        "query": " ".join(original_query.strip().split(" ")[1:]),
                    },
                    context,
                    update,
                    user_data,
                    chat_data,
                    respond,
                    read,
                    user,
                )
            fcaption = extra.get("caption", None)
            print(read(update), "->", repr(coll), repr(query), extra)
            if not coll or coll == "":
                return

            print(coll, query)
            if any(x in coll for x in "$./[]"):
                await respond(update)(
                    [
                        InlineQueryResultArticle(
                            id=str(uuid4()),
                            title='Invalid collection name "' + coll + '"',
                            input_message_content=InputTextMessageContent(
                                "This user is an actual idiot"
                            ),
                        )
                    ]
                )
                return
            query = self.process_search_query_further(query)

            colls = (
                list(
                    (
                        x["index"]["id"],
                        x["index"]["tags"],
                        x["index"].get("caption", None),
                        x["index"].get("cache_stale", False),
                    )
                    for x in self.db.db.storage.aggregate(
                        [
                            {"$match": {"user_id": user(update).id}},
                            {
                                "$project": {
                                    "index": "$collection." + coll + ".index",
                                    "_id": 0,
                                }
                            },
                            {"$unwind": "$index"},
                            {
                                "$match": {
                                    "$or": [{"index.tags": {"$all": q}} for q in query]
                                }
                            },
                            {"$limit": 5},
                        ]
                    )
                )
                if query
                else []
            )
            results: list[InlineQueryResult] = [
                InlineQueryResultArticle(
                    id=str(uuid4()),
                    title=">> "
                    + "|".join(
                        (" ".join(q) for q in (query or [["Your Recent Selections"]]))
                    ),
                    input_message_content=InputTextMessageContent(
                        "Search for `"
                        + "|".join(" ".join(q) for q in query)
                        + "' and more~"
                        if len(query)
                        else "Yes, these are your recents"
                    ),
                )
            ]
            userdata = self.db.db.storage.find_one({"user_id": user(update).id})
            chatid = userdata["collection"][coll]["id"]
            cachetime = 300
            if len(query) == 0:
                last_used = self.db.db.storage.find_one(
                    {"user_id": user(update).id},
                    projection={
                        "_id": 0,
                        "collection": 0,
                    },
                )["last_used"]
                print(">>>", last_used)
                last_used = last_used.get(coll, [])
                print(">>> || ", last_used, "in", chatid)
                for msgid in last_used:
                    msgid = int(msgid)
                    cmsg = self.db.db.message_cache.find_one(
                        {"$and": [{"chatid": chatid}, {"msg_id": msgid}]}
                    )
                    if not cmsg:
                        print("> id", msgid, "not found...?")
                    cmsg["caption"] = fcaption
                    copy = self.clone_messaage_with_data(cmsg, ["last", "used"])
                    if copy:
                        results.append(copy)
            if len(results) > 1:
                await respond(update)(results, cache_time=20, is_personal=True)
                return
            if len(colls) < 1:
                results.append(
                    InlineQueryResultArticle(
                        id=str(uuid4()),
                        title="no result matching query found",
                        input_message_content=InputTextMessageContent(
                            f'<imaginary result matching {"|".join(" ".join(q) for q in query)}>'
                        ),
                    )
                )
                cachetime = 60
            tempid = userdata.get("temp", None)
            print(userdata)

            if not tempid:
                results.append(
                    InlineQueryResultArticle(
                        id=str(uuid4()),
                        title="no temp set, some results might not be available",
                        input_message_content=InputTextMessageContent(
                            'no temp is set, "/temp <temp_chat_username>" in the bot chat'
                        ),
                    )
                )
            else:
                tempid = tempid["id"]

            for col in colls:
                try:
                    cloned_message = None
                    print(tempid, chatid, col)
                    cmsg = self.db.db.message_cache.find_one(
                        {"$and": [{"chatid": chatid}, {"msg_id": col[0]}]}
                    )
                    if cmsg and not col[3]:
                        print("cache hit for message", col[0], ":", cmsg)
                        cmsg["caption"] = (
                            fcaption
                            if fcaption not in ["$def", "$default", "$"]
                            else col[2]
                        )
                        cloned_message = self.clone_messaage_with_data(cmsg, col[1])
                    elif tempid:
                        print(
                            "cache miss for message",
                            col[0],
                            ("(cache was stale) ::" if col[3] else "::"),
                            "trying to load it",
                        )
                        msg = await context.bot.forward_message(
                            chat_id=tempid,
                            from_chat_id=chatid,
                            message_id=col[0],
                            disable_notification=True,
                        )
                        cloned_message = await self.try_clone_message(
                            context,
                            msg,
                            col[1],
                            id=col[0],
                            chid=chatid,
                            fcaption=fcaption,
                            dcaption=col[2],
                        )
                        print("duplicated message found:", msg)

                        await msg.delete()

                    if cloned_message is None:
                        print("message clone failed for", col[0])
                        continue
                    results.append(cloned_message)
                except Exception as e:
                    cachetime = 10
                    results.append(
                        InlineQueryResultArticle(
                            id=str(uuid4()),
                            title=f"Exception <{e}> occured while processing {col}",
                            input_message_content=InputTextMessageContent(
                                f"This bot is actually dumb\nException: {e}"
                            ),
                        )
                    )
            page = extra.get("page", 0)
            start_index = min(page * 20, len(results))
            end_index = min(start_index + 20, len(results))
            if start_index == end_index:
                start_index = 0
                end_index = 20
            await respond(update)(
                results[start_index:end_index], cache_time=cachetime, is_personal=True
            )
        except Exception as e:
            print(e)
            traceback.print_exc()
            await respond(update)(
                [
                    InlineQueryResultArticle(
                        id=str(uuid4()),
                        title=f"Exception <{e}> occured while processing {query} in {coll}",
                        input_message_content=InputTextMessageContent(
                            f"This bot is actually dumb\n{e}\nHint: you might be searching a nonexistent collection"
                        ),
                    )
                ],
                cache_time=10,
            )

    async def start_option_set(
        self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE
    ):
        # update.message.reply_text('Add this bot to a group/channel (or use this chat) and give me its ID or username')
        txt = update.message.text[len("/connect ") :].strip()
        if txt == "":
            await update.message.reply_text(
                "connect what? (repeat command with argument)"
            )
            return

        if any(x in txt for x in "$./[]"):
            await update.message.reply_text('Invalid collection name "' + txt + '"')
            return

        await update.message.reply_text("setting option " + txt)
        mcontext = {}
        mcontext["option"] = txt
        self.context[update.message.from_user.id] = mcontext

    async def set_temp(
        self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE
    ):
        username = update.message.text[len("/temp ") :]
        if username == "":
            await update.message.reply_text(
                "set temp to what? (repeat command with argument)"
            )
            return

        self.db.db.storage.update_one(
            {"user_id": update.message.from_user.id},
            {
                "$set": {
                    "temp": {
                        "id": username,
                    },
                }
            },
            upsert=True,
        )

        await update.message.reply_text("set temp storage to " + username)

    async def share(self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE):
        # share <collection> with <@username|id>
        command = update.message.text[len("/share ") :]
        try:
            coll, _, user_id, *_ = command.split(" ")
            try:
                coll = self.resolve_alias(coll, update.message.from_user.id)
                colls = list(
                    x["collection"]
                    for x in self.db.db.storage.aggregate(
                        [
                            {"$match": {"user_id": update.message.from_user.id}},
                            {
                                "$project": {
                                    "collection": "$collection." + coll,
                                    "_id": 0,
                                }
                            },
                        ]
                    )
                )
                if not colls:
                    raise Exception("ENOTFOUND")
                # we have the collection to share
                # now to insert it under the target
                mcoll = (
                    (
                        update.message.from_user.username
                        or "id" + str(update.message.from_user.id)
                    )
                    + "-"
                    + coll
                )
                if user_id.startswith("@"):
                    # username, add it to a different db, and resolve it later
                    self.db.db.late_share.update_one(
                        {"username": user_id[1:]},
                        {"$addToSet": {"shares": {mcoll: colls}}},
                        upsert=True,
                    )
                    await update.message.reply_text(
                        f"Shared collection {coll} with username {user_id}, they will receive it as soon as they try to access it (under name {mcoll})"
                    )
                else:
                    # user id
                    self.db.db.storage.update_one(
                        {"user_id": int(user_id)},
                        {
                            "$set": {
                                "collection." + mcoll: colls[0],
                                "last_used." + mcoll: [],
                            }
                        },
                        upsert=True,
                    )
                    await update.message.reply_text(
                        f"Shared collection {coll} with user {user_id} as {mcoll}"
                    )
            except Exception as e:
                print("cannot share", coll, "because", e)
                await update.message.reply_text(
                    f"Unknown collection {coll}, you can't share that which you do not have"
                )
        except:
            await update.message.reply_text(
                "Invalid command format, use /share <collection> with <@username|id>"
            )

    async def handle_api(
        self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE
    ):
        ssubcommand = update.message.text[len("/api ") :]
        cmd, *args = ssubcommand.split(" ")
        try:
            if cmd == "redeclare":
                # redeclare <name> <comm_type> <input> <output> <api_path>
                if len(args) != 5:
                    await update.message.reply_text(
                        f"invalid number of arguments, expected 5 (declare <name> <comm_type> <input> <output> <api_path>), got {len(args)}"
                    )
                    return
                name, comm_type, inp, out, path = args
                if comm_type not in self.external_api_handler.comms:
                    await update.message.reply_text(
                        f"invalid comm_type {comm_type}, valid types are: {self.external_api_handler.comms}"
                    )
                    return

                del self.external_api_handler.apis[name]
                self.external_api_handler.declare(name, comm_type, inp, out, path)
                await update.message.reply_text(
                    f"registered api {name} as {path}, with input {inp} and output {out} for you.\nnow define the IOs"
                )
            if cmd == "declare":
                # declare <name> <comm_type> <input> <output> <api_path>
                if len(args) != 5:
                    await update.message.reply_text(
                        f"invalid number of arguments, expected 5 (declare <name> <comm_type> <input> <output> <api_path>), got {len(args)}"
                    )
                    return
                name, comm_type, inp, out, path = args
                if comm_type not in self.external_api_handler.comms:
                    await update.message.reply_text(
                        f"invalid comm_type {comm_type}, valid types are: {self.external_api_handler.comms}"
                    )
                    return

                self.external_api_handler.declare(name, comm_type, inp, out, path)
                await update.message.reply_text(
                    f"registered api {name} as {path}, with input {inp} and output {out} for you.\nnow define the IOs"
                )
            elif cmd == "redefine":
                # redefine [input/output] <name> <type> <vname> ...request_body
                if len(args) < 4:
                    await update.message.reply_text(
                        f"invalid number of arguments, expected at least 4 arguments (define [input/output] <name> <type> <vname> ...request_body), but got {len(args)}"
                    )
                    return
                iotype, name, _type, vname, *req = args
                if iotype not in ["input", "output"]:
                    await update.message.reply_text(
                        f"invalid io type {iotype}, expected either `input` or `output`"
                    )
                    return
                del self.external_api_handler.ios[iotype][name]
                self.external_api_handler.define(
                    iotype, name, vname, _type, " ".join(req)
                )
                await update.message.reply_text(
                    f'registered {iotype} adapter({_type}) {name} as `{" ".join(req)}`({vname})'
                )
                return
            elif cmd == "define":
                # define [input/output] <name> <type> <vname> ...request_body
                if len(args) < 4:
                    await update.message.reply_text(
                        f"invalid number of arguments, expected at least 4 arguments (define [input/output] <name> <type> <vname> ...request_body), but got {len(args)}"
                    )
                    return
                iotype, name, _type, vname, *req = args
                if iotype not in ["input", "output"]:
                    await update.message.reply_text(
                        f"invalid io type {iotype}, expected either `input` or `output`"
                    )
                    return
                self.external_api_handler.define(
                    iotype, name, _type, vname, " ".join(req)
                )
                await update.message.reply_text(
                    f'registered {iotype} adapter({_type}) {name} as `{" ".join(req)}`({vname})'
                )
                return
            elif cmd == "list":
                if len(args) < 1:
                    await update.message.reply_text(
                        f"invalid number of arguments, expected 1 argument (list [api/io/input/output])"
                    )
                    return
                arg = args[0]
                if arg == "api":
                    await update.message.reply_text(
                        "\n".join(
                            f"{x}: ({v[0]}) input<{v[1]}> output<{v[2]}> = {v[3]}"
                            for x, v in self.external_api_handler.apis.items()
                        )
                    )
                    return
                elif arg == "input":
                    await asyncio.gather(
                        *[
                            update.message.reply_text(f"{x}\n{y}\n")
                            for x, y in self.external_api_handler.input_adapters.items()
                        ]
                    )
                    return
                elif arg == "output":
                    await asyncio.gather(
                        *[
                            update.message.reply_text(f"{x}\n{y}\n")
                            for x, y in self.external_api_handler.output_adapters.items()
                        ]
                    )
                    return
                else:
                    await update.message.reply_text(f"unknown subcommand {arg}")
            else:
                await update.message.reply_text("unknown command")
                return
        except Exception as e:
            await update.message.reply_text(str(e))

    async def handle_alias(
        self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE
    ):
        ssubcommand = update.message.text[len("/alias ") :]
        cmd, *args = ssubcommand.split(" ")
        if cmd == "set":
            if len(args) != 2:
                await update.message.reply_text(
                    f"invalid number of arguments, expected 2, got {len(args)}"
                )
                return
            self.add_alias(args[0], args[1], update.message.from_user.id)
            await update.message.reply_text(
                f'Added collection alias "{args[0]}" for "{args[1]}"'
            )
            return
        elif cmd == "get":
            await update.message.reply_text(
                f'collection "{args[0]}" resolves to "{self.resolve_alias(args[0], update.message.from_user.id)}"'
            )
            return
        elif cmd == "implicit":
            if len(args) != 1:
                await update.message.reply_text(
                    f"invalid number of arguments, expected 1, got {len(args)}"
                )
                return
            self.add_implicit_alias(
                context.bot.username, args[0], update.message.from_user.id
            )
            await update.message.reply_text(
                f'Added implicit collection alias "{args[0]}" for bot "{context.bot.username}" (this one)'
            )
            return
        else:
            await update.message.reply_text(f"Unknown command")
            return

    async def set_option(
        self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE
    ):
        mcontext = self.context.get(update.message.from_user.id, None)
        if not mcontext or not mcontext["option"]:
            await update.message.reply_text("set in what context? (/connect <context>)")
            return
        username = update.message.text[len("/set ") :]
        if username == "":
            await update.message.reply_text("set what? (repeat command with argument)")
            return

        self.db.db.storage.update_one(
            {"user_id": update.message.from_user.id},
            {
                "$set": {
                    "collection."
                    + mcontext["option"]: {"id": username, "index": [], "temp": []},
                    "last_used." + mcontext["option"]: [],
                }
            },
            upsert=True,
        )
        try:
            self.db.db.cindex.update_one(
                {},
                {"$addToSet": {"index." + username: update.message.from_user.id}},
                upsert=True,
            )
        except Exception as e:
            traceback.print_exc()
            print(e)
        del self.context[update.message.from_user.id]

    async def help(self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE):
        for sequence in HELP_TEXTS:
            prior = None
            for text in sequence["list"]:
                if prior is None:
                    prior = await update.message.reply_text(
                        text, parse_mode=sequence["mode"]
                    )
                else:
                    prior = await prior.reply_text(text, parse_mode=sequence["mode"])

    async def help_magics(
        self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE
    ):
        for i in MAGIC_HELP_TEXTS:
            await update.message.reply_text(i, parse_mode=ParseMode.MARKDOWN)

    async def reverse_search(
        self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE, fuzzy=False
    ):
        await update.message.reply_text(
            "Send the document/image/GIF (text will not be processed)"
        )
        ctx = self.context.get("do_reverse", [])
        fz = self.context.get("fuzz_reverse", {})
        ctx.append(update.message.from_user.id)
        self.context["do_reverse"] = ctx
        fz[update.message.from_user.id] = fuzzy
        self.context["fuzz_reverse"] = fz

    async def reverse_search_fuzzy(
        self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE
    ):
        return self.reverse_search(update, context, fuzzy=True)

    async def rehash_all(
        self, update: telegram.Update, context: ContextTypes.DEFAULT_TYPE
    ):
        index = list(
            (x["_id"], x["file_id"])
            for x in self.db.db.message_cache.find(
                {"$and": [{"file_id": {"$ne": None}}, {"cache_stale": None}]}
            )
        )
        await update.message.reply_text(f"found {len(index)} items, updating...")
        mod = 0
        for item in index:
            try:
                h = xxhash.xxh64(
                    await (
                        await context.bot.get_file(file_id=item[1])
                    ).download_as_bytearray()
                ).digest()
                mod += self.db.db.message_cache.update_one(
                    {"_id": item[0]}, {"$set": {"xxhash": h}}
                ).modified_count
            except:
                traceback.print_exc()
        await update.message.reply_text(f"Rehash done, updated {mod} entries")


class InstantMagicTag:
    def __init__(
        self, name, argument=None, arguments=None, handler=None, can_handle=None
    ):
        self.name = name
        self.handler = handler
        self.can_handle = can_handle or []
        if argument:
            self.arguments = [argument]
            self.kind = "single"
        elif arguments:
            self.arguments = arguments
            self.kind = "multiple"
        else:
            self.arguments = [None]
            self.kind = "single"

    def generate(self, collections, message, bot, **kwargs):
        if not self.handler or not all(
            x in self.can_handle for x in kwargs if kwargs[x]
        ):
            return {}
        return self.handler(collections, message, bot, self.arg)

    @property
    def arg(self):
        if self.kind == "single":
            return self.arguments[0]
        else:
            return self.arguments
