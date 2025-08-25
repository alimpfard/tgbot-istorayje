import os
import json, requests

PKE_TAGIFY_URL = os.environ["PKE_TAGIFY_URL"]


def pke_tagify(docs: list):
    res = requests.post(PKE_TAGIFY_URL + "/tagify", json=docs)
    try:
        return res.json()
    except:
        print(res, res.content)
        return None


def get_some_frame(url, format=None):
    print(url)
    res = requests.post(
        PKE_TAGIFY_URL + "/getframe", json={"url": url, "format": format}
    )
    return res.content


def process_gifops(url: str, ops: dict, format: str):
    res = requests.post(
        PKE_TAGIFY_URL + "/gifop", json={"url": url, "ops": ops, "format": format}
    )
    return res.content


def store_image(bot, file, chat):
    msg = bot.updater.bot.send_document(
        chat_id=chat, document=file, disable_notification=True
    )
    print(msg)
    return msg.effective_attachment.file_id
