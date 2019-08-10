import os
import json, requests

PKE_TAGIFY_URL = os.environ['PKE_TAGIFY_URL']

def pke_tagify(docs: list):
    res = requests.post(PKE_TAGIFY_URL, json=docs)
    return res.json()