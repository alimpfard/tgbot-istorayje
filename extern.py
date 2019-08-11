import os
import json, requests

PKE_TAGIFY_URL = os.environ['PKE_TAGIFY_URL']
STORE_URL = os.environ['STORE_URL']

def pke_tagify(docs: list):
    res = requests.post(PKE_TAGIFY_URL + '/tagify', json=docs)
    try:
        return res.json()
    except:
        print(res, res.content)
        return None

def get_some_frame(url, format=None):
    print(url)
    res = requests.post(PKE_TAGIFY_URL + '/getframe', json={'url': url, 'format': format})
    return res.content

def store_image(content, width, height, type, checksum):
    res = requests.post(STORE_URL,
            params={
                'width': width,
                'height': height,
                'type': type,
                'checksum': checksum,
            },
            data=content,
            headers={
                'User-Agent': "",
                'TE': 'Trailers',
                'Content-Type': 'raw'
            })
    try:
        return res.json().get('alias', None)
    except:
        print(res, res.content)
        return None

