import requests
import json
from lxml import html as xhtml
import magic
import re

POST_URL = "http://kanotype.iptime.org:8003/deepdanbooru/upload"
Mime = magic.Magic(mime=True)


def deepdan(image_data, *args):
    content_type = Mime.from_buffer(image_data)
    if content_type != "image/png" and content_type != "image/jpeg":
        return None
    fname = "request.jpeg"
    if content_type == "image/png":
        fname = "request.png"
    req = requests.post(
        POST_URL,
        files=(
            ("network_type", (None, "general")),
            ("file", (fname, image_data, content_type)),
        ),
    )
    return deepdan_parse(req.content)


XPATH = "/html/body/div/div/div/div[1]/div[1]/table/tbody/tr"
SPLIT_RE = re.compile(r"\s+")


def split(x):
    x = x.strip()
    tagscore = SPLIT_RE.split(x)
    if len(tagscore) > 1:
        tag, score = tagscore
        return (tag, float(score))
    else:
        return (tagscore, 1)


def deepdan_parse(content):
    xml = xhtml.fromstring(content)
    xps = xml.xpath(XPATH)
    return [split(x.text_content()) for x in xps]
