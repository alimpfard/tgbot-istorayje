import requests
import json
from lxml import html as xhtml
import magic
import re


POST_URL = "https://saucenao.com/search.php"


def searchSauceNao(link, *args):
    req = requests.post(
        POST_URL,
        files=(
            ("network_type", (None, "general")),
            ("file", ("", "", "application/octet-stream")),
            ("url", (None, link)),
            ("hide", (None, "0")),
            ("database", (None, "999")),
        ),
    )
    return saucenao_parse(req.content)


XPATH = '/html/body/div[2]/div[3]/div/table/tr/td[@class = "resulttablecontent"]'
SIMIL_XP = 'div[@class = "resultmatchinfo"]'
TITLE_XP = 'div/div[@class = "resulttitle"]'
CONT_XP = 'div/div[@class = "resultcontentcolumn"]'


def handle(node):
    mi = node.xpath(SIMIL_XP)
    ti = node.xpath(TITLE_XP)
    co = node.xpath(CONT_XP)

    if len(mi) == 0:
        mi = "50.00%"
    else:
        mi = mi[0].text_content()

    if len(ti) == 0:
        ti = "<Unknown Piece>"
    else:
        ti = ti[0].text_content()

    if len(co) == 0:
        co = "<No info>"
    else:
        x = co[0]
        for br in x.xpath("br"):
            br.tail = "\n" + br.tail if br.tail else "\n"
        co = x.text_content().strip()

    return {"similarity": float(mi[:-1]), "title": ti, "content": co}


def saucenao_parse(content):
    xml = xhtml.fromstring(content)
    xps = xml.xpath(XPATH)
    return [handle(x) for x in xps]
