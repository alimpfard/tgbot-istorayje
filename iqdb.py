import requests
import json
from lxml import html as xhtml
import magic
import re


GET_URL = 'https://iqdb.org/?url='

def searchIqdb(link, *args):
    req = requests.get(GET_URL + link)
    return iqdb_parse(req.content)

XPATH = '/html/body/div[2]/div/table'
SIMIL_XP = 'tr[5]'
TITLE_XP = 'tr/td/a/img/@src'
CONT_XP = 'tr/td/a/img/@title'
def handle(node):
    mi = node.xpath(SIMIL_XP)
    if not mi:
        return None

    ti = node.xpath(TITLE_XP)
    if not ti:
        return None

    co = node.xpath(CONT_XP)
    if not co:
        return None

    if len(mi) == 0:
        mi = '50.00% similarity'
    else:
        mi = mi[0].text_content()
    
    if len(ti) == 0:
        ti = '<Unknown Piece>'
    else:
        ti = ti[0]
    
    if len(co) == 0:
        co = '<No info>'
    else:
        co = co[0]

    return {'similarity': float(mi[:-1 - len(' similarity')]), 'title': ti, 'content': co}
    
def iqdb_parse(content):
    xml = xhtml.fromstring(content)
    xps = xml.xpath(XPATH)
    return list(filter(lambda x: x, [handle(x) for x in xps]))