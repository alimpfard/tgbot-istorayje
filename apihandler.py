import ast
import astor
import re
import requests
import json 
from telegram import (
    InlineQueryResultArticle, ParseMode, InputTextMessageContent
)
from uuid import uuid4
import urllib

from html.parser import HTMLParser
from lxml import html as xhtml


def subv(xbody, iotype, name):
    xast = ast.parse(xbody, f'{iotype}:{name}.body', 'single').body[0]
    if isinstance(xast, ast.Assign):
        if isinstance(xast.value, ast.Compare) and len(xast.value.ops) == 1 and isinstance(xast.value.ops[0], ast.In):
            # x = y in z -> (lambda x: z)(y)
            rast : ast.Call = ast.parse('x(y)', f'{iotype}:{name}.replacement_body', 'eval').body # call(...)
            targets = xast.targets[0]
            if isinstance(targets, ast.Tuple):
                targets = targets.elts
            else:
                targets = [targets]
            rast.func = ast.Lambda(ast.arguments([ast.arg(x, None) for x in targets], None,None,None,None,[]) , (lambda x: x if len(targets) == 1 else ast.Starred(x)) (xast.value.comparators[0]))
            rast.args = [xast.value.left]
            xast = rast
    return astor.to_source(xast)

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

class DotDict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __init__(self, dct):
        for key, value in dct.items():
            if hasattr(value, 'keys'):
                value = DotDict(value)
            elif isinstance(value, list):
                value = [DotDict({'a':x}).a for x in value]
            self[key] = value

class APIHandler(object):
    def __init__(self, bot):
        self.bot = bot
        self.input_adapters = None
        self.output_adapters = None
        self.apis = None
        self.load()
        self.res = {}
        self.ios = {
            'input': self.input_adapters,
            'output': self.output_adapters,
        }
        self.comms = (
            'graphql',
            'json/post',
            'html/xpath',
            'http/link',
            'http/json',
        )
        self.metavarre = re.compile(r'(?!\\)\$([\w:]+)')

    def flush(self):
        self.bot.db.db.external_apis.update_one({'kind': 'api'}, {'$set': {'data': self.apis}}, upsert=True)
        self.bot.db.db.external_apis.update_one({'kind': 'input'}, {'$set': {'data': self.input_adapters}}, upsert=True)
        self.bot.db.db.external_apis.update_one({'kind': 'output'}, {'$set': {'data': self.output_adapters}}, upsert=True)

    def load(self):
        self.apis = (self.bot.db.db.external_apis.find_one({'kind': 'api'}) or {'data': {}})['data']
        self.input_adapters = (self.bot.db.db.external_apis.find_one({'kind': 'input'}) or {'data': {}})['data']
        self.output_adapters = (self.bot.db.db.external_apis.find_one({'kind': 'output'}) or {'data': {}})['data']
    
    def gmetavarre(self, name):
        if name in self.res:
            return self.res[name]
        mre = re.compile(f'(?!\\\\)\\$({name})')
        self.res[name] = mre
        return mre

    def define(self, iotype, name, vname, body):
        if name in self.ios[iotype]:
            raise Exception(f'duplicate {iotype} IO {name}')
        
        replacements = set()
        for metavar in self.metavarre.finditer(body):
            if metavar.group(1) != vname:
                if metavar.group(1) in self.ios[iotype]:
                    replacements.add(metavar.group(1))
                else:
                    raise Exception(f'Unknown meta variable `{metavar.group(1)}` (at offset {metavar.pos})')
        xbody = self.gmetavarre(vname).sub(vname, body)
        
        for r in replacements:
            xbody = self.gmetavarre(r).sub(f'({self.ios[iotype][r]})', xbody)

        if not xbody:
            xbody = vname
        xbody = subv(xbody, iotype, name)
        body = f'lambda {vname}: {xbody}'
        compile(body, f'{iotype}:{name}', 'eval', dont_inherit=True)

        self.ios[iotype][name] = (vname, body)
        self.flush()

    def tgwrap(self, query, stuff):
        return [
            InlineQueryResultArticle(
                id=uuid4(),
                title=f"result {k}",
                input_message_content=InputTextMessageContent(x)
            )
        for k,x in stuff]

    def declare(self, name, comm_type, inp, out, path):
        if name in self.apis:
            raise Exception(f'duplicate API name {name}')
        
        for metavar in self.metavarre.finditer(path):
            if metavar.group(1) != 'result':
                raise Exception(f'Unknown meta variable `{metavar.group(1)}` (at offset {metavar.pos})')

        self.apis[name] = (comm_type, inp, out, path)
        self.flush()

    def adapter(self, name, adapter, value, env=None):
        vname, body = adapter
        if env is None:
            env = {}
        env.update({'strip_tags': strip_tags})
        return eval(compile(body, name, 'eval', dont_inherit=True), env, {})(value)

    def invoke(self, api, query):
        comm_type, inp, out, path = self.apis[api]
        if inp not in self.input_adapters:
            raise Exception(f'Undefined input adapter {inp}')
        
        if out not in self.output_adapters:
            raise Exception(f'Undefined ouput adapter {out}')

        inpv = self.input_adapters[inp]

        q = self.adapter(inp, inpv, query)
        if comm_type == 'http/link':
            path = self.metavarre.sub(urllib.parse.quote_plus(q), path)
            return path
        
        if comm_type == 'json/post':
            return DotDict({'x':requests.post(path, data=q).json()}).x
        
        if comm_type == 'http/json':
            path = self.metavarre.sub(urllib.parse.quote_plus(q), path)
            return DotDict({'x': requests.get(path).json()}).x
        
        if comm_type == 'html/xpath':
            path = self.metavarre.sub(urllib.parse.quote_plus(q), path)
            req = requests.get(path)
            if req.status_code != 200:
                raise Exception(f'{req.status_code}: {req.reason}')
            xml = xhtml.fromstring(req.content)
            return lambda x, xml=xml: xml.xpath(x)

        raise Exception(f'type {comm_type} not yet implemented')
    
    def render(self, api, value):
        comm_type, inp, out, path = self.apis[api]
        
        outv = self.output_adapters[out]
        q = self.adapter(out, outv, value)
        return self.tgwrap(api, q)