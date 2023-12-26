import ast
from os import environ

import astor
import re
import requests
import json
from telegram import (
    InlineQueryResultArticle, ParseMode, InputTextMessageContent,
    InlineQueryResultPhoto, InlineQueryResultGif
)
from uuid import uuid4
import urllib
from PIL import Image

from html.parser import HTMLParser
from lxml import html as xhtml
from flask import Flask, request, make_response
from threading import Thread

import xxhash

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
    return xast

def to_json(x):
    return json.dumps(x)

def suppress_exceptions(f):
    try:
        return f()
    except:
        return None

class InternalPhoto:
    def __init__(self, url, thumb_url=None, caption=None):
        self.url = url
        self.caption = caption
        self.thumb_url = thumb_url or url

def construct_image(obj):
    if isinstance(obj, str):
        return InternalPhoto(obj)
    if isinstance(obj, dict):
        return InternalPhoto(**obj)
    raise Exception("Invalid kind for @image " + type(obj))

def from_json(x):
    return DotDict({'x': json.loads(x)}).x

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

class TypeCastTransformationVisitor(ast.NodeTransformer):
    def __init__(self):
        self.uses = {
            'json': False,
            'query': False,
        }

    def start(self):
        self.uses = {
            'json': False,
            'query': False,
        }
        return self

    def visit_BinOp(self, node: ast.BinOp):
        if isinstance(node.op, ast.MatMult) and isinstance(node.right, ast.Name):
            self.generic_visit(node)
            # x @ty -> transform
            if node.right.id.lower() == 'json':
                self.uses['json'] = True
                return ast.copy_location(ast.Call(func=ast.Name('global_to_json'), args=[node.left], keywords=[]), node)
            elif node.right.id.lower() == 'image':
                self.uses['query'] = True
                return ast.copy_location(ast.Call(func=ast.Name('global_construct_image'), args=[node.left], keywords=[]), node)
            elif node.right.id.lower() == 'catch':
                return ast.copy_location(ast.Call(func=ast.Name('global_suppress_exceptions'), args=[
                    ast.copy_location(ast.Lambda(
                        args=ast.arguments(args=[], posonlyargs=[], varargs=None, kwonlyargs=None, kw_defaults=None, kwarg=None, defaults=[]),
                        body=node.left), node)
                ], keywords=[]), node)
        else:
            # not a name, visit it
            self.generic_visit(node)

        return node


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
        self.visitor = TypeCastTransformationVisitor()
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
            'lit.http/json',
            'identity',
        )
        self.metavarre = re.compile(r'(?!\\)\$([\w:]+)')
        self.server_thread = Thread(target=self.run_flask_server)
        self.server_port = int(environ.get('PORT', '8080')) + 1
        self.cached_images: dict[str, Image.Image] = {}

    def run_flask_server(self):
        app = Flask(__name__)

        @app.route('/image/<hash>')
        def get_image(hash):
            if hash not in self.cached_images:
                response = make_response('Nothing here mate')
                response.status_code = 404
                return response

            response = make_response(self.cached_images[hash].tobytes('jpeg'))
            response.content_type = 'image/jpg'
            return response

        @app.route('/thumb/<hash>')
        def get_image(hash):
            if hash not in self.cached_images:
                response = make_response('Nothing here mate')
                response.status_code = 404
                return response

            response = make_response(self.cached_images[hash].resize(size=(128, 128)).tobytes('jpeg'))
            response.content_type = 'image/jpg'
            return response

        app.run(host='0.0.0.0', port=self.server_port, load_dotenv=False)

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

    def define(self, iotype, name, _type, vname, body):
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
            xbody = self.gmetavarre(r).sub(f'({self.ios[iotype][r][2]})', xbody)

        if not xbody:
            xbody = vname
        print('> ', xbody)
        xbody = astor.to_source(self.visitor.start().visit(subv(xbody, iotype, name)))
        print('> ', xbody)
        body = f'lambda {vname}: {xbody}'
        compile(body, f'{iotype}:{name}', 'eval', dont_inherit=True)

        self.ios[iotype][name] = (vname, _type, body, self.visitor.uses)
        self.flush()

    def tgwrap(self, query, _type, stuff):
        def convert_to_result(uuid, k, x, rest):
            print(x)
            if isinstance(x, str):
                return InlineQueryResultArticle(
                    id=uuid,
                    title=f"result {k}",
                    input_message_content=InputTextMessageContent(x, parse_mode={'markdown': 'Markdown', 'html': 'HTML'}.get(_type)),
                    thumb_url=None if len(rest) == 0 else rest[0]
                )
            if isinstance(x, InternalPhoto):
                return InlineQueryResultPhoto(
                    id=uuid,
                    title=f"result {k}",
                    photo_url=x.url,
                    thumb_url=x.thumb_url,
                    caption=x.caption
                )
            if isinstance(x, Image.Image):
                hash = xxhash.xxh64(x.tobytes()).digest().hex()
                url = f"{environ.get('APP_URL')}:{self.server_port}/image/{hash}"
                thumb_url = f"{environ.get('APP_URL')}:{self.server_port}/thumb/{hash}"

                size = 0
                for key in self.cached_images:
                    size += len(self.cached_images[key].tobytes())
                if size > 32 * 1024 * 1024:
                    to_delete = size - 32 * 1024 * 1024
                    while to_delete > 0 and len(self.cached_images) != 0:
                        k = next(iter(self.cached_images))
                        to_delete -= len(self.cached_images[k].tobytes())
                        del self.cached_images[k]

                self.cached_images[hash] = x

                res = InlineQueryResultPhoto(
                    id=uuid,
                    title=f"result {k}",
                    photo_url=url,
                    thumb_url=thumb_url,
                    caption=k,
                )
                print(res)
                return res
            return InlineQueryResultArticle(
                id=uuid,
                title=f'{k} - Unknown result type',
                input_message_content=InputTextMessageContent(to_json(x))
            )


        return [ convert_to_result(uuid4(), k, x, rest) for k,x,*rest in stuff]

    def declare(self, name, comm_type, inp, out, path):
        if name in self.apis:
            raise Exception(f'duplicate API name {name}')

        for metavar in self.metavarre.finditer(path):
            if metavar.group(1) != 'result':
                raise Exception(f'Unknown meta variable `{metavar.group(1)}` (at offset {metavar.pos})')

        self.apis[name] = (comm_type, inp, out, path)
        self.flush()

    def adapter(self, name, adapter, value, env=None):
        vname, _type, body, *uses = adapter
        print(vname, _type, body)
        if env is None:
            env = {}
        if 'Image' not in env:
            env['Image'] = Image
        if 'global_suppress_exceptions' not in env:
            env.update({'global_suppress_exceptions': suppress_exceptions})
        if len(uses) > 0:
            uses = uses[0]
            if uses:
                if 'json' in uses and uses['json']:
                    env.update({'global_to_json': to_json})
                if 'query' in uses and uses['query']:
                    env.update({'global_construct_image': construct_image})

        env.update({'strip_tags': strip_tags})
        return (_type, eval(compile(body, name, 'eval', dont_inherit=True), env, {})(value))

    def invoke(self, api, query):
        comm_type, inp, out, path = self.apis[api]
        if inp not in self.input_adapters:
            raise Exception(f'Undefined input adapter {inp}')

        if out not in self.output_adapters:
            raise Exception(f'Undefined ouput adapter {out}')

        inpv = self.input_adapters[inp]

        _, q = self.adapter(inp, inpv, query)
        if comm_type == 'identity':
            return self.metavarre.sub(q, path)

        if comm_type == 'http/link':
            path = self.metavarre.sub(urllib.parse.quote_plus(q), path)
            return path

        if comm_type == 'json/post':
            path = self.metavarre.sub(q.get('pvalue', ''), path)
            res = requests.post(path, data=q.get('value', {}))
            return DotDict({'x':res.json()}).x

        if comm_type == 'http/json':
            path = self.metavarre.sub(urllib.parse.quote_plus(q), path)
            res = requests.get(path)
            return DotDict({'x': res.json()}).x

        if comm_type == 'lit.http/json':
            path = self.metavarre.sub(q, path)
            res = requests.get(path)
            return DotDict({'x': res.json()}).x

        if comm_type == 'html/xpath':
            path = self.metavarre.sub(urllib.parse.quote_plus(q), path)
            req = requests.get(path)
            if req.status_code != 200:
                raise Exception(f'{req.status_code}: {req.reason}')
            xml = xhtml.fromstring(req.content)
            return lambda x, xml=xml: xml.xpath(x)

        if comm_type == 'graphql':
            req = requests.post(path, json={'query': q, 'vars': {}}).json()
            return DotDict({'x': req}).x

        raise Exception(f'type {comm_type} not yet implemented')

    def render(self, api, value):
        comm_type, inp, out, path = self.apis[api]

        outv = self.output_adapters[out]
        _type, q = self.adapter(out, outv, value)
        return self.tgwrap(api, _type, q)
