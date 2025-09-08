import ast
from os import environ

import astor
import re
import requests
import json
from telegram import (
    InlineQueryResultArticle,
    InputTextMessageContent,
    InlineQueryResultPhoto,
    InlineQueryResultGif,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from uuid import uuid4, UUID
import urllib.parse
from PIL import Image

from html.parser import HTMLParser
from lxml import html as xhtml
from flask import Flask, request, make_response
from threading import Thread
from type import checked_as

import xxhash
import base64
import io


VAR_STORE = {}


def subv(xbody, iotype, name):
    xast = ast.parse(xbody, f"{iotype}:{name}.body", "single").body[0]
    if isinstance(xast, ast.Assign):
        if (
            isinstance(xast.value, ast.Compare)
            and len(xast.value.ops) == 1
            and isinstance(xast.value.ops[0], ast.In)
        ):
            # x = y in z -> (lambda x: z)(y)
            rast = checked_as(
                ast.parse("x(y)", f"{iotype}:{name}.replacement_body", "eval").body,
                ast.Call,
            )
            targets = xast.targets[0]
            if isinstance(targets, ast.Tuple):
                targets = targets.elts
            else:
                targets = [targets]
            targets = [checked_as(t, ast.Name).id for t in targets]

            rast.func = ast.Lambda(
                ast.arguments(
                    posonlyargs=[],
                    args=[ast.arg(x, None) for x in targets],
                    vararg=None,
                    kwonlyargs=[],
                    kw_defaults=[],
                    kwarg=None,
                    defaults=[],
                ),
                (lambda x: x if len(targets) == 1 else ast.Starred(x))(
                    xast.value.comparators[0]
                ),
            )
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


def get_source_query(x):
    try:
        return getattr(x, "__source_query__")
    except AttributeError:
        return None

class NextStep:
    def __init__(self, obj, query):
        self.obj = obj
        self.query = query


def set_next_step(x, query):
    # query must be either a string, or a dict of "button text" -> query.
    if not isinstance(query, (str, dict)):
        raise Exception("nextStep argument must be either string or dict")
    if isinstance(query, dict):
        for k in query:
            if not isinstance(k, str) or not isinstance(query[k], str):
                raise Exception("nextStep dict must be string -> string")

    return NextStep(x, query)

class InternalPhoto:
    def __init__(self, url, thumb_url=None, caption=None):
        self.url = url
        self.caption = caption
        self.thumb_url = thumb_url or url


def construct_image(obj):
    if isinstance(obj, str):
        if obj.startswith("data:image/"):
            # Actually not a URL, but a base64-encoded image
            # Parse it and present as Image
            # Extract the base64 data from the data URL
            _, data = obj.split(',', 1)
            # Decode the base64 data
            image_data = base64.b64decode(data)
            # Create a PIL Image from the decoded data
            image = Image.open(io.BytesIO(image_data))
            return image
        return InternalPhoto(obj)
    if isinstance(obj, dict):
        return InternalPhoto(**obj)
    raise Exception("Invalid kind for @image " + str(type(obj)))


def get_var_store(x):
    global VAR_STORE
    if not isinstance(x, str):
        raise Exception("varStore argument must be string")
    if x not in VAR_STORE:
        return None
    return VAR_STORE[x]

def set_var_store(value):
    global VAR_STORE
    x = uuid4().hex
    VAR_STORE[x] = value
    return x

def from_json(x):
    return DotDict({"x": json.loads(x)}).x


class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.fed = []

    def handle_data(self, data):
        self.fed.append(data)

    def get_data(self):
        return "".join(self.fed)


def strip_tags(html):
    if not html:
        return "[nothing here]"
    s = MLStripper()
    s.feed(html)
    x = s.get_data()
    print("stripped:", x)
    return x.strip()


class TypeCastTransformationVisitor(ast.NodeTransformer):
    def __init__(self):
        self.uses = {
            "json": False,
            "query": False,
            "varstore": False,
        }

    def start(self):
        self.uses = {
            "json": False,
            "query": False,
            "varstore": False,
        }
        return self

    def visit_BinOp(self, node: ast.BinOp):
        if isinstance(node.op, ast.MatMult):
            if isinstance(node.right, ast.Name):
                self.generic_visit(node)
                # x @ty -> transform
                if node.right.id.lower() == "json":
                    self.uses["json"] = True
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("global_to_json"),
                            args=[node.left],
                            keywords=[],
                        ),
                        node,
                    )
                elif node.right.id.lower() == "image":
                    self.uses["query"] = True
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("global_construct_image"),
                            args=[node.left],
                            keywords=[],
                        ),
                        node,
                    )
                elif node.right.id.lower() == "varstore":
                    # uuid @ varStore -> value
                    self.uses["varstore"] = True
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("global_get_var_store"),
                            args=[node.left],
                            keywords=[],
                        ),
                        node,
                    )
                elif node.right.id.lower() == "freshvar":
                    # x @ freshvar -> (uuid)
                    self.uses["varstore"] = True
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("global_set_var_store"),
                            args=[node.left],
                            keywords=[],
                        ),
                        node,
                    )
                elif node.right.id.lower() == "query":
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("global_get_source_query"),
                            args=[node.left],
                            keywords=[],
                        ),
                        node,
                    )
                elif node.right.id.lower() == "catch":
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("global_suppress_exceptions"),
                            args=[
                                ast.copy_location(
                                    ast.Lambda(
                                        args=ast.arguments(
                                            args=[],
                                            posonlyargs=[],
                                            vararg=None,
                                            kwonlyargs=[],
                                            kw_defaults=[],
                                            kwarg=None,
                                            defaults=[],
                                        ),
                                        body=node.left,
                                    ),
                                    node,
                                )
                            ],
                            keywords=[],
                        ),
                        node,
                    )
            elif isinstance(node.right, ast.Call) and isinstance(
                node.right.func, ast.Name
            ):
                self.generic_visit(node)
                # x @ f(...) -> transform
                if node.right.func.id.lower() == "nextstep":
                    # x#nextStep(query) -> set_next_step(x, query)
                    if len(node.right.args) != 1:
                        raise Exception(
                            f"nextStep expects exactly one argument, got {len(node.right.args)}"
                        )
                    return ast.copy_location(
                        ast.Call(
                            func=ast.Name("set_next_step"),
                            args=[node.left, node.right.args[0]],
                            keywords=[],
                        ),
                        node,
                    )
            # not a type, visit it
            self.generic_visit(node)
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
            if hasattr(value, "keys"):
                value = DotDict(value)
            elif isinstance(value, list):
                value = [DotDict({"a": x}).a for x in value]
            self[key] = value


class APIHandler(object):
    def __init__(self, bot):
        self.visitor = TypeCastTransformationVisitor()
        self.bot = bot
        self.input_adapters: dict = {}
        self.output_adapters: dict = {}
        self.apis = None
        self.load()
        self.res = {}
        self.ios = {
            "input": self.input_adapters,
            "output": self.output_adapters,
        }
        self.comms = (
            "graphql",
            "json/post",
            "html/xpath",
            "http/link",
            "http/json",
            "lit.http/json",
            "identity",
        )
        self.metavarre = re.compile(r"(?!\\)\$([\w:]+)")
        self.page_re = re.compile(
            r"(?!\\)\#page\[(\w+)\]\((.*)\)"
        )  # #page[var](...#var...)
        self.page_var_re = lambda var: re.compile(rf"(?!\\)\#{var}\b")  # #var
        self.cached_images: dict[str, Image.Image] = {}

    def flush(self):
        self.bot.db.db.external_apis.update_one(
            {"kind": "api"}, {"$set": {"data": self.apis}}, upsert=True
        )
        self.bot.db.db.external_apis.update_one(
            {"kind": "input"}, {"$set": {"data": self.input_adapters}}, upsert=True
        )
        self.bot.db.db.external_apis.update_one(
            {"kind": "output"}, {"$set": {"data": self.output_adapters}}, upsert=True
        )

    def load(self):
        self.apis = (
            self.bot.db.db.external_apis.find_one({"kind": "api"}) or {"data": {}}
        )["data"]
        self.input_adapters = (
            self.bot.db.db.external_apis.find_one({"kind": "input"}) or {"data": {}}
        )["data"]
        self.output_adapters = (
            self.bot.db.db.external_apis.find_one({"kind": "output"}) or {"data": {}}
        )["data"]

    def gmetavarre(self, name):
        if name in self.res:
            return self.res[name]
        mre = re.compile(f"(?!\\\\)\\$({name})")
        self.res[name] = mre
        return mre

    def define(self, iotype, name, _type, vname, body):
        if name in self.ios[iotype]:
            raise Exception(f"duplicate {iotype} IO {name}")

        replacements = set()
        for metavar in self.metavarre.finditer(body):
            if metavar.group(1) != vname:
                if metavar.group(1) in self.ios[iotype]:
                    replacements.add(metavar.group(1))
                else:
                    raise Exception(
                        f"Unknown meta variable `{metavar.group(1)}` (at offset {metavar.pos})"
                    )
        xbody = self.gmetavarre(vname).sub(vname, body)

        for r in replacements:
            xbody = self.gmetavarre(r).sub(f"({self.ios[iotype][r][2]})", xbody)

        if not xbody:
            xbody = vname
        print("> ", xbody)
        xbody = astor.to_source(self.visitor.start().visit(subv(xbody, iotype, name)))
        print("> ", xbody)
        body = f"lambda {vname}: {xbody}"
        compile(body, f"{iotype}:{name}", "eval", dont_inherit=True)

        self.ios[iotype][name] = (vname, _type, body, self.visitor.uses)
        self.flush()

    def tgwrap(self, query, _type, stuff):
        def convert_to_result(uuid: UUID, k, x, rest):
            print(x)
            reply_markup = None
            if isinstance(k, NextStep):
                buttons = { "Next Step": k.query } if isinstance(k.query, str) else k.query
                reply_markup = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text=k, switch_inline_query_current_chat=v) for k, v in buttons.items()]
                    ]
                )
                k = k.obj
                print("next step:", reply_markup)

            if isinstance(x, str):
                return InlineQueryResultArticle(
                    id=str(uuid),
                    title=f"result {k}",
                    input_message_content=InputTextMessageContent(
                        x,
                        parse_mode={"markdown": "Markdown", "html": "HTML"}.get(_type),
                    ),
                    thumbnail_url=None if len(rest) == 0 else rest[0],
                    reply_markup=reply_markup,
                )
            if isinstance(x, InternalPhoto):
                return InlineQueryResultPhoto(
                    id=str(uuid),
                    title=f"result {k}",
                    photo_url=x.url,
                    thumbnail_url=x.thumb_url,
                    caption=x.caption,
                    reply_markup=reply_markup,
                )
            if isinstance(x, Image.Image):
                hash = xxhash.xxh64(x.tobytes()).digest().hex()
                url = f"{environ.get('APP_URL')}/image/{hash}"
                thumb_url = f"{environ.get('APP_URL')}/thumb/{hash}"

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
                    id=str(uuid),
                    title=f"result {k}",
                    photo_url=url,
                    thumbnail_url=thumb_url,
                    photo_height=x.height,
                    photo_width=x.width,
                    caption=k,
                    reply_markup=reply_markup,
                )
                print(res)
                return res
            return InlineQueryResultArticle(
                id=str(uuid),
                title=f"{k} - Unknown result type",
                input_message_content=InputTextMessageContent(to_json(x)),
                reply_markup=reply_markup,
            )

        items = []
        for v in stuff:
            if isinstance(v, NextStep):
                items.append((NextStep(v.obj[0], v.query), *v.obj[1:]))
            else:
                items.append(v)

        x = [convert_to_result(uuid4(), k, x, rest) for k, x, *rest in items]
        print("tgwrap", query, _type, stuff, "->")
        for xx in x:
            print("   ", xx.to_json())
        return x

    def declare(self, name, comm_type, inp, out, path):
        if name in self.apis:
            raise Exception(f"duplicate API name {name}")

        for metavar in self.metavarre.finditer(path):
            if metavar.group(1) != "result":
                raise Exception(
                    f"Unknown meta variable `{metavar.group(1)}` (at offset {metavar.pos})"
                )

        self.apis[name] = (comm_type, inp, out, path)
        self.flush()

    def adapter(self, name, adapter, value, env=None):
        vname, _type, body, *uses = adapter
        print(vname, _type, body)
        if env is None:
            env = {}
        if "Image" not in env:
            env["Image"] = Image
        if "global_suppress_exceptions" not in env:
            env.update({"global_suppress_exceptions": suppress_exceptions})
        if "global_get_source_query" not in env:
            env.update({"global_get_source_query": get_source_query})
        if "set_next_step" not in env:
            env.update({"set_next_step": set_next_step})
        if len(uses) > 0:
            uses = uses[0]
            if uses:
                if "json" in uses and uses["json"]:
                    env.update({"global_to_json": to_json})
                if "query" in uses and uses["query"]:
                    env.update({"global_construct_image": construct_image})
                if "varstore" in uses and uses["varstore"]:
                    env.update({"global_get_var_store": get_var_store, "global_set_var_store": set_var_store})

        env.update({"strip_tags": strip_tags})
        return (
            _type,
            eval(compile(body, name, "eval", dont_inherit=True), env, {})(value),
        )

    def invoke(self, api, query, extra):
        comm_type, inp, out, path = self.apis[api]
        if inp not in self.input_adapters:
            raise Exception(f"Undefined input adapter {inp}")

        if out not in self.output_adapters:
            raise Exception(f"Undefined ouput adapter {out}")

        inpv = self.input_adapters[inp]

        _, q = self.adapter(inp, inpv, query, env=extra)
        if isinstance(q, NextStep):
            # Input cannot have nextStep, remove it
            q = q.obj

        def res(path=path):
            if "page" in extra:
                path = self.page_re.sub(
                    (
                        lambda match: self.page_var_re(match.group(1)).sub(
                            str(extra["page"]), match.group(2)
                        )
                    ),
                    path,
                )
            else:
                path = self.page_re.sub("", path)

            if comm_type == "identity":
                return self.metavarre.sub(q, path)

            if comm_type == "http/link":
                path = self.metavarre.sub(urllib.parse.quote_plus(q), path)
                return path

            if comm_type == "json/post":
                path = self.metavarre.sub(q.get("pvalue", ""), path)
                body = json.dumps(q.get("value", {}))
                print("posting to", path, "data", body)
                res = requests.post(path, data=body, headers={"Content-Type": "application/json"})
                return DotDict({"x": res.json()}).x

            if comm_type == "http/json":
                path = self.metavarre.sub(urllib.parse.quote_plus(q), path)
                res = requests.get(path)
                return DotDict({"x": res.json()}).x

            if comm_type == "lit.http/json":
                path = self.metavarre.sub(q, path)
                res = requests.get(path)
                return DotDict({"x": res.json()}).x

            if comm_type == "html/xpath":
                path = self.metavarre.sub(urllib.parse.quote_plus(q), path)
                req = requests.get(path)
                if req.status_code != 200:
                    raise Exception(f"{req.status_code}: {req.reason}")
                xml = xhtml.fromstring(req.content)
                return lambda x, xml=xml: xml.xpath(x)

            if comm_type == "graphql":
                req = requests.post(path, json={"query": q, "vars": {}}).json()
                return DotDict({"x": req}).x

            raise Exception(f"type {comm_type} not yet implemented")

        r = res()
        print("result:", r)
        setattr(r, "__source_query__", q)
        return r

    def render(self, api, value, extra):
        comm_type, inp, out, path = self.apis[api]

        outv = self.output_adapters[out]
        _type, q = self.adapter(out, outv, value, env=extra)
        return self.tgwrap(api, _type, q)
