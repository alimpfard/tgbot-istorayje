import re
import requests
import json 
from telegram import (
    InlineQueryResultArticle, ParseMode, InputTextMessageContent
)
from uuid import uuid4
import urllib

class DotDict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __init__(self, dct):
        for key, value in dct.items():
            if hasattr(value, 'keys'):
                value = DotDict(value)
            self[key] = value

class APIHandler(object):
    def __init__(self, bot):
        self.bot = bot
        self.input_adapters = None
        self.output_adapters = None
        self.apis = None
        self.load()
        self.ios = {
            'input': self.input_adapters,
            'output': self.output_adapters,
        }
        self.comms = (
            'graphql',
            'json',
            'html/xpath',
            'html/link',
        )
        self.metavarre = re.compile(r'(?!\\)\$(\w+)')

    def flush(self):
        self.bot.db.db.external_apis.update_one({'kind': 'api'}, {'$set': {'data': self.apis}}, upsert=True)
        self.bot.db.db.external_apis.update_one({'kind': 'input'}, {'$set': {'data': self.input_adapters}}, upsert=True)
        self.bot.db.db.external_apis.update_one({'kind': 'output'}, {'$set': {'data': self.output_adapters}}, upsert=True)

    def load(self):
        self.apis = (self.bot.db.db.external_apis.find_one({'kind': 'api'}) or {'data': {}})['data']
        self.input_adapters = (self.bot.db.db.external_apis.find_one({'kind': 'input'}) or {'data': {}})['data']
        self.output_adapters = (self.bot.db.db.external_apis.find_one({'kind': 'output'}) or {'data': {}})['data']

    def define(self, iotype, name, vname, body):
        if name in self.ios[iotype]:
            raise Exception(f'duplicate {iotype} IO {name}')

        for metavar in self.metavarre.finditer(body):
            if metavar.group(1) != vname:
                raise Exception(f'Unknown meta variable `{matavar.group(1)}` (at offset {metavar.pos})')
        
        self.ios[iotype][name] = (vname, f'lambda {vname}: {self.metavarre.sub(vname, body)}')
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
                raise Exception(f'Unknown meta variable `{matavar.group(1)}` (at offset {metavar.pos})')

        self.apis[name] = (comm_type, inp, out, path)
        self.flush()

    def adapter(self, name, adapter, value):
        vname, body = adapter
        return eval(compile(body, name, 'eval', dont_inherit=True), {}, {})(value)

    def invoke(self, api, query):
        comm_type, inp, out, path = self.apis[api]
        if inp not in self.input_adapters:
            raise Exception(f'Undefined input adapter {inp}')
        
        if out not in self.output_adapters:
            raise Exception(f'Undefined ouput adapter {out}')

        inpv = self.input_adapters[inp]

        q = self.adapter(inp, inpv, query)
        if comm_type == 'html/link':
            path = self.metavarre.sub(urllib.parse.quote_plus(q), path)
            return path
        else:
            raise Exception(f'type {comm_type} not yet implemented')
    
    def render(self, api, value):
        comm_type, inp, out, path = self.apis[api]
        
        outv = self.output_adapters[out]
        q = self.adapter(out, outv, value)
        return self.tgwrap(api, q)