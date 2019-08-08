from pymongo import MongoClient

class DB:
    def __init__(self, url='mongodb://127.0.0.1', name='istorayje'):
        self.client = MongoClient(url)
        self.db_name = name
        self.db = self.client[name]
    