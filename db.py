from pymongo import MongoClient

class DB:
    def __init__(self, addr='127.0.0.1', port=27017, name='istorayje'):
        self.client = MongoClient(addr, port)
        self.db_name = name
        self.db = self.client[name]
    