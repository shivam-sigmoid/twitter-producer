import pymongo


class Database:
    @staticmethod
    def create_db_connection(collection):
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["twitter_db"]
        col = db[collection]
        return col

