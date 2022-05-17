import pymongo
import sys

sys.path.append("../")
from data.API_Links import Mongo_Base_URL


class Database:
    @staticmethod
    def create_db_connection(collection):
        client = pymongo.MongoClient(Mongo_Base_URL)
        db = client["twitter_db"]
        col = db[collection]
        return col
