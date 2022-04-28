# import pymongo
#
# client = pymongo.MongoClient("mongodb://localhost:27017/")
# db = client["twitter_db"]
# # print(db.tweets.find({ "full_text": { "$regex": "/(donation|contribution|contri)/i"}}))
# tweets = db.tweets.find({ "full_text": { "$regex": "/(donation|contribution|contri)/i"}})
# print(tweets)
# for tweet in tweets:
#     print(tweet)

def throw_if_mongodb_is_unavailable(host, port):
    import socket
    sock = None
    try:
        sock = socket.create_connection(
            (host, port),
            timeout=1) # one second
    except socket.error as err:
        raise EnvironmentError(
            "Can't connect to MongoDB at {host}:{port} because: {err}"
            .format(**locals()))
    finally:
        if sock is not None:
            sock.close()

# elsewhere...
HOST = 'localhost'
PORT = 27017
throw_if_mongodb_is_unavailable(HOST, PORT)
import pymongo
conn = pymongo.MongoClient(HOST, PORT)
print(conn.admin.command('ismaster')['ok']==1.0)
# etc.

 # db.tweets.find({ full_text: { $regex: /(donation|contribution|contri)/i } })