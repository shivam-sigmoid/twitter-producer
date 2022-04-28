import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["twitter_db"]
# print(db.tweets.find({ "full_text": { "$regex": "/(donation|contribution|contri)/i"}}))
tweets = db.tweets.find({ "full_text": { "$regex": "/(donation|contribution|contri)/i"}})
print(tweets)
for tweet in tweets:
    print(tweet)


 # db.tweets.find({ full_text: { $regex: /(donation|contribution|contri)/i } })