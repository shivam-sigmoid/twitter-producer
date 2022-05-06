import datetime
import dateutil.parser


def get_pipeline_task_1():
    pipeline = [
        {"$match": {"location": {"$exists": "true"}}},
        {"$group": {"_id": {"Country": "$location"}, "Total_Tweets_Per_Country": {"$sum": 1}}},
        {"$project": {"_id.Country": 1, "Total_Tweets_Per_Country": 1}},
        {"$sort": {"Total_Tweets_Per_Country": -1}}
    ]
    return pipeline


def get_pipeline_task_2():
    pipeline = [
        {"$match": {"location": {"$exists": "true"}}},
        {"$group": {"_id": {"Country": "$location", "date": "$date"}, "tweets_per_day_per_Country": {"$sum": 1}}},
        {"$project": {"_id.date": 1, "_id.Country": 1, "tweets_per_day": 1, "tweets_per_day_per_Country": 1}},
        {"$sort": {"tweets_per_day_per_Country": -1}}
    ]
    return pipeline


def get_pipeline_task_2_date_wise(raw_date):
    date = dateutil.parser.parse(raw_date)
    pipeline = [
        {"$match": {"location": {"$exists": "true"}}},
        {"$match": {"date": datetime.datetime(date.year, date.month, date.day, 18, 30, 00)}},
        {"$group": {"_id": {"Country": "$location"}, "tweets_per_day_per_Country": {"$sum": 1}}},
        {"$sort": {"tweets_per_day_per_Country": -1}}
    ]
    return pipeline


def get_pipeline_task_7_week_wise(week_num):
    pipeline = [
        {"$match": {"week": week_num}},
        {"$group": {"_id": {"Country": "$country"}, "rank": {"$sum": "$rank"}}},
        {"$sort": {"rank": -1}}
    ]
    return pipeline


def get_pipeline_task_7():
    pipeline = [
        {"$group": {"_id": {"week": "$week", "Country": "$country"}, "rank": {"$sum": "$rank"}}},
        {"$sort": {"rank": -1}}
    ]
    return pipeline
