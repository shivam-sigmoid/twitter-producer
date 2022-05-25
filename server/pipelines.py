import datetime
import dateutil.parser


class Pipelines:
    @staticmethod
    def get_pipeline_task_1():
        pipeline = [
            {"$match": {"location": {"$exists": "true"}}},
            {"$group": {"_id": {"Country": "$location"}, "Total_Tweets_Per_Country": {"$sum": 1}}},
            {"$project": {"_id.Country": 1, "Total_Tweets_Per_Country": 1}},
            {"$sort": {"Total_Tweets_Per_Country": -1}}
        ]
        return pipeline

    @staticmethod
    def get_pipeline_task_2():
        pipeline = [
            {"$match": {"location": {"$exists": "true"}}},
            {"$group": {"_id": {"Country": "$location", "date": "$date"}, "tweets_per_day_per_Country": {"$sum": 1}}},
            {"$project": {"_id.date": 1, "_id.Country": 1, "tweets_per_day": 1, "tweets_per_day_per_Country": 1}},
            {"$sort": {"tweets_per_day_per_Country": -1}}
        ]
        return pipeline

    @staticmethod
    def get_pipeline_task_2_date_wise(raw_date):
        date = dateutil.parser.parse(raw_date)
        pipeline = [
            {"$match": {"location": {"$exists": "true"}}},
            {"$match": {"date": datetime.datetime(date.year, date.month, date.day, 18, 30, 00)}},
            {"$group": {"_id": {"Country": "$location"}, "tweets_per_day_per_Country": {"$sum": 1}}},
            {"$sort": {"tweets_per_day_per_Country": -1}}
        ]
        return pipeline

    @staticmethod
    def get_pipeline_task_6_1():
        pipeline = [
            {"$group": {"_id": "$purpose", "CountOfDonations": {"$sum": 1}, "TotalAmountInDollars": {"$sum": "$amount"},
                        "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"TotalAmountInDollars": -1}},
            {"$limit": 10}
        ]
        return pipeline

    @staticmethod
    def get_pipeline_task_7_week_wise(week_num):
        pipeline = [
            {"$match": {"week": week_num}},
            {"$group": {"_id": {"Country": "$country"}, "rank": {"$avg": "$rank"}}},
            {"$sort": {"rank": -1}}
        ]
        return pipeline

    @staticmethod
    def get_pipeline_task_7():
        pipeline = [
            {"$group": {"_id": {"week": "$week", "Country": "$country"}, "rank": {"$avg": "$rank"}}},
            {"$sort": {"rank": -1}}
        ]
        return pipeline

    @staticmethod
    def get_pipeline_8_country(country):
        pipeline_yr = [
            {"$match": {"country": country}},
            {"$group": {"_id": {"year": "$year", "Country": "$country"}, "cpi": {"$avg": "$cpi"},
                        "gdp": {"$avg": "$gdp"},
                        "shares": {"$avg": "$share"}}},
            {"$sort": {"gdp": -1}}
        ]
        pipeline_q = [
            {"$match": {"country": country}},
            {"$group": {"_id": {"quarter": "$quarter", "year": "$year", "Country": "$country"}, "cpi": {"$avg": "$cpi"},
                        "gdp": {"$avg": "$gdp"},
                        "shares": {"$avg": "$share"}}},
            {"$sort": {"gdp": -1}}
        ]
        pipeline_mon = [
            {"$match": {"country": country}},
            {"$group": {"_id": {"month": "$month_name", "year": "$year", "Country": "$country"},
                        "cpi": {"$avg": "$cpi"},
                        "shares": {"$avg": "$share"}}},
            {"$sort": {"shares": -1}}
        ]
        return pipeline_yr, pipeline_q, pipeline_mon
