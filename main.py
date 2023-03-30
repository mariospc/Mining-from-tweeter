import json

from pymongo import MongoClient
from neo4j import GraphDatabase

# URI = "neo4j+s://1f7e63a9.databases.neo4j.io"
# AUTH = ("neo4j", "HoBFPGcafmJZjt4QD5Cqsfo1_Ntmy6o3r5CwbktIiXI")
#
# driver = GraphDatabase.driver(URI, auth=("neo4j", "HoBFPGcafmJZjt4QD5Cqsfo1_Ntmy6o3r5CwbktIiXI"))

# Mongo Connection
client = MongoClient('mongodb://eu:ZFSXaX9E@db.csd.auth.gr:27117/eu?authSource=admin')
db = client.get_database('eu')
collection = db.get_collection("WDM1");
# allTweets = collection.find({"data.referenced_tweets": {"$exists": "false"}})
# usersResult = collection.find({}, {"includes.users.id": 1, "includes.users.name": 1})
# # distinct("_id", {}, {"includes.users.id": 1, "includes.users.name": 1})
#
# users = {}
# for element in usersResult:
#     for user in element['includes']['users']:
#         users[user["id"]] = user['name']

hashtags = []

hashtagResults = collection.find(
    {"$or": [{"includes.tweets": {"$exists": "true"}}, {"data.entities": {"$exists": "true"}}]},
    {"data.entities.hashtags.tag": 1, "includes.tweets.entities.hashtags.tag": 1})

for element in hashtagResults:
    if element['data'].get('entities') and element['data']['entities'].get('hashtags'):
        for hashtag in element['data']['entities']['hashtags']:
            hashtags.append(hashtag['tag'].lower())
    if element['includes'].get('tweets'):
        for tweet in element['includes']['tweets']:
            if (tweet.get('entities') and tweet['entities'].get('hashtags')):
                for has in tweet['entities']['hashtags']:
                    hashtags.append(has['tag'].lower())

hashtags = list(set(hashtags))
