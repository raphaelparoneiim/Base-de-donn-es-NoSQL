from pymongo import MongoClient
from datetime import datetime

MONGO_URI = "mongodb+srv://rparone2_db_user:gdH7lEKAwp4MT11y@cluster0.8gh72ko.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(MONGO_URI)

db = client.get_database("sample_mflix")
movies = db['movies']
comments = db['comments']
users = db['users']

print("----- EXO 1 : Top 10 films IMDb rating -----")
results = list(movies.find({}, {"_id":0, "title":1, "imdb.rating":1}).sort("imdb.rating",-1).limit(10))
for m in results:
    print(m["title"], "-", m["imdb"]["rating"])

print("\n----- EXO 2 : 5 films les plus récents -----")
results = list(movies.find({}, {"_id":0, "title":1, "year":1}).sort("year",-1).limit(5))
for m in results:
    print(m["title"], "-", m["year"])

print("\n----- EXO 3 : Film comédie avec le plus long runtime -----")
results = list(movies.find({"genres":"Comedy"}, {"_id":0, "title":1, "runtime":1}).sort("runtime",-1).limit(1))
for m in results:
    print(m["title"], "-", m["runtime"])

print("\n----- EXO 4 : Nombre total de films par genre -----")
pipeline = [
    {"$unwind":"$genres"},
    {"$group":{"_id":"$genres","count":{"$sum":1}}},
    {"$sort":{"count":-1}}
]
results = list(movies.aggregate(pipeline))
for r in results:
    print(r["_id"], "-", r["count"])

print("\n----- EXO 5 : Note moyenne IMDb par genre -----")
pipeline = [
    {"$unwind":"$genres"},
    {"$group":{"_id":"$genres","avgRating":{"$avg":"$imdb.rating"}}},
    {"$sort":{"avgRating":-1}}
]
results = list(movies.aggregate(pipeline))
for r in results:
    print(r["_id"], "-", round(r["avgRating"],2))

print("\n----- EXO 6 : Acteurs les plus fréquents -----")
pipeline = [
    {"$unwind":"$cast"},
    {"$group":{"_id":"$cast","count":{"$sum":1}}},
    {"$sort":{"count":-1}},
    {"$limit":10}
]
results = list(movies.aggregate(pipeline))
for r in results:
    print(r["_id"], "-", r["count"])

print("\n----- EXO 7 : Nombre de commentaires par film -----")
pipeline = [
    {"$group":{"_id":"$movie_id","numComments":{"$sum":1}}}
]
results = list(comments.aggregate(pipeline))
for r in results[:10]:  # affiche juste 10 pour lisibilité
    print(r)

print("\n----- EXO 8 : Film avec le plus grand nombre de votes IMDb -----")
results = list(
    movies.find(
        {"imdb.votes": {"$type": "int"}},  # ne prend que les entiers
        {"_id":0, "title":1, "imdb.votes":1}
    ).sort("imdb.votes", -1).limit(1)
)
for r in results:
    print(r)


print("\n----- EXO 9 : Films avec quelques commentaires (5 premiers films seulement) -----")
pipeline = [
    {"$limit": 5},  # on ne prend que 5 films pour la demo
    {"$lookup":{
        "from":"comments",
        "localField":"_id",
        "foreignField":"movie_id",
        "as":"comments_list"
    }},
    {"$project":{"title":1, "comments_list":1, "_id":0}}
]
results = list(movies.aggregate(pipeline))
for r in results:
    print(r["title"], "-", len(r["comments_list"]), "commentaires")

print("\n----- EXO 10 : Films avec au moins un commentaire après 2020 -----")
pipeline = [
    {"$match":{"date":{"$gt":datetime(2015,1,1)}}},
    {"$group":{"_id":"$movie_id"}},
    {"$lookup":{
        "from":"movies",
        "localField":"_id",
        "foreignField":"_id",
        "as":"movie_info"
    }},
    {"$unwind":"$movie_info"},
    {"$project":{"title":"$movie_info.title","_id":0}}
]
results = list(comments.aggregate(pipeline))
for r in results[:10]:
    print(r)

print("\n----- EXO 11 : Utilisateurs avec leurs films favoris -----")
pipeline = [
    {"$lookup":{
        "from":"movies",
        "localField":"favorites",
        "foreignField":"_id",
        "as":"favorite_movies"
    }},
    {"$project":{"name":1,"favorite_movies.title":1,"_id":0}}
]
results = list(users.aggregate(pipeline))
for r in results[:5]:  # affiche juste 5 pour lisibilité
    print(r)

print("\n----- EXO 12 : Nombre de commentaires par utilisateur -----")
pipeline = [
    {"$group":{"_id":"$email","numComments":{"$sum":1}}}
]
results = list(comments.aggregate(pipeline))
for r in results[:10]:
    print(r)
