from pymongo import MongoClient

MONGO_URI = "mongodb+srv://rparone2_db_user:gdH7lEKAwp4MT11y@cluster0.8gh72ko.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(MONGO_URI)

db = client.get_database("sample_mflix")
movies = db['movies']

print("----- EXO 1 : Films sortis en 1999 -----")
results = list(movies.find({"year":1999}, {"_id":0, "title":1, "year":1}))
for m in results:
    print(m["title"], "-", m["year"])

print("\n----- EXO 2 : Films dont le genre inclut 'Comedy' -----")
results = list(movies.find({"genres":"Comedy"}, {"_id":0, "title":1, "genres":1}))
for m in results:
    print(m["title"], "-", m["genres"])

print("\n----- EXO 3 : Film avec le titre exact 'The Matrix' -----")
result = movies.find_one({"title":"The Matrix"}, {"_id":0})
print(result)

print("\n----- EXO 4 : Films dont le runtime > 120 min -----")
results = list(movies.find({"runtime":{"$gt":120}}, {"_id":0, "title":1, "runtime":1}))
for m in results:
    print(m["title"], "-", m["runtime"])

print("\n----- EXO 5 : Afficher seulement title et year -----")
results = list(movies.find({}, {"_id":0, "title":1, "year":1}))
for m in results[:10]:  # limite affichage pour lisibilité
    print(m)

print("\n----- EXO 6 : Films avec imdb.rating > 8 -----")
results = list(movies.find({"imdb.rating":{"$gt":8}}, {"_id":0, "title":1, "imdb":1}))
for m in results[:10]:
    print(m["title"], "-", m["imdb"]["rating"])

print("\n----- EXO 7 : Films sortis entre 1990 et 2000 -----")
results = list(movies.find({"year":{"$gte":1990,"$lte":2000}}, {"_id":0, "title":1, "year":1}))
for m in results[:10]:
    print(m["title"], "-", m["year"])

print("\n----- EXO 8 : Films avec genres 'Action' et 'Sci-Fi' -----")
results = list(movies.find({"genres":{"$all":["Action","Sci-Fi"]}}, {"_id":0, "title":1, "genres":1}))
for m in results:
    print(m["title"], "-", m["genres"])

print("\n----- EXO 9 : Films où 'Tom Hanks' est dans le cast -----")
results = list(movies.find({"cast":"Tom Hanks"}, {"_id":0, "title":1, "cast":1}))
for m in results:
    print(m["title"], "-", m["cast"])

print("\n----- EXO 10 : Films dont le plot contient 'space' -----")
results = list(movies.find({"plot":{"$regex":"space","$options":"i"}}, {"_id":0, "title":1, "plot":1}))
for m in results[:10]:
    print(m["title"], "-", m["plot"])
