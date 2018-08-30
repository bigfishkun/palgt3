from database import *
import pymongo

collection = DB_CONN["basic"]
doc = collection.find_one({"code":"600864"},{"timeToMarket":1,"_id":0})
print(doc)





