import psycopg2
from pymongo import MongoClient
from decimal import Decimal
from bson.decimal128 import Decimal128
from bson.objectid import ObjectId
import datetime
import time
import json
import uuid

client = MongoClient("mongodb://localhost:27017")
db=client['BigData'] 
col=db['Uber']
mdbcur=col.find()
with psycopg2.connect(host="localhost",
    database="Uber",
    user="postgres", 
    password="12345",
    port='5432') as pgconn:
    pgcur=pgconn.cursor()
   
    for doc in mdbcur:
        data=doc
        if 'id' in data.keys():
            id = data["id"]
        else:
            id = str(uuid.uuid4())

        if 'datetime' in data.keys():
            dateTime = data["datetime"]
        else:
            dateTime = datetime.datetime.now()

        if 'price' in data.keys():
            price = data["price"]
        else:
            price = 0

        if 'distance' in data.keys():
            distance = data["distance"]
        else:
            distance = 0

        if 'latitude' in data.keys():
            latitude = data["latitude"]
        else:
            latitude = 0

        if 'longitude' in data.keys():
            longitude = data["longitude"]
        else:
            longitude = 0

        if 'source' in data.keys():
            source = data["source"]
        else:
            source = ""

        if 'destination' in data.keys():
            destination = data["destination"]
        else:
            destination = ""
        
        sqlQuery = """INSERT INTO UBER(id, datetime, price, distance,latitude,longitude,source,destination)
        VALUES (%s, %s, %s, %s,%s, %s, %s, %s)"""
        record = (id,dateTime,price,distance,latitude,longitude,source,destination)
        pgcur.execute(sqlQuery,record)
        pgconn.commit()
        #time.sleep(1)
    pgcur.close()
    mdbcur.close()