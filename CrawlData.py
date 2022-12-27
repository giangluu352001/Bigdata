import json
import sys
import pandas as pd
import pymongo


def import_content():
    mng_client = pymongo.MongoClient('mongodb://localhost:27017')
    mng_db = mng_client['BigData'] 
    collection_name = 'Uber' 
    db_cm = mng_db[collection_name]

    data = pd.read_csv("UberDataSet.csv")
    data_json = json.loads(data.to_json(orient='records'))
    db_cm.insert_many(data_json)


import_content()