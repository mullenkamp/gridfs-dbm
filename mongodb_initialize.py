# -*- coding: utf-8 -*-
"""
Created on Sat Nov 16 08:04:46 2019

@author: michaelek
"""
import os
import pandas as pd
import yaml
import json
from time import sleep
# from tethysts import Tethys
from pymongo import MongoClient, InsertOne, DeleteOne, ReplaceOne, UpdateOne, errors

pd.set_option('display.max_columns', 10)
pd.set_option('display.max_rows', 30)

#############################################
### Parameters

base_dir = os.path.realpath(os.path.dirname(__file__))

database = 'db'

schema_dir = 'schemas'

chunks_yml = 'chunks_schema.yml'
chunks_coll = 'fs.chunks'

chunks_index1 = [('files_id', 1), ('n', 1)]
chunks_index2 = [('uploadDate', 1)]

files_yml = 'files_schema.yml'
files_coll = 'fs.files'

files_index1 = [('filename', 1), ('uploadDate', 1)]
files_index2 = [('uploadDate', 1)]

try:
    param = os.environ.copy()
    db_service = param['db_service']
except:
    db_service = 'db'

ttl = 100

############################################
### Initialize the collections, set the schemas, and set the indexes

# client = MongoClient(db_service)
client = MongoClient('127.0.0.1')
# client = MongoClient('db', password=root_pass, username=root_user)
# client = MongoClient('127.0.0.1', password=root_pass, username=root_user)
# client = MongoClient('tethys-ts.duckdns.org', password=root_pass, username=root_user)

db = client[database]

print(db.list_collection_names())

## chunks collection
with open(os.path.join(base_dir, schema_dir, chunks_yml)) as yml:
    chunks1 = yaml.safe_load(yml)

try:
    db.create_collection(chunks_coll, validator={'$jsonSchema': chunks1})
except:
    db.command('collMod', chunks_coll, validator= {'$jsonSchema': chunks1})
    db[chunks_coll].drop_indexes()

db[chunks_coll].create_index(chunks_index1, unique=True)
db[chunks_coll].create_index(chunks_index2, expireAfterSeconds=ttl)


## files collection
with open(os.path.join(base_dir, schema_dir, chunks_yml)) as yml:
    files1 = yaml.safe_load(yml)

try:
    db.create_collection(files_coll, validator={'$jsonSchema': files1})
except:
    db.command('collMod', files_coll, validator= {'$jsonSchema': files1})
    db[files_coll].drop_indexes()

db[files_coll].create_index(files_index1)
db[files_coll].create_index(files_index2, expireAfterSeconds=ttl)

print(db.list_collection_names())
print('finished initialization')

client.close()
