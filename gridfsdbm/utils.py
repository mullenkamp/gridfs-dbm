#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
import os
import io
from hashlib import blake2b, blake2s
from time import time
import gridfs

############################################
### Parameters

chunks_coll = 'fs.chunks'
chunks_index1 = [('files_id', 1), ('n', 1)]
chunks_index2 = [('uploadDate', 1)]

files_coll = 'fs.files'
files_index1 = [('filename', 1), ('uploadDate', 1)]
files_index2 = [('uploadDate', 1)]

############################################
### Functions


def drop_index(coll, index):
    """

    """
    try:
        coll.drop_index(index)
    except:
        pass


def set_indexes(db, ttl=None):
    """

    """
    db[chunks_coll].create_index(chunks_index1, unique=True)
    drop_index(db[chunks_coll], chunks_index2)
    if isinstance(ttl, int):
        db[chunks_coll].create_index(chunks_index2, expireAfterSeconds=ttl)

    db[files_coll].create_index(files_index1)
    drop_index(db[files_coll], files_index2)
    if isinstance(ttl, int):
        db[files_coll].create_index(files_index2, expireAfterSeconds=ttl)


def set_item(db, key, value):
    """

    """
    fs = gridfs.GridFSBucket(db)
    if isinstance(value, bytes):
        obid = fs.upload_from_stream(key, io.BytesIO(value))
    else:
        obid = fs.upload_from_stream(key, value)

    return obid


def update_chunks_date(db, objectids, ttl):
    """

    """
    if isinstance(ttl, int):
        if isinstance(objectids, list):
            db['fs.chunks'].update_many({'files_id': {'$in': objectids}}, {'$currentDate': {'uploadDate': True}})
        else:
            db['fs.chunks'].update_many({'files_id': objectids}, {'$currentDate': {'uploadDate': True}})












































































