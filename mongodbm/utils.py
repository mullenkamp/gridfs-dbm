#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
import io
import gridfs

############################################
### Parameters

chunks_coll = 'fs.chunks'
chunks_index1 = [('files_id', 1), ('n', 1)]
chunks_index2 = [('uploadDate', 1)]

files_coll = 'fs.files'
files_index1 = [('filename', 1), ('uploadDate', 1)]
files_index2 = [('uploadDate', 1)]

ttl_index_name = 'uploadDate_1'

############################################
### Functions


def drop_index(coll, index):
    """

    """
    try:
        coll.drop_index(index)
    except:
        pass


def set_indexes(db, ttl: int=None):
    """

    """
    ## Base indexes required by GridFS
    db[chunks_coll].create_index(chunks_index1, unique=True)
    db[files_coll].create_index(files_index1)

    ## Check and assign ttl indexes
    index1 = db[files_coll].index_information()

    if isinstance(ttl, int):
        if ttl_index_name in index1:
            old_ttl = index1[ttl_index_name]['expireAfterSeconds']
            if old_ttl != ttl:
                drop_index(db[chunks_coll], chunks_index2)
                drop_index(db[files_coll], files_index2)
                db[chunks_coll].create_index(chunks_index2, expireAfterSeconds=ttl)
                db[files_coll].create_index(files_index2, expireAfterSeconds=ttl)
        else:
            db[chunks_coll].create_index(chunks_index2, expireAfterSeconds=ttl)
            db[files_coll].create_index(files_index2, expireAfterSeconds=ttl)
    else:
        if ttl_index_name in index1:
            drop_index(db[chunks_coll], chunks_index2)
            drop_index(db[files_coll], files_index2)


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












































































