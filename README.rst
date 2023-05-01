mongodbm
==================================

Introduction
------------
mongodbm is a Python package to use a MongoDB server as a python dbm-style key/value database. It implements all of the MutableMapping methods in Python and is implemented in MongoDB using the GridFS spec to allow for values of any size. Keys must be strings (as the filename in GridFS) and values can be set as either bytes or file-like objects. Values returned by mongodbm are always file-like objects (specifically the GridOut object from pymongo).

This package was primarily made to be a fast and persistent key/value store that can have multiple readers and writers across multiple Python instances. MongoDB handles the race conditions so that I don't have to. The initial primary use-case is as a local cache for web apps where the source data are stored in remote S3 systems.

A MongoDB server must be running and accessible for mongodbm to work.

If someone is brave enough to try this out, take a look at the doc strings of the mongodbm.open function to find out what you need to do. It's pretty straightforward.

Installation
------------
Install via pip::

  pip install mongodbm

Or conda::

  conda install -c mullenkamp mongodbm


I'll probably put it on conda-forge once I feel like it's up to an appropriate standard...

TODO
-----
More documentation and I need to write a lot more tests for the functionality.
