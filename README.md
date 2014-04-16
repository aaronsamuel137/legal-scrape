legal-scrape
============

For scraping legal data concurrently


Dependencies
------------

Requires the following python modules

- requests
- beautifulsoup 3
- pymongo
- redis

Installation with pip:

```
pip install pymongo beautifulsoup redis requests
```

Requires mongodb and redis. Installation with homebrew on OSX:

```
brew install mongo redis
```

Running
-------

Make sure the database servers are running on their default ports. Each implementation is in its own file. recitation1.py is the shared concurrent object implementation, recitation2.py is the implementation that doesn't rely on shared memory, and recitation3.py uses the redis server. Run these files with python 2.7.