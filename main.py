from multiprocessing import Process, Lock, Pool
from multiprocessing.managers import BaseManager
from concurrentqueue import ConcurrentQueue

import os
import random
import json
import requests
import re

from pymongo import MongoClient

NUM_THREADS = 4

# set up database connection
client = MongoClient('localhost', 27017)
db = client.legal_db
collection = db.legal

class QueueManager(BaseManager):
    pass

QueueManager.register('ConcurrentQueue', ConcurrentQueue)

def parse(item):
    print 'process {} parsing url {}'.format(os.getpid(), item['link'])
    link_re = re.compile("http://www\.legis\.state\.pa\.us//WU01/LI/LI/CT/HTM/[0-9]+/[0-9].*\'")
    r = requests.get(item['link'])
    link = link_re.findall(r.text)[0].replace("'", '')
    r2 = requests.get(link)

    data = {'title': item['title'],
            'body': r2.text}
    collection.insert(data)

def fill_queue(q):
    items = json.load(open('items.json'))
    for i in items:
        q.enq(i)

def pares_urls(q):
    item = q.deq()
    while item != -1:
        parse(item)
        item = q.deq()

def print_results():
    for item in collection.find():
        print item['title']

    print '\nloaded {} items'.format(collection.count())
    collection.remove()

def run_tests_with_object():
    manager = QueueManager()
    manager.start()
    q = manager.ConcurrentQueue()

    fill_queue(q)

    processes = []

    for i in range(NUM_THREADS):
        processes.append(Process(target=pares_urls, args=(q, )))

    for i in range(NUM_THREADS):
        processes[i].start()

    for i in range(NUM_THREADS):
        processes[i].join()

    print_results()

def run_tests_with_pool():
    q = json.load(open('items.json'))
    pool = Pool(processes=NUM_THREADS)
    result = pool.map(parse, q)

    print_results()

def main():
    run_tests_with_object()
    # run_tests_with_pool()

main()
