from multiprocessing import Process, Lock, Manager
from multiprocessing.managers import BaseManager

import os
import random
import json
import requests
import re

from pymongo import MongoClient

NUM_THREADS = 4

class ConcurrentQueue():
    def __init__(self):
        self.data = []
        self.lock = Lock()

    def deq(self):
        self.lock.acquire()
        try:
            item = self.data.pop(0)
        except IndexError:
            item = -1
        finally:
            self.lock.release()
        return item

    def enq(self, item):
        self.lock.acquire()
        try:
            self.data.append(item)
        finally:
            self.lock.release()

    def view(self):
        self.lock.acquire()
        try:
            print self.data
        finally:
            self.lock.release()

    def __str__(self):
        return self.data

class QueueManager(BaseManager):
    pass

QueueManager.register('ConcurrentQueue', ConcurrentQueue)

def parse(item, collection):
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

def pares_urls(q, collection):
    item = q.deq()
    while item != -1:
        print 'process {} parsing url {}'.format(os.getpid(), item['link'])
        parse(item, collection)
        item = q.deq()

def test():
    client = MongoClient('localhost', 27017)
    db = client.legal_db
    collection = db.legal

    manager = QueueManager()
    manager.start()
    q = manager.ConcurrentQueue()

    fill_queue(q)

    processes = []

    for i in range(NUM_THREADS):
        processes.append(Process(target=pares_urls, args=(q, collection)))

    for i in range(NUM_THREADS):
        processes[i].start()

    for i in range(NUM_THREADS):
        processes[i].join()

    for item in collection.find():
        print item['title']

test()
