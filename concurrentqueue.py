from multiprocessing import Process, Lock, Manager
from multiprocessing.managers import BaseManager

import os
import random
import json
import requests
import re

# from pymongo import MongoClient

class ConcurrentQueue():
    def __init__(self):
        self.data = []
        self.lock = Lock()

    def deq(self):
        self.lock.acquire()
        try:
            item = self.data.pop(0)
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

def parse(url):
    link_re = re.compile("http://www\.legis\.state\.pa\.us//WU01/LI/LI/CT/HTM/[0-9]+/[0-9].*\'")
    r = requests.get(url)
    link = link_re.findall(r.text)[0].replace("'", '')
    print link
    r2 = requests.get(link)
    print r2.text

def fill_queue(q):
    items = json.load(open('items.json'))
    for i in items:
        q.enq(i['link'])

def test_queue(q):
    url = q.deq()
    parse(url)


def test():
    # client = MongoClient('localhost', 27017)
    # db = client.legal_db
    # collection = db.legal

    manager = QueueManager()
    manager.start()
    q = manager.ConcurrentQueue()

    fill_queue(q)

    p1 = Process(target=test_queue, args=(q,))
    p2 = Process(target=test_queue, args=(q,))

    p1.start()
    p2.start()

    print p1.pid
    print p2.pid

    p1.join()
    p2.join()

test()
