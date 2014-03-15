from multiprocessing import Process, Lock, Manager
from multiprocessing.managers import BaseManager
import os
import random
import json

NUM_THREADS = 4

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

def fill_queue(q):
    items = json.load(open('items.json'))
    for i in items:
        q.enq(i['link'])

def test_queue(q):
    print q.deq(), os.getpid()
    print q.deq(), os.getpid()
    print q.deq(), os.getpid()
    print q.deq(), os.getpid()

def test():
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
