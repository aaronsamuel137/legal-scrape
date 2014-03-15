from multiprocessing import Process, Lock
import os
import random

NUM_THREADS = 4

class ConcurrentQueue():
    def __init__(self):
        self.data = []
        self.lock = Lock()

    def deq(self):
        self.lock.acquire()
        item = self.data.pop(0)
        self.lock.release()
        return item

    def enq(self, item):
        self.lock.acquire()
        self.data.append(item)
        self.lock.release()

def test_queue(q):
    q.enq(5)
    q.enq(7)
    q.enq(9)
    print q.deq()
    print q.data

def test():
    q = ConcurrentQueue()

    p1 = Process(target=test_queue, args=(q,))
    p2 = Process(target=test_queue, args=(q,))

    p1.start()
    p2.start()

    print p1.pid
    print p2.pid

    p1.join()
    p2.join()

    print q.data


test()
