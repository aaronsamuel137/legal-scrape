from multiprocessing import Lock
from collections import deque

class ConcurrentQueue():
    def __init__(self):
        self.data = deque()
        self.lock = Lock()

    def deq(self):
        self.lock.acquire()
        try:
            item = self.data.popleft()
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

    def get_size(self):
        return len(self.data)

    def __str__(self):
        return self.data
