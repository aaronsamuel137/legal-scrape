from multiprocessing import Lock

class ConcurrentQueue():
    def __init__(self):
        self.data = []
        self.lock = Lock()
        self.size = 0

    def deq(self):
        self.lock.acquire()
        try:
            item = self.data.pop(0)
            self.size -= 1
        except IndexError:
            item = -1
        finally:
            self.lock.release()
        return item

    def enq(self, item):
        self.lock.acquire()
        try:
            self.data.append(item)
            self.size += 1
        finally:
            self.lock.release()

    def view(self):
        self.lock.acquire()
        try:
            print self.data
        finally:
            self.lock.release()

    def get_size(self):
        return self.size

    def __str__(self):
        return self.data
