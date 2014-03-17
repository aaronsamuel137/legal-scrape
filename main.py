from multiprocessing import Process, Lock, Pool
from multiprocessing.managers import BaseManager
from concurrentqueue import ConcurrentQueue
from spider import Spider
from BeautifulSoup import BeautifulSoup

import os
import random
import json
import requests
import re
import time

from pymongo import MongoClient

NUM_THREADS = 4

# set up database connection
client = MongoClient('localhost', 27017)
db = client.legal_db
collection = db.legal

link_re = re.compile("http://www\.legis\.state\.pa\.us//WU01/LI/LI/CT/HTM/[0-9]+/[0-9].*\'")

class QueueManager(BaseManager):
    pass

QueueManager.register('ConcurrentQueue', ConcurrentQueue)

def parse(item):
    print 'process {} parsing url {}'.format(os.getpid(), item['link'])
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

def parse_urls(q):
    item = q.deq()
    while item != -1:
        print 'process {} parsing url {}'.format(os.getpid(), item['link'])
        r = requests.get(item['link'])
        link = link_re.findall(r.text)[0].replace("'", '')
        r2 = requests.get(link)
        soup = BeautifulSoup(r2.text)
        data = soup.find('pre').getText()
        item['data'] = data
        collection.insert(item)
        item = q.deq()

def print_results():
    for item in collection.find():
        print item['title']

    print '\nloaded {} items'.format(collection.count())
    collection.remove()

def crawl(url, q):
    print 'calling crawl'
    Spider().crawl(url, q)

def run_tests_with_object():
    url = "http://www.legis.state.pa.us/cfdocs/legis/LI/Public/cons_index.cfm"

    manager = QueueManager()
    manager.start()
    q = manager.ConcurrentQueue()

    # fill_queue(q)

    # start a spider crawling for urls
    p = Process(target=crawl, args=(url, q, ))
    p.start()

    processes = []

    for i in range(NUM_THREADS):
        processes.append(Process(target=parse_urls, args=(q, )))

    # wait until some items are in the queue before starting the parsing threads
    while q.get_size() < 1:
        pass

    for i in range(NUM_THREADS):
        processes[i].start()

    for i in range(NUM_THREADS):
        processes[i].join()

    p.join()

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
