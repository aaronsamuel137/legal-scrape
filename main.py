from multiprocessing import Process, Lock, Pool
from multiprocessing.managers import BaseManager
from concurrentqueue import ConcurrentQueue
from spider import Spider
from bs4 import BeautifulSoup

import os
import random
import json
import requests
import re
import time

from pymongo import MongoClient

NUM_THREADS = 3

# open an error log
log = open('error.log', 'w')

# set up database connection
client = MongoClient('localhost', 27017)
db = client.legal_db
collection = db.legal

# regex for getting the links to the actual data from the main link
link_re = re.compile("http://www\.legis\.state\.pa\.us//WU01/LI/LI/CT/HTM/[0-9]+/[0-9].*\'")

# set up a manager to allow our queue to live in shared memory
class QueueManager(BaseManager):
    pass

QueueManager.register('ConcurrentQueue', ConcurrentQueue)

def parse_urls(q):
    """
    The main parsing function. Takes a concurrent queue as an argument.

    """
    s = requests.Session()
    item = q.deq()

    # keep dequing items until the queue is empty
    while item != -1:
        print 'process {} parsing url {}'.format(os.getpid(), item['link'])
        print 'queue size is', q.get_size()

        r = s.get(item['link'])

        # try to find link to data using regex
        matches = link_re.findall(r.text)
        if len(matches) > 0:
            link = matches[0].replace("'", '')
            r2 = s.get(link)
            soup = BeautifulSoup(r2.text)

            # get the title of the html page
            try:
                page_title = soup.find('title')
                title = page_title.getText()
                item['page_title'] = title
            except:
                log.write('title not found on url: ' + link)

            # try to find text surrounded by pre tag
            # this applies to some documents and not others
            pre = soup.find('pre')
            if pre is not None:
                data = pre.getText()
                item['data'] = data

            # otherwise, just get all the p tags
            else:
                try:
                    ps = soup.findAll('p')
                    text = ''
                    for i, p in enumerate(ps):
                        if i == 0:
                            # the first p tag is usual a title or something,
                            # so store it seperately
                            line1 = ps[0].getText()
                        else:
                            text += (p.getText() + '\n')
                    item['data'] = {
                        'first_line': line1,
                        'text': text
                    }
                except Exception as e:
                    log.write('error occured: ' + str(e))
                    log.write('url is ' + str(link))

            try:
                collection.insert(item)
            except:
                log.write('error adding item to database')

        else:
            log.write('data link not found in page ' + item['link'])

        item = q.deq()

def crawl(url, q):
    print 'calling crawl'
    Spider().crawl(url, q)

def run_tests_with_object():
    url = "http://www.legis.state.pa.us/cfdocs/legis/LI/Public/cons_index.cfm"

    # set up out queue as a shared object
    manager = QueueManager()
    manager.start()
    q = manager.ConcurrentQueue()

    # start a spider crawling for urls
    p = Process(target=crawl, args=(url, q, ))
    p.start()

    # start other processes for parsing the urls
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

def main():
    run_tests_with_object()
    log.close()

main()
