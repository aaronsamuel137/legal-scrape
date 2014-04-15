from multiprocessing import Process
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
import redis
import pickle

from pymongo import MongoClient

NUM_PROCESSES = 4

# open an error log
log = open('error.log', 'w')

# set up database connection
client = MongoClient('localhost', 27017)
db = client.legal_db
collection = db.legal

# regex for getting the links to the actual data from the main link
link_re = re.compile("http://www\.legis\.state\.pa\.us//WU01/LI/LI/CT/HTM/[0-9]+/[0-9].*\'")

def parse_urls(red_serve):
    """
    The main parsing function. Takes a concurrent queue as an argument.

    """
    s = requests.Session()

    while int(red_serve.llen('urls')) > 0:
        item = red_serve.rpop('urls')
        item = pickle.loads(item)

        # keep dequing items until the queue is empty

        # print 'process {} parsing url {}'.format(os.getpid(), item['link'])
        # print 'queue size is', q.get_size()

        r = s.get(item['link'])

        # try to find link to data using regex
        matches = link_re.findall(r.text)
        if len(matches) > 0:
            link = matches[0].replace("'", '')
            r2 = s.get(link)
            soup = BeautifulSoup(r2.text)

            # try to find text surrounded by pre tag
            # this applies to some documents and not others
            pre = soup.find('pre')
            if pre is not None:
                data = pre.getText()
                item['data'] = data.strip()

            # otherwise, just get all the p tags
            else:
                try:
                    ps = soup.findAll('p')
                    text = ''
                    for p in ps:
                        text += (p.getText() + '\n')

                    item['data'] = text.strip()

                except Exception as e:
                    log.write('error occured: ' + str(e))
                    log.write('url is ' + str(link))

            try:
                collection.insert(item)
            except:
                log.write('error adding item to database')

        else:
            log.write('data link not found in page ' + item['link'])

def crawl(url, r_server):
    """
    Starts a spider crawling for all the useful urls in this domain and adding them
    to the shared queue. After the spider finishes, this processes starts parse the
    urls along with the other processes.

    """
    Spider().crawl(url, r_server)
    parse_urls(r_server)

def main(url):
    url = "http://www.legis.state.pa.us/cfdocs/legis/LI/Public/cons_index.cfm"

    # start the redis server
    r = redis.StrictRedis(host='localhost', port=6379, db=0)

    # start a spider crawling for urls
    p = Process(target=crawl, args=(url, r, ))
    p.start()

    # start other processes for parsing the urls
    processes = []
    for i in range(NUM_PROCESSES-1):
        processes.append(Process(target=parse_urls, args=(r, )))

    # wait until some items are in the queue before starting the parsing threads
    while int(r.llen('urls')) < 1:
        pass

    for i in range(NUM_PROCESSES-1):
        processes[i].start()

    for i in range(NUM_PROCESSES-1):
        processes[i].join()

    p.join()

main()
