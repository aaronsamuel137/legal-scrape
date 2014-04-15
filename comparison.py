from multiprocessing import Process
from collections import deque
from BeautifulSoup import BeautifulSoup
from pymongo import MongoClient

import requests
import re
import os

NUM_PROCESSES = 4

# open an error log
log = open('error.log', 'w')

# set up database connection
client = MongoClient('localhost', 27017)
db = client.legal_db
collection = db.legal

# regex for getting the links to the actual data from the main link
link_re = re.compile("http://www\.legis\.state\.pa\.us//WU01/LI/LI/CT/HTM/[0-9]+/[0-9].*\'")

domain = 'http://www.legis.state.pa.us'

def get_data_link(url, s):
    """
    Returns the link for the actual content for the page.

    """
    r = s.get(url)
    link = link_re.findall(r.text)[0].replace("'", '')
    return link

def get_urls(url):
    """
    Crawls the main url looking for sub-urls.

    """
    print 'Getting urls from main statutes page', url
    s = requests.Session()

    num_urls = 0
    r = requests.get(url)
    soup = BeautifulSoup(r.text)
    items = []

    trs = soup.findAll('tr')
    for tr in trs:
        tds = tr.findAll('td')
        if len(tds) == 6:
            title = tds[1].getText()
            link = tds[3].find('a')['href']
            item = {
                'main_page': title,
            }
            item['link'] = get_data_link(link, s)
            items.append(item)

    return items

def crawl_and_parse(items):
    """
    This method crawls the relevant links looking for urls to parse, and puts them
    in a queue. It then pops them urls of one by one, parses them into a standard format,
    and puts them in the database.

    """
    my_items = deque()
    s = requests.Session()

    for item in items:
        print 'process {} crawling {} for urls'.format(os.getpid(), item['main_page'])
        r = s.get(item['link'])
        soup = BeautifulSoup(r.text)
        main = soup.title.getText()
        urls = soup.findAll('a')
        chre = re.compile("(?<=chpt=)\d+")

        # crawl for urls and store in queue
        for url in urls:
            href = url['href']
            isChapt = chre.search(href)
            if isChapt == None:
                mySub = "NoChap"
            else:
                mySub = isChapt.group(0)
            if href.startswith('/'):
                link = domain + href
                my_items.append({
                    'main_page': main,
                    'sub-page': mySub,
                    'section': url.parent.parent.getText().lstrip(),
                    'link': link
                })

    print 'process {} beginning to parse {} found urls'.format(os.getpid(), len(my_items))
    # parse all the urls from the queue
    for i in range(len(my_items)):
        try:
            item = my_items.popleft()
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

        except Exception as err:
            log.write('error', err)

    print 'process {} finished'.format(os.getpid())

def main():
    url = "http://www.legis.state.pa.us/cfdocs/legis/LI/Public/cons_index.cfm"
    items = get_urls(url)

    print 'got {} links from the main page'.format(len(items))

    sub_sections = [[] for i in range(NUM_PROCESSES)]
    for i, item in enumerate(items):
        sub_sections[i % NUM_PROCESSES].append(item)

    processes = []
    for i in range(NUM_PROCESSES):
        processes.append(Process(target=crawl_and_parse, args=(sub_sections[i], )))

    for i in range(NUM_PROCESSES):
        processes[i].start()

    for i in range(NUM_PROCESSES):
        processes[i].join()

main()
