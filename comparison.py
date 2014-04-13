from multiprocessing import Process
from collections import deque
from BeautifulSoup import BeautifulSoup
from pymongo import MongoClient

import requests
import re
import os

NUM_THREADS = 4

# open an error log
log = open('error.log', 'w')

# set up database connection
client = MongoClient('localhost', 27017)
db = client.legal_db
collection = db.legal

# regex for getting the links to the actual data from the main link
link_re = re.compile("http://www\.legis\.state\.pa\.us//WU01/LI/LI/CT/HTM/[0-9]+/[0-9].*\'")

domain = 'http://www.legis.state.pa.us'

ITEMS = [
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/00/00.HTM?10', 'main_page': u'CONSTITUTION OF PENNSYLVANIA'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/01/01.HTM?35', 'main_page': u'GENERAL PROVISIONS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/02/02.HTM?36', 'main_page': u'ADMINISTRATIVE LAW AND PROCEDURE'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/03/03.HTM?37', 'main_page': u'AGRICULTURE'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/04/04.HTM?38', 'main_page': u'AMUSEMENTS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/05/05.HTM?30', 'main_page': u'ATHLETICS AND SPORTS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/06/06.HTM?33', 'main_page': u'BAILEES AND FACTORS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/07/07.HTM?33', 'main_page': u'BANKS AND BANKING'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/08/08.HTM?26', 'main_page': u'BOROUGHS AND INCORPORATED TOWNS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/09/09.HTM?27', 'main_page': u'BURIAL GROUNDS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/10/10.HTM?27', 'main_page': u'CHARITIES'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/11/11.HTM?28', 'main_page': u'CITIES'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/12/12.HTM?22', 'main_page': u'COMMERCE AND TRADE'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/13/13.HTM?23', 'main_page': u'COMMERCIAL CODE'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/14/14.HTM?24', 'main_page': u'COMMUNITY AFFAIRS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/15/15.HTM?52', 'main_page': u'CORPORATIONS AND UNINCORPORATED ASSOCIATIONS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/16/16.HTM?55', 'main_page': u'COUNTIES'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/17/17.HTM?56', 'main_page': u'CREDIT UNIONS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/18/18.HTM?17', 'main_page': u'CRIMES AND OFFENSES'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/20/20.HTM?51', 'main_page': u'DECEDENTS, ESTATES AND FIDUCIARIES'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/22/22.HTM?52', 'main_page': u'DETECTIVES AND PRIVATE POLICE'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/23/23.HTM?43', 'main_page': u'DOMESTIC RELATIONS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/24/24.HTM?44', 'main_page': u'EDUCATION'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/25/25.HTM?47', 'main_page': u'ELECTIONS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/26/26.HTM?39', 'main_page': u'EMINENT DOMAIN'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/27/27.HTM?40', 'main_page': u'ENVIRONMENTAL RESOURCES'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/28/28.HTM?41', 'main_page': u'ESCHEATS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/29/29.HTM?25', 'main_page': u'FEDERAL RELATIONS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/30/30.HTM?26', 'main_page': u'FISH'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/31/31.HTM?27', 'main_page': u'FOOD'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/32/32.HTM?28', 'main_page': u'FORESTS, WATERS AND STATE PARKS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/33/33.HTM?20', 'main_page': u'FRAUDS, STATUTE OF'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/34/34.HTM?21', 'main_page': u'GAME'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/35/35.HTM?46', 'main_page': u'HEALTH AND SAFETY'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/36/36.HTM?16', 'main_page': u'HIGHWAYS AND BRIDGES'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/37/37.HTM?16', 'main_page': u'HISTORICAL AND MUSEUMS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/38/38.HTM?18', 'main_page': u'HOLIDAYS AND OBSERVANCES'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/39/39.HTM?19', 'main_page': u'INSOLVENCY AND ASSIGNMENTS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/40/40.HTM?10', 'main_page': u'INSURANCE'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/42/42.HTM?94', 'main_page': u'JUDICIARY AND JUDICIAL PROCEDURE'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/43/43.HTM?14', 'main_page': u'LABOR'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/44/44.HTM?43', 'main_page': u'LAW AND JUSTICE'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/45/45.HTM?44', 'main_page': u'LEGAL NOTICES'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/46/46.HTM?45', 'main_page': u'LEGISLATURE'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/47/47.HTM?46', 'main_page': u'LIQUOR'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/48/48.HTM?38', 'main_page': u'LODGING AND HOUSING'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/49/49.HTM?41', 'main_page': u"MECHANICS' LIENS"},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/50/50.HTM?42', 'main_page': u'MENTAL HEALTH'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/51/51.HTM?34', 'main_page': u'MILITARY AFFAIRS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/52/52.HTM?35', 'main_page': u'MINES AND MINING'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/53/53.HTM?5', 'main_page': u'MUNICIPALITIES GENERALLY'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/54/54.HTM?29', 'main_page': u'NAMES'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/57/57.HTM?30', 'main_page': u'NOTARIES PUBLIC'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/58/58.HTM?94', 'main_page': u'OIL AND GAS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/59/59.HTM?32', 'main_page': u'PARTNERSHIPS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/60/60.HTM?61', 'main_page': u'PEDDLERS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/61/61.HTM?64', 'main_page': u'PRISONS AND PAROLE'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/62/62.HTM?65', 'main_page': u'PROCUREMENT'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/63/63.HTM?56', 'main_page': u'PROFESSIONS AND OCCUPATIONS (STATE LICENSED)'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/64/64.HTM?4', 'main_page': u'PUBLIC AUTHORITIES AND QUASI-PUBLIC CORPORATIONS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/65/65.HTM?58', 'main_page': u'PUBLIC OFFICERS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/66/66.HTM?60', 'main_page': u'PUBLIC UTILITIES'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/67/67.HTM?53', 'main_page': u'PUBLIC WELFARE'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/68/68.HTM?54', 'main_page': u'REAL AND PERSONAL PROPERTY'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/69/69.HTM?55', 'main_page': u'SAVINGS ASSOCIATIONS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/70/70.HTM?81', 'main_page': u'SECURITIES'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/71/71.HTM?75', 'main_page': u'STATE GOVERNMENT'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/72/72.HTM?76', 'main_page': u'TAXATION AND FISCAL AFFAIRS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/73/73.HTM?77', 'main_page': u'TOWNSHIPS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/74/74.HTM?65', 'main_page': u'TRANSPORTATION'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/75/75.HTM?73', 'main_page': u'VEHICLES'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/76/76.HTM?51', 'main_page': u'WEIGHTS, MEASURES AND STANDARDS'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/77/77.HTM?52', 'main_page': u"WORKMEN'S COMPENSATION"},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/78/78.HTM?53', 'main_page': u'ZONING AND PLANNING'},
    {'link': u'http://www.legis.state.pa.us//WU01/LI/LI/CT/HTM/79/79.HTM?54', 'main_page': u'SUPPLEMENTARY PROVISIONS'}
]

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

    sub_sections = [[] for i in range(NUM_THREADS)]
    for i, item in enumerate(items):
        sub_sections[i % NUM_THREADS].append(item)

    processes = []
    for i in range(NUM_THREADS):
        processes.append(Process(target=crawl_and_parse, args=(sub_sections[i], )))

    for i in range(NUM_THREADS):
        processes[i].start()

    for i in range(NUM_THREADS):
        processes[i].join()

main()
