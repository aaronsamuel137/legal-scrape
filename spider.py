import requests
import re
import os

from BeautifulSoup import BeautifulSoup
from concurrentqueue import ConcurrentQueue

domain = 'http://www.legis.state.pa.us'

class Spider():

    def crawl(self, url, q):
        print 'calling crawl with url', url
        r = requests.get(url)
        soup = BeautifulSoup(r.text)

        trs = soup.findAll('tr')
        for tr in trs:
            tds = tr.findAll('td')
            if len(tds) == 6:
                title = tds[1].getText()
                link = tds[3].find('a')['href']
                item = {
                    'main_page': title,
                }
                item['link'] = self.get_data_link(link)
                self.crawl_again(item, q)

    def get_data_link(self, url):
        link_re = re.compile("http://www\.legis\.state\.pa\.us//WU01/LI/LI/CT/HTM/[0-9]+/[0-9].*\'")
        # print 'getting data link from', url
        r = requests.get(url)
        link = link_re.findall(r.text)[0].replace("'", '')
        # print 'got data link', link
        return link

    def crawl_again(self, item, q):
        # print 'getting all links from page', item['link']
        r = requests.get(item['link'])
        soup = BeautifulSoup(r.text)
        urls = soup.findAll('a')
        for url in urls:
            href = url['href']
            if href.startswith('/'):
                link = domain + href
                q.enq({
                    'main_page': item['main_page'],
                    'link_text': url.getText(),
                    'link': link
                })
                print 'process {} adding url {} to queue'.format(os.getpid(), link)


# s = Spider()
# url = s.get_data_link('http://www.legis.state.pa.us/cfdocs/legis/LI/consCheck.cfm?txtType=HTM&ttl=0')
# s.crawl_again(url)