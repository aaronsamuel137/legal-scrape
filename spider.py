import requests
import re
import os

from BeautifulSoup import BeautifulSoup
from concurrentqueue import ConcurrentQueue

domain = 'http://www.legis.state.pa.us'

class Spider():
    """
    This spider class is specific to the domain http://www.legis.state.pa.us.
    It looks for all links to all statutes and puts them in a queue, along with
    relevant meta-information.

    """

    def crawl(self, url, q):
        """
        Crawls the main url looking for sub-urls.

        """
        print 'calling crawl with url', url
        s = requests.Session()

        num_urls = 0
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
                item['link'] = self.get_data_link(link, s)
                num_urls += self.crawl_again(item, q, s)

        print 'total urls crawled:', num_urls

    def get_data_link(self, url, s):
        """
        Returns the link for the actual content for the page.

        """
        link_re = re.compile("http://www\.legis\.state\.pa\.us//WU01/LI/LI/CT/HTM/[0-9]+/[0-9].*\'")
        r = s.get(url)
        link = link_re.findall(r.text)[0].replace("'", '')
        return link

    def crawl_again(self, item, q, s):
        """
        Crawls the content page, looking for all urls in the same domain.

        """
        r = s.get(item['link'])
        soup = BeautifulSoup(r.text)
        main = soup.title.getText()
        urls = soup.findAll('a')
        chre = re.compile("(?<=chpt=)\d+")
        for url in urls:
            href = url['href']
            isChapt = chre.search(href)
            if isChapt == None:
                mySub = "NoChap"
            else:
                mySub = isChapt.group(0)
            if href.startswith('/'):
                link = domain + href
                q.enq({
                    'main_page': main,
                    'sub-page': mySub,
                    'section': url.parent.parent.getText().lstrip(),
                    'link': link
                })
        return len(urls)
