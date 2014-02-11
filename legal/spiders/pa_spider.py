from scrapy.spider import Spider
from scrapy.selector import Selector

from legal.items import LegalItem

class PaSpider(Spider):
    name = "pa"
    allowed_domains = ["legis.state.pa.us"]
    start_urls = [
        "http://www.legis.state.pa.us/cfdocs/legis/LI/Public/cons_index.cfm"
    ]

    def parse(self, response):
        sel = Selector(response)
        trs = sel.xpath('//tr')
        items = []
        for tr in trs:
            tds = tr.xpath('./td')
            if len(tds) == 6:
                item = LegalItem()
                item['title'] = tds[1].xpath('text()').extract()
                item['link'] = tds[3].xpath('a/@href').extract()
                items.append(item)

        return items
