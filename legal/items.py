from scrapy.item import Item, Field

class LegalItem(Item):
    title = Field()
    link = Field()
    desc = Field()
