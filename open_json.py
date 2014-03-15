import json
import requests
import re


# link_re = re.compile("http://www\.legis\.state\.pa\.us//WU01/LI/LI/CT/HTM/[0-9]+/[0-9]\.HTM\?[0-9]+")
link_re = re.compile("http://www\.legis\.state\.pa\.us//WU01/LI/LI/CT/HTM/[0-9]+/[0-9].*\'")

# class shared(object):
#     def __init__(self):
#         pass
#     def add_urls(self, filename):
#         obj = json.load(open('items.json'))


# obj = json.load(open('items.json'))

# r = requests.get('http://www.legis.state.pa.us/cfdocs/legis/LI/consCheck.cfm?txtType=HTM&ttl=01')
r = requests.get('http://www.legis.state.pa.us/cfdocs/legis/LI/consCheck.cfm?txtType=HTM&ttl=01&div=0&chpt=17')
link = link_re.findall(r.text)[0].replace("'", '')
print link
r2 = requests.get(link)
print r2.text
