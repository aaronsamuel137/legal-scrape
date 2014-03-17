import json
import requests
import re

from BeautifulSoup import BeautifulSoup

link_re = re.compile("http://www\.legis\.state\.pa\.us//WU01/LI/LI/CT/HTM/[0-9]+/[0-9].*\'")

r = requests.get('http://www.legis.state.pa.us/cfdocs/legis/LI/consCheck.cfm?txtType=HTM&ttl=0')
link = link_re.findall(r.text)[0].replace("'", '')
r2 = requests.get(link)

soup = BeautifulSoup(r2.text)
ps = soup.findAll('p')

for p in ps:
    print p.getText()
