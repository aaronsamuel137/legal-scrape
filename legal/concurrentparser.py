from multiprocessing import Pool 
import sys
import re
import requests
from bs4 import BeautifulSoup

def findAndParse(newURL):
	link_re = re.compile("http://www\.legis\.state\.pa\.us//WU01/LI/LI/CT/HTM/[0-9]+/[0-9].*\'")
	r = requests.get(url)
    link = link_re.findall(r.text)[0]
    r2 = requests.get(link)

    forParsing = BeautifulSoup(r2.content)



if __name__ == '__main__':
	numP = sys.argv[1]
	pool = Pool(processes=numP)
	result = apply_async(findAndParse)
