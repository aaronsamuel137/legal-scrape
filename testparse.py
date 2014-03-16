#!usr/bin/python

import sys
import re
import requests
from bs4 import BeautifulSoup

def parseLegal(aURL, mainKey):
    link_re = re.compile("http://www\.legis\.state\.pa\.us//WU01/LI/LI/CT/HTM/[0-9]+/[0-9].*\'")
    r = requests.get(aURL)
    link = link_re.findall(r.text)[0]
    r2 = requests.get(link)

    forParsing = BeautifulSoup(r2.content)
    p_tags = forParsing.find_all('p')
    theLaw = {}
    theLaw[mainKey] = {}

    table = re.compile('TABLE OF CONTENTS')

    startCount = 0
    encounterEnd = ""
    amIart = False
    thisArt = ""

    for ps in p_tags:
        for childs in ps.children:
            if startCount == 0:
                startCount = atBeg(childs.string)
                checkBg = findEnd(childs.string, table)
                if checkBG == 1:
                    encounterEnd = childs.string
                    startCount = 1
                else:
                    continue

            if not re.match(encounterEnd, childs.string)
                elif not amIart:
                    if re.match("ARTICLE",childs.string):
                        amIart = True
                        thisArt = childs.string
                        theLaw[mainKey][childs.string] = ""
                elif amIart:
                    if re.match("ARTICLE",childs.string):
                        theLaw[mainKey][thisArt][childs.string] = ""
                    else:
                        theLaw[mainKey][thisArt][childs.string] = ""

# def atBeg(testString):


            
def findEnd(testString, regTest):
    if not regTest.match(testString):
        return 1
    else: 
        return 0 




    # with open('text.txt', 'a+') as tst:
    #     for ps in p_tags:
    #         for childs in ps.children:
    #             tst.write(childs.string.encode('utf-8'))



if __name__ == '__main__':
    myURL = "http://www.legis.state.pa.us/cfdocs/legis/LI/consCheck.cfm?txtType=HTM&ttl=0"
    title = "Constitution of Pennsylvania"
    parseLegal(myURL, title)