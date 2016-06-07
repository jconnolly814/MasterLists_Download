__author__ = 'JConno02'

from bs4 import BeautifulSoup
import requests
import sys
import os


import pandas as pd

import csv


date= 20160606
groups =['Cetaceans','Pinnipeds','Sea Turtles','Other Marine Reptiles','Corals','Abalone' ]
outpath= r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\TESSQueries\20160606'
url = "http://www.nmfs.noaa.gov/pr/species/esa/listed.htm"

def makelist(table): #http://stackoverflow.com/questions/2870667/how-to-convert-an-html-table-to-an-array-in-python
  result = []
  allrows = table.findAll('tr')
  for row in allrows:
    result.append([])
    allcols = row.findAll('td')
    for col in allcols:
      thestrings = [unicode(s) for s in col.findAll(text=True)]
      thetext = ''.join(thestrings)
      removeline = thetext.replace('\n',' ')
      removeline = removeline .replace(',',' ')
      finalline= removeline.decode('utf8', 'replace').encode('ascii', 'replace')
      finalline=finalline.replace('?',' ')
      #print removeline
      result[-1].append(finalline)
  return result

def create_outtable(outInfo, csvname, header):
    ## CHANGE added a function to export to the dictionaries and lists to csv to QA the intermediate steps and to have copies of the final tables
    if type(outInfo) is dict:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, lineterminator='\n')
            writer.writerow(header)
            for k, v in outInfo.items():
                val = []
                val.append(k)
                val.append(outInfo[k])
                writer.writerow(val)
    elif type(outInfo) is list:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, delimiter ="\n",quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
            for val in outInfo:
                writer.writerow([val])

r = requests.get(url)
reload(sys)
sys.setdefaultencoding('utf8')

soup = BeautifulSoup(r.content, 'html.parser')

tables = soup.find_all('table')
counter =0
final =[]
for table in tables:
    #print table.prettify()

    outtable =makelist(table)

    header = ['species']
    create_outtable(outtable,(outpath+ os.sep + 'NMFS_'+str(counter)+'_' + str(date) + '.csv'),header)
    counter+=1
    inter =[]

    for v in outtable:

        pop =False
        try:
            firstcol = v[0].decode('utf8', 'replace').encode('ascii', 'replace')
            breaklist = firstcol.split('(')

            if len(breaklist ) ==1:
                if " ".join(str(v[0]).split()) in groups:
                    group = " ".join(str(v[0]).split())
                    print group
                    continue
            elif len(breaklist) >= 2:
                pop= True
                commonname= breaklist[0]
                print commonname
                sciname =  breaklist[(len(breaklist)-1)].replace(')','')
        except:
            pass
        current_species =[]

        try:
            current_species.append(commonname)
            current_species.append(sciname)
            current_species.append(group)
            for i in v:
                i = i.decode('utf8', 'replace').encode('ascii', 'replace')
                i= i.replace('?',' ')
                i= " ".join(i.split())
                current_species.append(i)
            inter.append(current_species)
        except:
            pass
    print inter
    for i in inter:
        print i
        sp=[]
        for j in i:
            print j
            sp.append(j)
        print sp
        final.append(sp)
print final
create_outtable(final,(outpath+ os.sep + 'NMFSb_'+str(counter)+'_' + str(date) + '.csv'),header)


##Fix ring seal- so that the pop is correct
### read in csv take the max values try to complete one row per pop
