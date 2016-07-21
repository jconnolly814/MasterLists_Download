__author__ = 'JConno02'

import os
import unicodedata
import csv

from bs4 import BeautifulSoup
import requests
import pandas as pd

date = 20160606
groups = ['Cetaceans', 'Pinnipeds', 'Sea Turtles', 'Other Marine Reptiles', 'Corals', 'Abalone']
outpath = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\TESSQueries\20160606\NMFS'
url = "http://www.nmfs.noaa.gov/pr/species/esa/listed.htm"


def makelist(table):  # http://stackoverflow.com/questions/2870667/how-to-convert-an-html-table-to-an-array-in-python
    result = []
    allrows = table.findAll('tr')
    for row in allrows:
        result.append([])
        allcols = row.findAll('td')
        for col in allcols:
            thestrings = [unicode(s) for s in col.findAll(text=True)]
            thetext = ''.join(thestrings)
            removeline = thetext.replace('\n', ' ')
            removeline = removeline.replace(',', ' ')
            finalline = unicodedata.normalize("NFKD", removeline)
            finalline = finalline.replace('?', ' ')
            # print removeline
            result[-1].append(finalline)
    return result


def create_outtable(outInfo, csvname, header):
    ## CHANGE added a function to export to the dictionaries and lists to csv to QA the intermediate steps and to have copies of the final tables
    if type(outInfo) is dict:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, lineterminator='\n')
            for k, v in outInfo.items():
                val = []
                val.append(k)
                val.append(outInfo[k])
                val.append(header)
                writer.writerow(val)
    elif type(outInfo) is list:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, delimiter="\n", quoting=csv.QUOTE_MINIMAL)
            for val in outInfo:
                val.append(header)
                writer.writerow([val])


def create_Finaltable(outInfo, csvname):
    ## CHANGE added a function to export to the dictionaries and lists to csv to QA the intermediate steps and to have copies of the final tables
    if type(outInfo) is dict:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, lineterminator='\n')
            for k, v in outInfo.items():
                val = []
                val.append(k)
                val.append(outInfo[k])

                writer.writerow(val)
    elif type(outInfo) is list:
        with open(csvname, "wb") as output:
            writer = csv.writer(output, delimiter="\n", quoting=csv.QUOTE_MINIMAL)
            for val in outInfo:
                writer.writerow([val])



r = requests.get(url)

soup = BeautifulSoup(r.content, 'html.parser')
# print soup.prettify()

h2 = soup.find_all('h2')
title = []
for v in h2:
    t = v.getText()
    title.append(t)

tables = soup.find_all('table')

counter = 0
final = []
inter = []

for table in tables:
    t = unicodedata.normalize("NFKD", title[counter])
    subgroup = 'None'
    # print table.prettify()
    outtable = makelist(table)
    header = t
    create_outtable(outtable, (outpath + os.sep + 'NMFS_' + str(counter) + '_' + str(date) + '.csv'), header)
    current_species = []

    pop = False
    for v in outtable:
        # print len(v)
        current_species = []
        # print current_species

        if len(v) == 2:
            subgroup = None
            breaklist = v[0].split('(')
            if len(breaklist) == 1:
                subgroup = " ".join(v[0].split())
            else:
                pop = True
                commonname = unicodedata.normalize("NFKD", breaklist[0])
                sciname = unicodedata.normalize("NFKD", breaklist[(len(breaklist) - 1)].replace(')', ''))
        elif len(v) == 6:
            spe_infoList = v[0].split('(')
            print len(spe_infoList)
            if len(spe_infoList) == 1:
                current_species.append(commonname)
                current_species.append(sciname)
                for i in v:
                    current_species.append(unicodedata.normalize("NFKD", i))
                print pop
                continue

            elif len(spe_infoList) >= 2:
                com = unicodedata.normalize("NFKD", spe_infoList[0])
                sci = unicodedata.normalize("NFKD", spe_infoList[(len(spe_infoList) - 1)].replace(')', ''))
                current_species.append(com)
                current_species.append(sci)
                for i in v:
                    current_species.append(unicodedata.normalize("NFKD", i))
        else:
            pass
        try:
            print commonname
            print sciname
        except:
            pass
        print current_species
        if len(current_species) != 0:
            inter.append(current_species)

    counter += 1
#print inter
outDF_Full = pd.DataFrame(inter)
fulltable = (outpath + os.sep + 'NMFSb_' + str(counter) + '_' + str(date) + '.csv')
outDF_Full.to_csv(fulltable, encoding='utf-8')

# create_Finaltable(inter, (outpath + os.sep + 'NMFSb_' + str(counter) + '_' + str(date) + '.csv'))
##Fix ring seal- so that the pop is correct
### read in csv take the max values try to complete one row per pop