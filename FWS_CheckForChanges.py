__author__ = 'JConno02'

from bs4 import BeautifulSoup
import requests
import sys
import os


import pandas as pd

import csv

oldMaster =''
newMaster


__author__ = 'JConno02'

from bs4 import BeautifulSoup
import requests
import sys
import os

import pandas as pd

import csv

##Todo rather than loading in dict add a field to df that is in out and pop field based on if ent in on filter list then select all rows that are in
date = 2016060722
inpath_FULLTESS =r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\TESSQueries\20160606\FilteredinPandas\FullTess_20160607.csv'
outpath = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\TESSQueries\20160606\FilteredinPandas'
url = "http://ecos.fws.gov/services/TessQuery?request=query&xquery=/SPECIES_DETAIL"

r = requests.get(url)
reload(sys)
sys.setdefaultencoding('utf8')
## These are the variables use within the xml, if FWS changes their variable this will need to be updated

identifier='entity_id'
deafultTags = ['spcode', 'vipcode', 'sciname', 'comname', 'invname', 'pop_abbrev', 'pop_desc', 'family', 'status',
               'status_text', 'lead_agency', 'lead_region', 'country', 'listing_date', 'dps', 'refuge_occurrence',
               'delisting_date']


colOrder = {
    9: 'spcode',
    10: 'vipcode',
    2: 'sciname',
    1: 'comname',
    3: 'invname',
    6: 'pop_abbrev',
    7: 'pop_desc',
    8: 'family',
    4: 'status',
    5: 'status_text',
    11: 'lead_agency',
    12: 'lead_region',
    13: 'country',
    14: 'listing_date',
    15: 'dps',
    16: 'refuge_occurrence'}


def CreatDicts(list_vars,id):
    for v in list_vars:
        globals()[v] = {}
    print '\nSpecies information that will be extracted {0} using the identifier {1}'.format(list_vars, id)

def list_dicts():
    # global spcode, vipcode, sciname, comname, invname, pop_abbrev, pop_desc, family, status, status_text, lead_agency, lead_region, country, listing_date, dps, refuge_occurrence
    listitems = []
    listdicts = []
    for name in globals():
        if not name.startswith('__'):
            listitems.append(name)
    for value in listitems:
        if type((globals()[value])) is dict:
            listdicts.append(value)
    listdicts.remove('colOrder')
    return listdicts


def LoadDatainDicts (incsv,id):
    allEntities =[]
    dfFullTess = pd.read_csv(incsv)
    header= list(dfFullTess.columns.values)
    print header
    entidindex = (header.index(id))
    print entidindex
    rowcount = dfFullTess.count(axis=0, level=None, numeric_only=False)
    rowindex = rowcount.values[0]
    colindex = len(header)-1 # to make base 0

    row = 0
    while row < (rowindex):
        entid = str(dfFullTess.iloc[row, entidindex]) #Entid must  be in position 0
        print "Working on species {0}, row {1}".format(entid, row)

        allEntities.append(str(entid))
        col=1 ## assuming an index row from pandas export
        while col <(colindex+1): # base 0
            if col == entidindex:
                col+=1
                continue
            else:
                colheader_dict = header[col]
                value = dfFullTess.iloc[row, col]
                ((globals()[colheader_dict][entid]))= str(value)
                col+=1
        row += 1
    print allEntities
    return allEntities


def filterquery(listvars, entlist):
    filterentlist = []
    possAnswer = ['Yes', 'No']
    askQ = True
    while askQ:
        status = raw_input('Where is the status located {0}: '.format(listvars))
        if status not in listvars:
            print 'This is not a valid answer: remove quotes and spaces'
        else:
            liststatus = set(globals()[status].values())
            break

    while askQ:
        country = raw_input('Where is the country  located {0}: '.format(listvars))
        if country not in listvars:
            print 'This is not a valid answer: remove quotes and spaces'
        else:
            break
    Failed = False
    while askQ:
        StatusConsidered = ['Experimental Population  Non-Essential', 'Threatened', 'Endangered', 'Proposed Threatened',
                            'Proposed Endangered', 'Candidate']

        defaultStatus = raw_input(
            'The current status being considered are:  \n{0}\nWould you like to add additional statuses? : Yes or No: '.format(
                StatusConsidered))

        if defaultStatus not in possAnswer:
            print('This is not a valid answer, must be Yes or No')
        else:
            if defaultStatus == 'Yes':
                filtered_status = raw_input('Which statuses should be included in the filtered table{0}? '.format(
                    liststatus))  ##TODO print out list one value per line
                listinput = filtered_status.split(",")
                for v in listinput:
                    if v not in liststatus:
                        print v
                        Failed = True
                        print 'This is not a valid answer: values must be separated by a comma without a space'
                    else:
                        if v not in StatusConsidered:
                            StatusConsidered.append(v)

            else:
                if Failed == False:
                    askQ = False
                else:
                    askQ = True

    for i in entlist:
        check_status = globals()[status][i]

        if check_status in StatusConsidered:
            filterentlist.append(i)

    for i in filterentlist:
        check_country = str(globals()[country][i])
        check_country = " ".join(check_country.split())
        print i
        print check_country
        if check_country == '2':
            filterentlist.remove(i)
            print 'removed {0}'.format(i)
        elif check_country == 'None':
            filterentlist.remove(i)
            print 'removed None {0}'.format(i)
        else:
            pass

    print filterentlist
    return filterentlist


def CreateSpecisTable(species_entList, species_info_var, colOrder):
    list_cols = colOrder.values()
    list_index = colOrder.keys()

    counter = ((max(list_index)) + 1)
    for v in species_info_var:
        if v not in list_cols:
            # print v
            # print counter
            colOrder[counter] = v
            counter += 1
        else:
            continue

    list_final = colOrder.keys()
    header = colOrder.values()

    outlist = []
    for i in species_entList:
        col = 1
        current_species = []
        entid = str(i)
        current_species.append(entid)
        while col < ((max(list_final) + 1)):
            current_col = colOrder[col]
            value = ((globals()[current_col][entid]))
            try:
                vclean = value.encode('utf8', 'replace')
                current_species.append(vclean)
                col += 1
            except (UnicodeEncodeError, UnicodeDecodeError):
                current_species.append("removed")
                col += 1

        outlist.append(current_species)
    return outlist, header


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
            writer = csv.writer(output, delimiter=",", quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
            for val in outInfo:
                writer.writerow([val])


CreatDicts(deafultTags,identifier)
sp_info_need = list_dicts()
fulllist= LoadDatainDicts(inpath_FULLTESS,identifier)

filter_list = filterquery(sp_info_need,fulllist)
print "There are {0} listed entities that will be considered".format(len(filter_list))

filterResults, header = CreateSpecisTable(filter_list, deafultTags, colOrder)

finalheader = ['EntityID']
for v in header:
    finalheader.append(v)

for value in sp_info_need:
    create_outtable(globals()[value], (outpath+ os.sep+ str(value)+"_"+str(date)+'.csv') , finalheader)


outfilter = outpath + os.sep + 'FilteredTess_' + str(date) + '.csv'



outDF_filtered = pd.DataFrame(filterResults, columns=finalheader)
outDF_filtered.to_csv(outfilter, encoding='utf-8')



# create_outtable(filterResults, outfilter, finalheader)

