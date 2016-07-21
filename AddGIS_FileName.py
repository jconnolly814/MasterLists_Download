import datetime
import os
import arcpy

import pandas as pd

masterlist = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\April2016\addColumnsMaster_June2016\MasterJune2016_20160628.csv'
colIndex_entId = 0
addcol = 'Range_Filename'
inlocation = 'J:\Workspace\ESA_Species\CriticalHabitat\NAD_Final'

outfile = r'C:\Users\JConno02\Documents\Projects\ESA\MasterLists\April2016\addColumnsMaster_June2016\MasterJune2016_20160628_CH.csv'

addinfo_dict = {}


def loop_all_species_gis(in_path, fields, data_dict):
    if in_path[-3:] == 'gdb':
        ingdb = in_path
        arcpy.env.workspace = ingdb
        fclist = arcpy.ListFeatureClasses()
        for fc in fclist:
            with arcpy.da.SearchCursor(fc, fields) as cursor:
                for row in cursor:
                    entid = row[0]
                    filename = row[1]
                    data_dict[entid] = filename
                del row, cursor
    else:
        list_ws = os.listdir(in_path)
        print list_ws
        for v in list_ws:
            print v
            if v[-3:] == 'gdb':
                ingdb = in_path + os.sep + v
                arcpy.env.workspace = ingdb
                fclist = arcpy.ListFeatureClasses()
                for fc in fclist:
                    with arcpy.da.SearchCursor(fc, fields) as cursor:
                        for row in cursor:
                            entid = row[0]
                            filename = row[1]
                            data_dict[entid] = filename
                        del row, cursor

            else:
                continue


start_script = datetime.datetime.now()
print "Script started at {0}".format(start_script)

search_field = ['EntityID', 'FileName']
loop_all_species_gis(inlocation, search_field, addinfo_dict)
list_entid = addinfo_dict.keys()


Master_df = pd.read_csv(masterlist)

listheader = Master_df.columns.values.tolist()

listheader.append(addcol)

entidindex = colIndex_entId

rowcount = Master_df.count(axis=0, level=None, numeric_only=False)
rowindex = rowcount.values[0]
colindex = len(listheader) - 2  # to make base 0

row = 0
while row < rowindex:
    entid = str(Master_df.iloc[row, entidindex])
    if entid in list_entid:
        add_value= str(addinfo_dict[entid])
        print add_value
        Master_df.loc[row,addcol] = add_value
    else:
        Master_df.loc[row,addcol] = 'FALSE'
    row += 1

Master_df.to_csv(outfile, header=listheader)

end = datetime.datetime.now()
print "Elapse time {0}".format(end - start_script)
