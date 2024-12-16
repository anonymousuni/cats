# -*- coding: utf-8 -*-
"""
Created on Tue Feb  9 17:19:54 2021

@author: Baifan Zhou (SIRIUS/UiO)
@author: Nikolay Nikolov (SINTEF)
"""

import os
import numpy as np
import pandas as pd
import sys
import re
import json
from pymongo import MongoClient

# python prepare.py ${input_folder_path}

srcdir = sys.argv[1]

# fix path if it does not end with path separator
if(srcdir[-1] != os.path.sep):
    srcdir = srcdir + os.path.sep

# %% data source path
path_metainfo = srcdir + 'metainfo.csv'
path_raw_to_do_mapping_metainfo = '/code/mappings/raw_to_do_mapping_metainfo.csv'
path_raw_to_do_mapping_refcurve = '/code/mappings/raw_to_do_mapping_referencecurve.csv'
dir_refcurve = srcdir + 'reference_curves'
list_curve_featurenames = ['Time', 'Current', 'Voltage', 'Resistance', 'PWM', 'ZWKS', 'Force']

# %% MongoDB target database connection
mongo_host = os.environ['MONGO_HOST']
mongo_user = os.environ['MONGO_USERNAME']
mongo_password = os.environ['MONGO_PASSWORD']
mongo_target_db = os.environ['MONGO_TARGET_DATABASE']

client = MongoClient(mongo_host, username=mongo_user, password=mongo_password)
db = client[mongo_target_db]

# %% renaming metainfo
df_metainfo = pd.read_csv(path_metainfo,encoding = "ISO-8859-1")
df_raw_to_do_mapping_metainfo = pd.read_csv(path_raw_to_do_mapping_metainfo)
dict_raw_to_do_mapping_metainfo = dict(zip(df_raw_to_do_mapping_metainfo['VariableNameBOS6000'],df_raw_to_do_mapping_metainfo['VariableNameUDF']))
df_metainfo_udf = df_metainfo.copy()
df_metainfo_udf.columns = [dict_raw_to_do_mapping_metainfo[key] for key in df_metainfo_udf.columns]

# %% store metadata to database
mongo_collection = db["metadata"]
metainfo_udf_obj = {
    "meta_id": "1",
    "metainfo": json.dumps(df_metainfo_udf.to_dict())
}
# check if the record already exists in the database
cexisting_metainfo_udf_obj = mongo_collection.find_one({"meta_id": "1"})
if cexisting_metainfo_udf_obj == None:
    mongo_collection.insert_one(metainfo_udf_obj)
else:
    mongo_collection.replace_one({"meta_id": "1"}, metainfo_udf_obj)

csaved_val = mongo_collection.find_one({"meta_id": "1"})
cdict = json.loads(csaved_val['metainfo'])
cdf_reloaded_metainfo = pd.DataFrame.from_dict(cdict)

# df_metainfo_udf.to_csv(dstdir + os.path.sep + 'metainfo_udf.csv', index=False)

# %% integrate feedback curves and reference curves

print('\n integrating feedback curves and reference curves ...')

# store all ref curves to a dict
df_raw_to_do_mapping_refcurve = pd.read_csv(path_raw_to_do_mapping_refcurve)
dict_raw_to_do_mapping_refcurve = dict(zip(df_raw_to_do_mapping_refcurve['VariableNameBOS6000'],df_raw_to_do_mapping_refcurve['VariableNameUDF']))

list_refcurve_csv = os.listdir(dir_refcurve)
irefcurve = 0

mongo_collection = db["reference_curves"]
for irefcurve in range(len(list_refcurve_csv)):
    cfilename_refcurve = list_refcurve_csv[irefcurve]
    
    cwm = re.sub('Prog.+','',cfilename_refcurve)
    cprogno = int(re.sub('WeldingMachine\d+Prog','',cfilename_refcurve).replace('.csv',''))
    cindex_meta = np.where((df_metainfo_udf.loc[:,"ProgNo"] == cprogno) & (df_metainfo_udf.loc[:,"ControlName"] == cwm))[0][0]
    cprogid = df_metainfo_udf.loc[:,'ProgID'].values[cindex_meta]
    
    cdf_refcurve = pd.read_csv(dir_refcurve + os.path.sep + cfilename_refcurve).loc[:,list_curve_featurenames]
    cdf_refcurve.columns = [dict_raw_to_do_mapping_refcurve[key] for key in cdf_refcurve.columns]
    crefcurve_obj = {
        "prog_id": str(cprogid),
        "ref_curve": json.dumps(cdf_refcurve.to_dict())
    }

    # check if the record already exists in the database
    cexisting_refcurve = mongo_collection.find_one({"prog_id": str(cprogid)})
    if cexisting_refcurve == None:
        mongo_collection.insert_one(crefcurve_obj)
    else:
        mongo_collection.replace_one({"prog_id": str(cprogid)}, crefcurve_obj)
    
    csaved_val = mongo_collection.find_one({"prog_id": str(cprogid)})
    cdict = json.loads(csaved_val['ref_curve'])
    cdf_reloaded_refcurve = pd.DataFrame.from_dict(cdict)