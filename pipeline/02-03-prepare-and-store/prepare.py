import os
import numpy as np
import pandas as pd
import sys
import datetime
import json
import time
from pymongo import MongoClient
import pika

# python prepare.py ${main_protocol_file_path} ${work_folder_path} ${output_folder_path} ${output_mq_name}

def send_message_to_queue(msg, queue_name):
    rabbitmq_host = os.environ['RABBITMQ_HOST']
    rabbitmq_port = 5672
    rabbitmq_user = os.environ['RABBITMQ_USER']
    rabbitmq_pass = os.environ['RABBITMQ_PASS']

    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=msg,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    print(" [x] Sent %r" % msg)
    connection.close()

dir_working = sys.argv[2]
# fix path if it does not end with path separator
if(dir_working[-1] != os.path.sep):
    dir_working = dir_working + os.path.sep

# work folder is the same as work folder
srcdir = dir_working

dstdir = sys.argv[3]
if(dstdir[-1] != os.path.sep):
    dstdir = dstdir + os.path.sep

output_mq_name = sys.argv[4]

# %% MongoDB target database connection
mongo_host = os.environ['MONGO_HOST']
mongo_user = os.environ['MONGO_USERNAME']
mongo_password = os.environ['MONGO_PASSWORD']
mongo_target_db = os.environ['MONGO_REFERENCE_DATABASE']

client = MongoClient(mongo_host, username=mongo_user, password=mongo_password)
db = client[mongo_target_db]

# %% data source path
path_main = sys.argv[1] + os.path.sep + 'main_' + \
    sys.argv[1].split('slice_')[1] + '.csv'
path_raw_to_do_mapping_main = '/code/mappings/raw_to_do_mapping_main.csv'
path_raw_to_do_mapping_feedbackcurve = '/code/mappings/raw_to_do_mapping_feedbackcurve.csv'
dir_feedback = sys.argv[1] + os.path.sep + 'feedback_curves'+ os.path.sep
list_curve_featurenames = ['Time', 'Current',
                           'Voltage', 'Resistance', 'PWM', 'ZWKS', 'Force']

# %% meta info path and retrieve meta info
mongo_collection = db["metadata"]
csaved_val = mongo_collection.find_one({"meta_id": "1"})
cdict = json.loads(csaved_val['metainfo'])
df_meta_info_udf = pd.DataFrame.from_dict(cdict)

# %% data prepared path
dstdir_csv = dstdir + 'data_prepared_' + str(time.time()).replace('.', '') + os.path.sep + 'csv' + os.path.sep
if not os.path.exists(dstdir_csv):
    os.makedirs(dstdir_csv)

# %% renaming main
print('\n data preparation started ...')
print('\n integrating main protocol and metainfo ...')
df_main = pd.read_csv(path_main, encoding="ISO-8859-1")

df_raw_to_do_mapping_main = pd.read_csv(path_raw_to_do_mapping_main)
dict_raw_to_do_mapping_main = dict(zip(
    df_raw_to_do_mapping_main['VariableNameBOS6000'], df_raw_to_do_mapping_main['VariableNameUDF']))
df_main_udf = df_main.copy()
df_main_udf.columns = [dict_raw_to_do_mapping_main[key]
                       for key in df_main_udf.columns]

# change datetime to YYYYmmdd_HHMMSSffffffff for better sorting
arr_datetime = pd.to_datetime(
    df_main_udf.loc[:, 'DateTime'].values, dayfirst=True)
arr_datetime_udf = np.array([cdatetime.strftime("%Y%m%d_%H%M%S.%f")[
                            :-3] for cdatetime in arr_datetime])
df_main_udf.loc[:, 'DateTime'] = arr_datetime_udf


# %% integrate main and metainfo

# sf stands for single features after data preparation
df_sf_udf = df_main_udf.copy()

num_row, num_col = df_main_udf.shape
arr_progid_meta = df_meta_info_udf.loc[:, 'ProgID'].values
arr_progid_main = df_main_udf.loc[:, 'ProgID'].values
arr_index_meta_in_main = np.ndarray((num_row,), dtype=int)
for irecord in range(df_main_udf.shape[0]):
    cprogid_crecord = arr_progid_main[irecord]
    try:
        cindex_meta = np.where(arr_progid_meta == cprogid_crecord)[0][0]
    except:
        print('\nERROR!!')
        print('cprogid_crecord: ', cprogid_crecord)
        print('timestamp: ', datetime.time())
        print('sys.argv[1]: ', sys.argv[1])
        print('irecord: ', irecord)
        continue
    arr_index_meta_in_main[irecord] = cindex_meta

icol = 3
for icol in range(df_meta_info_udf.shape[1]):
    ccolname = df_meta_info_udf.columns[icol]
    if ccolname in df_main_udf:
        continue
    carr_ccol = df_meta_info_udf.iloc[:, icol]
    carr_metainfo_to_sf = np.array(
        [carr_ccol[cindex] for cindex in arr_index_meta_in_main])
    df_sf_udf[ccolname] = carr_metainfo_to_sf

path_single_feature_prepared = dstdir_csv + 'singlefeatures_allwms.csv'

# %% split main according to welding machines

print('\n splitting single features by welding machines ...')

arr_wms = df_main_udf.loc[:, 'ControlName'].values
list_all_wms = list(np.unique(arr_wms))
num_wms = len(list_all_wms)


# %% sort
cwm = list_all_wms[0]
for cwm in list_all_wms:
    cdstdir_csv = dstdir_csv + cwm + os.path.sep
    if not os.path.exists(cdstdir_csv):
        os.makedirs(cdstdir_csv)
    cpath_cwm_singlefeature = dstdir_csv + os.path.sep + \
        cwm + os.path.sep + 'singlefeature.csv'
    cpath_cwm_singlefeature_sorted_id = dstdir_csv + os.path.sep + \
        cwm + os.path.sep + 'singlefeature_sorted_id.csv'
    cpath_cwm_singlefeature_sorted_cdw = dstdir_csv + os.path.sep + \
        cwm + os.path.sep + 'singlefeature_sorted_cdw.csv'

    cwm_indices = np.where(arr_wms == cwm)[0]
    cdf_cwmsf = df_main_udf.iloc[cwm_indices, :].copy()

    # sort by id
    carr_id = cdf_cwmsf['id'].values
    cdf_cwmsf_sorted_id = cdf_cwmsf.sort_values(
        by=['id']).reset_index(drop=True)
    cdf_cwmsf_sorted_id.to_csv(cpath_cwm_singlefeature_sorted_id, index=False)

    # drop short-circuit measurements
    cdf_cwmsf_sorted_dropsc = cdf_cwmsf_sorted_id.drop(
        np.where(cdf_cwmsf_sorted_id['ProgNo'] > 200)[0]).reset_index(drop=True)

    # sort by cap_dress_wear
    num_all = len(cdf_cwmsf_sorted_dropsc)
    carr_wear = cdf_cwmsf_sorted_dropsc['CapWearCount']
    carr_dress = cdf_cwmsf_sorted_dropsc['CapDressCount']
    carr_dressdiff = np.append(0, np.diff(carr_dress))
    carr_newcap = np.zeros(carr_dressdiff.shape)
    indices_newcap = np.where(carr_dressdiff < 0)[0]
    carr_newcap[indices_newcap] = 1
    carr_capcount = carr_newcap.cumsum().astype(int)
    cdf_cwmsf_sorted_dropsc['CapCount'] = carr_capcount
    carr_cap_dress_wear = np.array(['{}{:02d}{:03d}'.format(
        carr_capcount[i], carr_dress[i], carr_wear[i]) for i in range(num_all)]).astype(int)
    cdf_cwmsf_sorted_dropsc['CapCountDressWear'] = carr_cap_dress_wear
    cdf_cwmsf_sorted_cdw = cdf_cwmsf_sorted_dropsc.sort_values(
        by=['CapCountDressWear']).reset_index(drop=True)
    cdf_cwmsf_sorted_cdw.to_csv(
        cpath_cwm_singlefeature_sorted_cdw, index=False)

print('\n integration of main protocol and metainfo finished')


# %% integrate feedback curves and reference curves

print('\n integrating feedback curves and reference curves ...')
# store all ref curves to a dict

list_all_loaded_progids = list(np.unique(arr_progid_main))
dict_refcurve = dict()
mongo_collection = db["reference_curves"]
path_raw_to_do_mapping_refcurve = '/code/mappings/raw_to_do_mapping_referencecurve.csv'
df_raw_to_do_mapping_refcurve = pd.read_csv(path_raw_to_do_mapping_refcurve)
dict_raw_to_do_mapping_refcurve = dict(zip(df_raw_to_do_mapping_refcurve['VariableNameBOS6000'],df_raw_to_do_mapping_refcurve['VariableNameUDF']))

for cpid in list_all_loaded_progids:
    # if not all programs are loaded - wait 1 sec 5 times
    for i in range(5):
        cref_curve = mongo_collection.find_one({"prog_id": str(cpid)})
        if cref_curve == None:
            time.sleep(1)
        else:
            break
    if cref_curve == None:
        raise Exception("Could not find reference data for program ID: " + cpid)
    
    cdf_refcurve = pd.DataFrame.from_dict(json.loads(cref_curve['ref_curve']))
    dict_refcurve[cpid] = cdf_refcurve

# %% merge refcurve and feedback curve
df_raw_to_do_mapping_feedbackcurve = pd.read_csv(
    path_raw_to_do_mapping_feedbackcurve)
dict_raw_to_do_mapping_feedbackcurve = dict(zip(
    df_raw_to_do_mapping_feedbackcurve['VariableNameBOS6000'], df_raw_to_do_mapping_feedbackcurve['VariableNameUDF']))

for cwm in list_all_wms:
    print('\n integrating {} ...'.format(cwm))
    cdstdir_csv = dstdir_csv + cwm + os.path.sep + 'processcurves' + os.path.sep
    if not os.path.exists(cdstdir_csv):
        os.makedirs(cdstdir_csv)
    cpath_cwm_singlefeature = dstdir_csv + os.path.sep + \
        cwm + os.path.sep + 'singlefeature_sorted_cdw.csv'
    cdf_cwmsf = pd.read_csv(cpath_cwm_singlefeature)
    num_records, num_sf = cdf_cwmsf.shape
    irecord = 0
    for irecord in range(num_records):
        if (irecord+1) % 100 == 0:
            print(
                '\n {}/{} feedback curves of {} finished ...'.format(irecord+1, num_records, cwm))

        cdatetime = cdf_cwmsf.loc[irecord, 'DateTime'].replace('.', '')
        cprogno = cdf_cwmsf.loc[irecord, 'ProgNo']
        cdress = cdf_cwmsf.loc[irecord, 'CapDressCount']
        cwear = cdf_cwmsf.loc[irecord, 'CapWearCount']
        ccurve_filename = cdatetime + '_' + cwm + '_Prog' + \
            str(cprogno) + '_Dress' + str(cdress) + \
            '_Wear' + str(cwear) + '.csv'
        cpath_feedbackcurve = dir_feedback + ccurve_filename
        cdf_feedbackcurve = pd.read_csv(
            cpath_feedbackcurve).loc[:, list_curve_featurenames]
        cdf_feedbackcurve.columns = [
            dict_raw_to_do_mapping_feedbackcurve[key] for key in cdf_feedbackcurve.columns]
        cprogid = cdf_cwmsf.loc[:, 'ProgID'][irecord]
        cdf_refcurve = dict_refcurve[cprogid]
        # save to csv
        crecordid = cdf_cwmsf.loc[irecord, 'id']
        cpath_processcurve = cdstdir_csv + str(crecordid) + '.csv'
        df_curve_concat = pd.concat(
            [cdf_feedbackcurve, cdf_refcurve.iloc[:, 1:]], axis=1)
        df_curve_concat.to_csv(cpath_processcurve, index=False)

print('\n integrating feedback curves and reference curves finished!')