import pandas as pd
import os
import sys
import glob
import csv
import shutil
import time
import pika
import json

# python slice.py ${main_protocol_file_path} ${feedback_curve_folder_path} ${work_folder_path} ${output_folder_path} ${output_mq_name}

main_protocol_file_path = sys.argv[1]
feedback_curve_folder_path = sys.argv[2]
# fix path if it does not end with path separator
if(feedback_curve_folder_path[-1] != os.path.sep):
    feedback_curve_folder_path = feedback_curve_folder_path + os.path.sep

feedback_curve_folder_content = os.listdir(feedback_curve_folder_path)

work_folder_path = sys.argv[3]
# fix path if it does not end with path separator
if(work_folder_path[-1] != os.path.sep):
    work_folder_path = work_folder_path + os.path.sep

output_folder_path = sys.argv[4]
if(output_folder_path[-1] != os.path.sep):
    output_folder_path = output_folder_path + os.path.sep

output_mq_name = sys.argv[5]

chunksize = int(os.environ['CHUNK_SIZE'])
slice_size = int(os.environ['SLICE_SIZE'])
rabbitmq_host = os.environ['RABBITMQ_HOST']
rabbitmq_port = 5672
rabbitmq_user = os.environ['RABBITMQ_USER']
rabbitmq_pass = os.environ['RABBITMQ_PASS']

current_slice_size = 0

current_slice_path = ''
current_main_protocol_file_path = ''
current_slice_feedback_curves_folder_path = ''

total_slices = 0

def send_message_to_queue(msg, queue_name):
    global total_slices
    total_slices += 1
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

def get_next_slice_suffix():
    return str(time.time()).replace('.', '')

def move_current_slice_to_output():
    global current_slice_path
    # create output slice path
    output_slice_path = output_folder_path + os.path.basename(current_slice_path)
    # move computed files to output
    shutil.move(current_slice_path, output_slice_path)

def find_file_in_feedback_curves(fname):
    for idx, fb_curve_fname in enumerate(feedback_curve_folder_content):
        if fname in fb_curve_fname:
            del feedback_curve_folder_content[idx]
            return fb_curve_fname
    return ''

def process_chunk(df_chunk):
    global current_slice_size
    for index, row in df_chunk.iterrows():
        cdtime = pd.to_datetime(row['dateTime'], dayfirst=True)
        c_newdtime = cdtime.strftime("%Y%m%d_%H%M%S.%f")[:-3].replace('.', '')

        file_found = find_file_in_feedback_curves(c_newdtime + '_' + row['timerName'] + '_')
        if file_found:
            with open(current_main_protocol_file_path, 'a+', newline='') as main_protocol_csv:
                main_protocol_writer = csv.DictWriter(main_protocol_csv, fieldnames=row.keys())

                if not os.path.getsize(current_main_protocol_file_path):
                    main_protocol_writer.writeheader()

                main_protocol_writer.writerow(row.to_dict())

            current_feedback_curve_work_path = current_slice_feedback_curves_folder_path + os.path.sep + file_found
            shutil.copy(feedback_curve_folder_path + file_found, current_feedback_curve_work_path)

            current_slice_size += 1

            if current_slice_size == slice_size:
                move_current_slice_to_output()

                output_slice_name = os.path.basename(current_slice_path)
                message = {
                    "DATA": output_slice_name,
                    "SOURCE": os.environ['NODE_NAME']
                }
                send_message_to_queue(json.dumps(message), output_mq_name)

                assign_and_create_current_slice_paths()

                current_slice_size = 0
        else:
            with open(main_protocol_skipped_file_path, 'a+', newline='') as main_protocol_skipped_csv:
                main_protocol_skipped_writer = csv.DictWriter(main_protocol_skipped_csv, fieldnames=row.keys())

                if not os.path.getsize(main_protocol_skipped_file_path):
                    main_protocol_skipped_writer.writeheader()

                main_protocol_skipped_writer.writerow(row.to_dict())

def assign_and_create_current_slice_paths():
    global current_slice_path
    global current_main_protocol_file_path
    global current_slice_feedback_curves_folder_path

    slice_suffix = get_next_slice_suffix()
    current_slice_path = work_folder_path + 'slice_' + slice_suffix
    if not os.path.exists(current_slice_path):
        os.makedirs(current_slice_path)

    current_main_protocol_file_path = current_slice_path + os.path.sep + 'main_' + slice_suffix + '.csv'

    current_slice_feedback_curves_folder_path = current_slice_path + os.path.sep + 'feedback_curves'
    if not os.path.exists(current_slice_feedback_curves_folder_path):
        os.makedirs(current_slice_feedback_curves_folder_path)

assign_and_create_current_slice_paths()
main_protocol_skipped_file_path = work_folder_path + os.path.basename(main_protocol_file_path) + '_skipped'

with pd.read_csv(main_protocol_file_path, chunksize=chunksize) as reader:
    for df_chunk in reader:
        process_chunk(df_chunk)

print(f'current_slice_size={current_slice_size}')
if current_slice_size > 0:
    move_current_slice_to_output()
    output_slice_name = os.path.basename(current_slice_path)
    message = {
        "DATA": output_slice_name,
        "SOURCE": os.environ['NODE_NAME']
    }
    send_message_to_queue(json.dumps(message), output_mq_name)
else:
    if os.path.exists(current_slice_path):
        os.removedirs(current_slice_path + os.path.sep + 'feedback_curves')
        # os.removedirs(current_slice_path)

os.remove(main_protocol_file_path)

if os.path.exists(main_protocol_skipped_file_path):
    shutil.move(main_protocol_skipped_file_path, main_protocol_file_path)

work_dir = '/work'
if not os.path.exists(work_dir):
    os.makedirs(work_dir)

# Write the value of total_slices to a file
with open('/work/total_slices.txt', 'w') as f:
    f.write(str(total_slices))