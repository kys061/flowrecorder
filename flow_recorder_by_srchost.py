#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Saisei Networks Inc. All rights reserved.

import os
import subprocess
from datetime import datetime, timedelta
import time
import re
import csv
import itertools
import logging
import pandas as pd

# Env for (eg. echo 'show int stm3 flows top 100 by average_rate' | ./stm_cli.py admin:admin@localhost)
INTERFACE_LIST = [
    'stm9',		# Interface name
    'stm10'		# Interface name
]
TOP_NUM = '100'
ARRIVAL_RATE = '10'
USERNAME = 'admin'
PASSWORD = 'admin'
TOP_NUM_FOR_FIELD = '10'

# Init path and filename
FLOW_LOG_FOLDER_PATH = r'/var/log/flows'
FLOW_USER_LOG_FOLDER = r'/var/log/flows/users'
SCRIPT_MON_LOG_FILE = r'/var/log/flow_recorder.log'
SCRIPT_MON_LOG_FOLDER = r'/var/log'
STM_SCRIPT_PATH = r'/opt/stm/target/pcli/stm_cli.py'

# logger setting
Logger = logging.getLogger('saisei.flow.recorder')
Logger.setLevel(logging.INFO)
handler = logging.FileHandler(SCRIPT_MON_LOG_FILE)
handler.setLevel(logging.INFO)
filter = logging.Filter('saisei.flow')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

handler.setFormatter(formatter)
handler.addFilter(filter)
Logger.addHandler(handler)
Logger.addFilter(filter)

# For parse_fieldname in order to reduce cpu loads.
CMD_FOR_FIELD = ["echo \'show int {} flows with arrival_rate > {} top {} by \
                average_rate select distress geolocation autonomous_system \
                retransmissions round_trip_time timeouts udp_jitter\' | {} \
                {}:{}@localhost".format(interface, ARRIVAL_RATE,
                                        TOP_NUM_FOR_FIELD, STM_SCRIPT_PATH,
                                        USERNAME, PASSWORD)
                 for interface in INTERFACE_LIST]

CMD = []
for i in range(len(INTERFACE_LIST)):
    CMD.append("echo \'show int {} flows with arrival_rate > {} top {} by \
               average_rate select distress geolocation autonomous_system \
               retransmissions round_trip_time timeouts udp_jitter\' | \
               {} {}:{}@localhost".format(INTERFACE_LIST[i], ARRIVAL_RATE,
               TOP_NUM, STM_SCRIPT_PATH, USERNAME, PASSWORD))
#    CMD.append('echo \'show int '+INTERFACE_LIST[i]+r' flows with arrival_rate > '\
#               +ARRIVAL_RATE+' top '+TOP_NUM + ' by average_rate select distress  \
#               geolocation autonomous_system retransmissions round_trip_time \
#               timeouts udp_jitter\' | '+STM_SCRIPT_PATH+' '+USERNAME+r':'
#               +PASSWORD+r'@localhost')

# For several interfaces(eg. iterate logging for interfaces(stm01, stm02) sequently)
INTERVAL = 20
# Recording type selecting(0: only csv, 1: only txt, others:csv and txt)
RECORD_TYPE = 2
# Get current time
CURRENTTIME_INIT = datetime.today().strftime("%Y:%m:%d")
# archive count
archive_count = 1


# Parse the current date
def parsedate(today_date):
    try:
        parseDate = today_date.split(':')
        year = parseDate[0]
        month = parseDate[1]
        day = parseDate[2]
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE, "parsedate() cannot be executed, {}".format(e))
        pass
    return [year, month, day]

def parse_fieldnames(data):
    try:
        str_fieldnames = ''
        for i in range(1):
            str_fieldnames = data.splitlines()[1]
        #print(str_fieldnames)
        fieldname = str_fieldnames.split()
        fieldnames = []
        for field in fieldname:
            fieldnames.append(re.sub('\"|\'', "", field))
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE, "parsedate() cannot be executed, {}".format(e))
        pass
    return fieldnames
# Parse the data from the command
def parse_csv(csv_data_row, csv_filepath, save_csv_users_folder):
    """
    def parse is the function that parses raw data from the shell into csv and txt
    csv_data_row is the raw data,
    csv_filepath is the path for csv,
    save_csv_users_folder is the path for users(srchost or dsthost)
    """
    try:
        pattern = r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
        pattern_01 = r'[-]{1,10}'
#        pattern_02 = r'flow|in_if|eg_if|srchost|srcport|dsthost|dstport|ip_prot|app|duration|arr_rate|target_rate|avgrate|bytes|pkts|disc_pkts|udp_jitter|touts|rtt|rexmits|geoloc|distress|\"AS\"'
        pattern_03 = r'\s+\n'
        pattern_04 = r'Flows at [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'

        m = re.search(pattern, csv_data_row)
        startidx = m.start()
        endidx = m.end()
        csv_time = csv_data_row[startidx:endidx]

#        t = re.search(pattern_04, csv_data_row)
#        s_idx = t.start()
#        e_idx = t.end()
#        title_time = csv_data_row[s_idx:e_idx]

        # Get fieldnames
        fieldnames = parse_fieldnames(csv_data_row)

        # Make field pattern
        pattern_02 = ''
        for field in fieldnames:
            pattern_02 += field+"|"

        csv_data_row = re.sub(pattern, "", csv_data_row)
        csv_data_row = re.sub(pattern_01, "", csv_data_row)
        csv_data_row = re.sub(pattern_02, "", csv_data_row)
        csv_data_row = re.sub(pattern_03, "\r\n", csv_data_row)
        reader = csv.DictReader(itertools.islice(csv_data_row.splitlines(), 1,
                                                 None),
                                delimiter=' ',
                                skipinitialspace=True,
                                fieldnames=fieldnames)

        result = sorted(reader, key=lambda d: d['srchost'])
        # for
        count_values = 1
        labels = []
        labels = fieldnames
        # Make Values
        for row in result:
            values = []
            for label in fieldnames:
                values.append(row[label])
            middles = []

#            value = []
#            for label in fieldnames:
#                value.append(row[label])
#            print value

            for label in labels:
                middles.append('='*len(label))

            labelLine = list()
            middleLine = list()
            valueLine = list()

            for label, middle, value in zip(labels, middles, values):
                padding = max(len(str(label)), len(str(value)))
                labelLine.append('{0:<{1}}'.format(label, padding))  # generate a string with the variable whitespace padding
                middleLine.append('{0:<{1}}'.format(middle, padding))
                valueLine.append('{0:<{1}}'.format(value, padding))
            # Add datetime
            timestamp = 'timestamp'
            labelLine.insert(0, '{0:<{1}}'.format(timestamp, len(str(csv_time))))
            middleLine.insert(0, '{0:<{1}}'.format('='*len(timestamp), len(str(csv_time))))
            valueLine.insert(0, '{0:<{1}}'.format(csv_time, len(str(csv_time))))

#            if count_values == 1:
#                print ('{} length of each result -> {}\r'.format(title_time, len(result)))
#                print ('\t'.join(labelLine) + '\r')
#                print ('\t'.join(middleLine) + '\n')

#            print ('\t'.join(valueLine)+'\r')
#            count_values += 1

#            if count_values == len(result)+1:
#                count_values = 1
################################################################################################################
#   STM9
################################################################################################################
            if row['in_if'] == 'stm9':
                flowlog_csv_by_dsthost_path = save_csv_users_folder + row['dsthost'] + '-' + row['in_if'] + '-inbound.csv'
                flowlog_txt_by_dsthost_path = save_csv_users_folder + row['dsthost'] + '-' + row['in_if'] + '-inbound.txt'
################################################################################################################
#   FOR CSV if row's in_if is STM9
################################################################################################################
                if not (os.path.isfile(flowlog_csv_by_dsthost_path)):
                    csv_file = open(flowlog_csv_by_dsthost_path, 'w')
                    csv_file.close()
                    with open(flowlog_csv_by_dsthost_path, "a") as output:
                        output.write(','.join(fieldnames))
                        output.write('\r\n')
                        output.write('{},'.format(csv_time))
                        writer = csv.DictWriter(f=output, fieldnames=reader.fieldnames)
                        writer.writerow(row)
                else:
                    with open(flowlog_csv_by_dsthost_path, "a") as output:
                        output.write('{},'.format(csv_time))
                        writer = csv.DictWriter(f=output, fieldnames=reader.fieldnames)
                        writer.writerow(row)
################################################################################################################
#   FOR TXT if row's in_if is STM9
################################################################################################################
                if not (os.path.isfile(flowlog_txt_by_dsthost_path)):
                    txt_file = open(flowlog_txt_by_dsthost_path, 'w')
                    txt_file.close()
                    if count_values >= 1 or count_values < len(result)+1:
                        with open(flowlog_txt_by_dsthost_path, "a") as output:
                            output.write('    '.join(labelLine) + '\r\n')
                            #output.write('    '.join(middleLine) + '\r\n')
                    with open(flowlog_txt_by_dsthost_path, "a") as output:
                        output.write('    '.join(valueLine)+'\r\n')
                    count_values += 1

                    if count_values == len(result)+1:
                        count_values = 1
                else:
                    with open(flowlog_txt_by_dsthost_path, "a") as output:
                        output.write('    '.join(valueLine)+'\r\n')
                    count_values += 1

                    if count_values == len(result)+1:
                        count_values = 1
################################################################################################################
#   STM10
################################################################################################################
            elif row['in_if'] == 'stm10':
                flowlog_csv_by_srchost_path = save_csv_users_folder + row['srchost'] + '-' + row['in_if'] + '-outbound.csv'
                flowlog_txt_by_srchost_path = save_csv_users_folder + row['srchost'] + '-' + row['in_if'] + '-outbound.txt'
################################################################################################################
#   FOR CSV if row's in_if is STM10
################################################################################################################
                if not (os.path.isfile(flowlog_csv_by_srchost_path)):
                    csv_file = open(flowlog_csv_by_srchost_path, 'w')
                    csv_file.close()
                    with open(flowlog_csv_by_srchost_path, "a") as output:
                        output.write(','.join(fieldnames))
                        output.write('\r\n')
                        output.write('{},'.format(csv_time))
                        writer = csv.DictWriter(f=output, fieldnames=reader.fieldnames)
                        writer.writerow(row)
                else:
                    with open(flowlog_csv_by_srchost_path, "a") as output:
                        output.write('{},'.format(csv_time))
                        writer = csv.DictWriter(f=output, fieldnames=reader.fieldnames)
                        writer.writerow(row)
################################################################################################################
#   FOR TXT if row's in_if is STM10
################################################################################################################
                if not (os.path.isfile(flowlog_txt_by_srchost_path)):
                    txt_file = open(flowlog_txt_by_srchost_path, 'w')
                    txt_file.close()
                    if count_values >= 1 or count_values < len(result)+1:
                        with open(flowlog_txt_by_srchost_path, "a") as output:
                            output.write('    '.join(labelLine) + '\r\n')
                            #output.write('    '.join(middleLine) + '\r\n')
                    with open(flowlog_txt_by_srchost_path, "a") as output:
                        output.write('    '.join(valueLine)+'\r\n')
                    count_values += 1

                    if count_values == len(result)+1:
                        count_values = 1
                else:
                    with open(flowlog_txt_by_srchost_path, "a") as output:
                        output.write('    '.join(valueLine)+'\r\n')
                    count_values += 1

                    if count_values == len(result)+1:
                        count_values = 1
            else:
                pass
            #for row in reader:
            #        with open(csv_filepath, "a") as log:
            #                log.write('{},'.format(csv_time))
            #        with open(csv_filepath, "a") as output:
            #                writer = csv.DictWriter(f=output, fieldnames=reader.fieldnames)
            #                writer.writerow(row)
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE, "parse_csv() cannot be executed, {}".format(e))
        pass
    return 0

# Excute command in shell
def subprocess_open(command):
    try:
        popen = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (stdoutdata, stderrdata) = popen.communicate()
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE, "subprocess_open() cannot be executed, {}".format(e))
        pass
    return stdoutdata, stderrdata

# Create log folder by YearMon
def create_folder(foldername):
    try:
        folder_year_mon = FLOW_LOG_FOLDER_PATH + '/' + foldername[0] + foldername[1]
        folder_user_year_mon = FLOW_USER_LOG_FOLDER + '/' + foldername[0] + foldername[1]

        if not os.path.exists(folder_year_mon):
            os.makedirs(folder_year_mon)
        if not os.path.exists(folder_user_year_mon):
            os.makedirs(folder_user_year_mon)
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE, "create_folder() cannot be executed, {}".format(e))
        pass

# Write the txt type log from the command
def do_txt_log(cmd_txt, save_filepath, foldername, curTime):
    try:
        if not (os.path.isfile(save_filepath)):
            create_folder(foldername)
            subprocess_open(cmd_txt)
        else:
            subprocess_open(cmd_txt)
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE, "do_txt_log() cannot be executed, {}".format(e))
        pass

# Write the csv type log from the command
def do_csv_log(cmd_csv, save_csv_filepath, save_csv_users_folder, foldername, curTime):
    try:
        if not (os.path.isfile(save_csv_filepath)):
            create_folder(foldername)
            csv_file = open(save_csv_filepath, 'w')
            csv_file.close()
            raw_csv_data = subprocess_open(cmd_csv)
            if 'Cannot connect to server' in raw_csv_data[0]:
                logger(1, SCRIPT_MON_LOG_FILE, "{}".format(raw_csv_data[0]))
            elif 'does not exist' in raw_csv_data[0]:
                logger(1, SCRIPT_MON_LOG_FILE, "{}".format(raw_csv_data[0]))
            elif 'no matching objects' in raw_csv_data[0]:
                logger(1, SCRIPT_MON_LOG_FILE, "{}".format(raw_csv_data[0]))
            else:
                fieldnames = parse_fieldnames(raw_csv_data[0])
                fieldnames.insert(0, 'timestamp')
                with open(save_csv_filepath, "a") as log:
                    log.write(','.join(fieldnames))
                    log.write('\r\n')
                time.sleep(1)
                parse_csv(raw_csv_data[0], save_csv_filepath, save_csv_users_folder)
                Logger.info('Flow info from interfaces {} is extracted to {} \
successfully!'.format(INTERFACE_LIST[i], save_csv_users_folder))
#                logger(0, SCRIPT_MON_LOG_FILE, 'Flow info from interfaces {} \
#s extracted to {} successfully!'.format(INTERFACE_LIST[i], save_csv_filepath))
        else:
            raw_csv_data = subprocess_open(cmd_csv)
            if 'Cannot connect to server' in raw_csv_data[0]:
                logger(1, SCRIPT_MON_LOG_FILE, "{}".format(raw_csv_data[0]))
            elif 'does not exist' in raw_csv_data[0]:
                logger(1, SCRIPT_MON_LOG_FILE, "{}".format(raw_csv_data[0]))
            elif 'no matching objects' in raw_csv_data[0]:
                logger(1, SCRIPT_MON_LOG_FILE, "{}".format(raw_csv_data[0]))
            else:
                parse_csv(raw_csv_data[0], save_csv_filepath, save_csv_users_folder)
                Logger.info('Flow info from interfaces {} is extracted to {} \
successfully!'.format(INTERFACE_LIST[i], save_csv_users_folder))
#                logger(0, SCRIPT_MON_LOG_FILE, 'Flow info from interfaces {} \
#s extracted to {} successfully!'.format(INTERFACE_LIST[i], save_csv_filepath))
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE, "do_csv_log() cannot be executed, {}".format(e))
        pass

# Get current date
def get_nowdate():
    try:
        nowdate = datetime.today().strftime("%Y:%m:%d")
        nowdatetime = datetime.today().strftime("%Y/%m/%d %H:%M:%S")
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE, "get_nowdate() cannot be executed, {}".format(e))
        pass
    return [nowdate, nowdatetime]

# Get last month
'''Return int'''
def get_lastmonth():
    first_day_of_current_month = datetime.today().replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_month_of_current_year = datetime.today().replace(month=1, day=1)
    last_year = first_month_of_current_year - timedelta(days=1)
    return (first_month_of_current_year.year, last_year.year, first_day_of_current_month.month, last_day_of_previous_month.month)

def is_month_begin():
    return datetime.today() + pd.offsets.MonthBegin(0) == datetime.today()

# Get filepath and command string
def get_filepaths(foldername, i):
    try:
        #txt_filename = foldername[0] + foldername[1] + foldername[2] + r'_flowinfo_' + INTERFACE_LIST[i] + r'_' + TOP_NUM + r'.txt'
        #csv_filename = foldername[0] + foldername[1] + foldername[2] + r'_flowinfo_' + INTERFACE_LIST[i] + r'_' + TOP_NUM + r'.csv'
	#echo 'show int stm10 flows with arrival_rate > 10 top 10000 by average_rate select distres admin:admin@localhost
	#echo 'show int stm10 flows with arrival_rate > 10 top 10000 by average_rate select distress geolocation autonomous_system  retransmissions round_trip_time timeouts udp_jitter'
    #| /opt/stm/target/pcli/stm_cli.py admin:admin@localhost
    #exec_cmd_txt = 'echo \'show int {} flows with arrival_rate > {} top {} by average_rate select distress geolocation autonomous_system  retransmissions round_trip_time timeouts udp_jitter\' | {} {}:{}@localhost'.format(INTERFACE_LIST[i], ARRIVAL_RATE, TOPNUM, STM_SCRIPT_PATH, USERNAME, PASSWORD) + save_txt_filepath
        save_txt_filepath = FLOW_LOG_FOLDER_PATH + '/' + foldername[0] + foldername[1] + '/' + foldername[0] + foldername[1] + foldername[2] + r'_flowinfo_' + INTERFACE_LIST[i] + r'_' + TOP_NUM + r'.txt'
        save_csv_filepath = FLOW_LOG_FOLDER_PATH + '/' + foldername[0] + foldername[1] + '/' + foldername[0] + foldername[1] + foldername[2] + r'_flowinfo_' + INTERFACE_LIST[i] + r'_' + TOP_NUM + r'_test.csv'
        save_csv_users_folder = FLOW_USER_LOG_FOLDER + '/' + foldername[0] + foldername[1] + '/' + foldername[0] + foldername[1] + foldername[2] + r'-'
        exec_cmd_txt = 'echo \'show int ' + INTERFACE_LIST[i] + r' flows with arrival_rate > ' + ARRIVAL_RATE + ' top ' + TOP_NUM + ' by average_rate select distress geolocation autonomous_system  retransmissions round_trip_time timeouts udp_jitter\' | ' + STM_SCRIPT_PATH + ' ' + USERNAME + r':' + PASSWORD + r'@localhost >> ' + save_txt_filepath
	#exec_cmd_txt = 'echo \'show int ' + INTERFACE_LIST[i] + r' flows top ' + TOP_NUM + ' by average_rate\' | ' + STM_SCRIPT_PATH + ' ' + USERNAME + r':' + PASSWORD + r'@localhost >> ' + save_txt_filepath
        exec_cmd_csv = 'echo \'show int ' + INTERFACE_LIST[i] + r' flows with arrival_rate > ' + ARRIVAL_RATE + ' top ' + TOP_NUM + ' by average_rate select distress geolocation autonomous_system  retransmissions round_trip_time timeouts udp_jitter\' | ' + STM_SCRIPT_PATH + ' ' + USERNAME + r':' + PASSWORD + r'@localhost'
	#exec_cmd_csv = 'echo \'show int ' + INTERFACE_LIST[i] + r' flows top ' + TOP_NUM + ' by average_rate\' | ' + STM_SCRIPT_PATH + ' ' + USERNAME + r':' + PASSWORD + r'@localhost'
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE, "get_nowdate() cannot be executed, {}".format(e))
        pass
    return [ save_txt_filepath, exec_cmd_txt, save_csv_filepath, exec_cmd_csv, save_csv_users_folder ]

# Logging helper
def logger(type, path, contents):
    current = datetime.today().strftime("%Y/%m/%d %H:%M:%S")
    if not (os.path.isfile(path)):
        csv_file = open(path, 'w')
        csv_file.close()
    try:
        if type == 0:	# INFO
            with open(path, "a") as log:
                log.write(current + " INFO : " + contents)
                log.write('\r\n')
        elif type == 1:	# ERROR
            with open(path, "a") as log:
                log.write(current + " ERROR : " + contents)
                log.write('\r\n')
    except Exception as e:
        with open(SCRIPT_MON_LOG_FILE, "a") as log:
            log.write("{} cannot logging; {}".format(current,e))
        pass

def main():
    while True:
#        if is_month_begin():
#            global archive_count
#            print ("archive last_month folder")
#            current_year, last_year, current_month, last_month = get_lastmonth()
#            diff_month = current_month - last_month
#            diff_year = current_year - last_year
#            if current_month >= 1 or current_month < 10:
#                current_month = '0'+str(current_month)
#            else:
#                current_month = str(current_month)
#            if last_month >= 1 or last_month < 10:
#                last_month = '0'+str(last_month)
#            else:
#                last_month = str(last_month)
#            current_year = str(current_year)
#            last_year = str(last_year)
#            archive_path = FLOW_USER_LOG_FOLDER + '/' + last_year + last_month
#            if archive_count == 1:
#                make_archive_logfolder(archive_path)
#                archive_count += 1
#        else:
#            print ("not archive last_month folder")
#            archive_count += 1
        curTime = get_nowdate() # curTime[0] - ymd, curTime[1] - ymd h:m:s
        foldername = parsedate(curTime[0])
        for i in range(len(INTERFACE_LIST)):
            file_paths = get_filepaths(foldername, i)	# Get list of filepaths[txtpath, txtcmd, csvpath, csvcmd]
            if RECORD_TYPE == 0:
                do_txt_log(file_paths[1], file_paths[0], foldername, curTime)	# cmd, path, foldername, date
            elif RECORD_TYPE == 1:
                do_csv_log(file_paths[3], file_paths[2], foldername, curTime)
            else:
                #do_txt_log(file_paths[1], file_paths[0], foldername, curTime)	# cmd, path, foldername, date
                #logger(0, SCRIPT_MON_LOG_FILE, 'Flow info from interfaces {} is extracted to {} successfully!'.format(INTERFACE_LIST[i], file_paths[0]))
                do_csv_log(file_paths[3], file_paths[2], file_paths[4], foldername, curTime)
                #logger(0, SCRIPT_MON_LOG_FILE, 'Flow info from interfaces {} is extracted to {} successfully!'.format(INTERFACE_LIST[i], file_paths[2]))
            time.sleep(INTERVAL)
        time.sleep(30)

foldername_init = parsedate(CURRENTTIME_INIT)
create_folder(foldername_init)

if __name__ == "__main__":
    main()
