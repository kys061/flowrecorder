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

''' Env for (eg. echo 'show int stm3 flows top 100 by average_rate' |'''
'''./stm_cli.py admin:admin@localhost)'''
INTERFACE_LIST = [
    'stm9',		# Interface name
    'stm10'		# Interface name
]
TOP_NUM = '80000'
ARRIVAL_RATE = '10'
USERNAME = 'admin'
PASSWORD = 'admin'
TOP_NUM_FOR_FIELD = '10'

# Init path and filename
FLOW_LOG_FOLDER_PATH = r'/var/log/flows'
SCRIPT_MON_LOG_FILE = r'/var/log/flow_recorder.log'
SCRIPT_MON_LOG_FOLDER = r'/var/log'
STM_SCRIPT_PATH = r'/opt/stm/target/pcli/stm_cli.py'

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

# Get last month
'''Return int'''
def get_lastmonth():
    first_day_of_current_month = datetime.today().replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    return (first_day_of_current_month.month, last_day_of_previous_month.month)

# Parse the current date
def parsedate(today_date):
    try:
        parseDate = today_date.split(':')
        year = parseDate[0]
        month = parseDate[1]
        day = parseDate[2]
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE,
               "parsedate() cannot be executed, {}".format(e))
        pass
    return [year, month, day]

def parse_fieldnames(data):
    try:
        str_fieldnames = ''
        for i in range(1):
            str_fieldnames = data.splitlines()[1]
        fieldname = str_fieldnames.split()
        fieldnames = []
        for field in fieldname:
            fieldnames.append(re.sub('\"|\'', "", field))
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE,
               "parsedate() cannot be executed, {}".format(e))
        pass
    return fieldnames
# Parse the data from the command
def parse_csv(csv_data_row, csv_filepath):
    try:
        pattern = r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
        pattern_01 = r'[-]{1,10}'
        pattern_03 = r'\s+\n'
        m = re.search(pattern, csv_data_row)
        startidx = m.start()
        endidx = m.end()
        csv_time = csv_data_row[startidx:endidx]

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
        reader = csv.DictReader(itertools.islice(csv_data_row.splitlines(),
                                                 1, None),
                                delimiter=' ',
                                skipinitialspace=True,
                                fieldnames=fieldnames)
        for row in reader:
            with open(csv_filepath, "a") as output:
                output.write('{},'.format(csv_time))
                writer = csv.DictWriter(f=output, fieldnames=reader.fieldnames)
                writer.writerow(row)
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE,
               "parse_csv() cannot be executed, {}".format(e))
        pass
    return 0
# Excute command in shell
def subprocess_open(command):
    try:
        popen = subprocess.Popen(command, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, shell=True)
        (stdoutdata, stderrdata) = popen.communicate()
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE,
               "subprocess_open() cannot be executed, {}".format(e))
        pass
    return stdoutdata, stderrdata
# Create log folder by YearMon
def create_folder(foldername):
    try:
        folder_year_mon = FLOW_LOG_FOLDER_PATH+'/'+foldername[0]+foldername[1]
        if not os.path.exists(folder_year_mon):
            os.makedirs(folder_year_mon)
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE,
               "create_folder() cannot be executed, {}".format(e))
        pass

# Write the txt type log from the command
def do_txt_log(cmd_txt, save_filepath, foldername, curTime, i):
    try:
        pattern = r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
        pattern_01 = r'[-]{1,10}'
        pattern_03 = r'\s+\n'
        pattern_04 = r'Flows at [0-9]{4}-[0-9]{2}-[0-9]{2} \
                        [0-9]{2}:[0-9]{2}:[0-9]{2}'
        raw_txt_data = subprocess_open(CMD_FOR_FIELD)
        fieldnames = parse_fieldnames(raw_txt_data[0])

        pattern_02 = ''
        for field in fieldnames:
            pattern_02 += field+"|"

        if not (os.path.isfile(save_filepath)):
            create_folder(foldername)
            txt_file = open(save_filepath, 'w')
            txt_file.close()
            fieldnames.insert(0, 'timestamp')
            proc = subprocess.Popen(cmd_txt, shell=True,
                                    stdout=subprocess.PIPE, bufsize=1)
            with proc.stdout:
                for line in iter(proc.stdout.readline, b''):
                    if re.search(pattern, line):
                        m = re.search(pattern, line)
                        startidx = m.start()
                        endidx = m.end()
                        csv_time = line[startidx:endidx]
                    line = re.sub(pattern_04, "", line)
                    line = re.sub(pattern_01, "", line)
#                    line = re.sub(pattern_02, "", line)
                    line = re.sub('"+', "", line)
                    line = re.sub(pattern_03, "", line)
                    # ERR Routine
                    if 'Cannot connect to server' in line:
                        logger(1, SCRIPT_MON_LOG_FILE, "{}".format(line))
                    elif 'does not exist' in line:
                        logger(1, SCRIPT_MON_LOG_FILE, "{}".format(line))
                    elif 'no matching objects' in line:
                        logger(1, SCRIPT_MON_LOG_FILE, "{}".format(line))
                    elif line != '' and line != '\n':
                        with open(save_filepath, "a") as output:
                            if not fieldnames[1] in line:
                                output.write('{}\t'.format(csv_time))
                                output.write('{}'.format(line))
                            else:
                                output.write('timestamp\t\t')
                                output.write('{}'.format(line))
                    else:
                        pass

        else:
            proc = subprocess.Popen(cmd_txt, shell=True,
                                    stdout=subprocess.PIPE, bufsize=1)
            with proc.stdout:
                for line in iter(proc.stdout.readline, b''):
                    if re.search(pattern, line):
                        m = re.search(pattern, line)
                        startidx = m.start()
                        endidx = m.end()
                        csv_time = line[startidx:endidx]
                    line = re.sub(pattern_04, "", line)
                    line = re.sub(pattern_01, "", line)
                    line = re.sub(pattern_02, "", line)
                    line = re.sub('"+', "", line)
                    line = re.sub(pattern_03, "", line)
                    # ER Routine
                    if 'Cannot connect to server' in line:
                        logger(1, SCRIPT_MON_LOG_FILE, "{}".format(line))
                    elif 'does not exist' in line:
                        logger(1, SCRIPT_MON_LOG_FILE, "{}".format(line))
                    elif 'no matching objects' in line:
                        logger(1, SCRIPT_MON_LOG_FILE, "{}".format(line))
                    elif line != '' and line != '\n':
                    #re.sub(pattern_03, "\r\n", csv_data_row)
                        with open(save_filepath, "a") as output:
                            output.write('{}\t'.format(csv_time))
                            output.write('{}'.format(line))
                    else:
                        pass
            logger(0, SCRIPT_MON_LOG_FILE,
                   'Flow info from interfaces {} is extracted to {} \
successfully!'.format(INTERFACE_LIST[i], save_filepath))
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE,
               "do_txt_log() cannot be executed, {}".format(e))
        pass

# Write the csv type log from the command
def do_csv_log(cmd_csv, save_csv_filepath, foldername, curTime, i):
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
                parse_csv(raw_csv_data[0], save_csv_filepath)
                logger(0, SCRIPT_MON_LOG_FILE, 'Flow info from interfaces {} \
is extracted to {} successfully!'.format(INTERFACE_LIST[i], save_csv_filepath))
        else:
            raw_csv_data = subprocess_open(cmd_csv)
            if 'Cannot connect to server' in raw_csv_data[0]:
                logger(1, SCRIPT_MON_LOG_FILE, "{}".format(raw_csv_data[0]))
            elif 'does not exist' in raw_csv_data[0]:
                logger(1, SCRIPT_MON_LOG_FILE, "{}".format(raw_csv_data[0]))
            elif 'no matching objects' in raw_csv_data[0]:
                logger(1, SCRIPT_MON_LOG_FILE, "{}".format(raw_csv_data[0]))
            else:
                parse_csv(raw_csv_data[0], save_csv_filepath)
                logger(0, SCRIPT_MON_LOG_FILE, 'Flow info from interfaces {} \
is extracted to {} successfully!'.format(INTERFACE_LIST[i], save_csv_filepath))
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE,
               "do_csv_log() cannot be executed, {}".format(e))
        pass

# Get current date
def get_nowdate():
    try:
        nowdate = datetime.today().strftime("%Y:%m:%d")
        nowdatetime = datetime.today().strftime("%Y/%m/%d %H:%M:%S")
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE,
               "get_nowdate() cannot be executed, {}".format(e))
        pass
    return [nowdate, nowdatetime]

# Get filepath and command string
def get_filepaths(foldername, i):
    try:
        save_txt_filepath = FLOW_LOG_FOLDER_PATH + '/' + foldername[0] + foldername[1]\
                            + '/' + foldername[0] + foldername[1] + foldername[2] + \
                            r'_flowinfo_' + INTERFACE_LIST[i] + r'_' + TOP_NUM + r'.txt'

        save_csv_filepath = FLOW_LOG_FOLDER_PATH + '/' + foldername[0] + foldername[1]\
                            + '/' + foldername[0] + foldername[1] + foldername[2] + \
                            r'_flowinfo_' + INTERFACE_LIST[i] + r'_' + TOP_NUM + r'.csv'

        exec_cmd_txt = 'echo \'show int ' + INTERFACE_LIST[i] + \
                        r' flows with arrival_rate > ' + ARRIVAL_RATE + ' top ' + \
                        TOP_NUM + ' by average_rate select distress geolocation \
                        autonomous_system  retransmissions round_trip_time \
                        timeouts udp_jitter\' | ' + STM_SCRIPT_PATH + ' ' + USERNAME + \
                        r':' + PASSWORD + r'@localhost >> ' + save_txt_filepath

        exec_cmd_csv = 'echo \'show int ' + INTERFACE_LIST[i] + \
                        r' flows with arrival_rate > ' + ARRIVAL_RATE + ' top ' + \
                        TOP_NUM + ' by average_rate select distress geolocation \
                        autonomous_system  retransmissions round_trip_time \
                        timeouts udp_jitter\' | ' + STM_SCRIPT_PATH + ' ' + USERNAME + \
                        r':' + PASSWORD + r'@localhost'
    except Exception as e:
        logger(1, SCRIPT_MON_LOG_FILE,
               "get_nowdate() cannot be executed, {}".format(e))
        pass
    return [ save_txt_filepath, exec_cmd_txt, save_csv_filepath, exec_cmd_csv ]

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
        # curTime[0] - ymd, curTime[1] - ymd h:m:s
        #current_month, last_month = get_lastmonth()
        #print ("current {}, last {}".format(current_month, last_month))
        curTime = get_nowdate()
        foldername = parsedate(curTime[0])
        for i in range(len(INTERFACE_LIST)):
            # Get list of filepaths[txtpath, txtcmd, csvpath, csvcmd]
            file_paths = get_filepaths(foldername, i)
            if RECORD_TYPE == 0:
                #do_csv_log(file_paths[3], file_paths[2], foldername, curTime, i)
                do_csv_log(CMD[i], file_paths[2], foldername, curTime, i)
            elif RECORD_TYPE == 1:
                #do_txt_log(file_paths[1], file_paths[0], foldername, curTime, i)
                # cmd, path, foldername, date, i
                do_txt_log(CMD[i], file_paths[0], foldername, curTime, i)
            else:
                do_txt_log(CMD[i], file_paths[0], foldername, curTime, i)
#                    logger(0, SCRIPT_MON_LOG_FILE,
#                            'Flow info from interfaces {} is extracted to {} \
#                            successfully!'.format(INTERFACE_LIST[i],
#                                                    file_paths[0]))
                do_csv_log(CMD[i], file_paths[2], foldername, curTime, i)
#                    logger(0, SCRIPT_MON_LOG_FILE,
#                            'Flow info from interfaces {} is extracted to {} \
#                            successfully!'.format(INTERFACE_LIST[i],
#                                                    file_paths[2]))
            time.sleep(INTERVAL+1)
        time.sleep(31)


foldername_init = parsedate(CURRENTTIME_INIT)
create_folder(foldername_init)

if __name__ == "__main__":
    main()
