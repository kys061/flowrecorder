#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Saisei Networks Inc. All rights reserved.

import sys
sys.path.append("/home/saisei/module")
from datetime import datetime, timedelta
import time
from flowrecorder import *


# Env for (eg. echo 'show int stm3 flows top 100 by average_rate' | ./stm_cli.py
# admin:admin@localhost)
D_INTERFACE_LIST = {
        'external':'stm9',          # Interface name
        'internal':'stm10'          # Interface name
}
TOP_NUM = '70'
ARRIVAL_RATE = '10'
USERNAME = 'admin'
PASSWORD = 'admin'
TOP_NUM_FOR_FIELD = '10'
STM_SCRIPT_PATH = r'/opt/stm/target/pcli/stm_cli.py'

# For parse_fieldname in order to reduce cpu loads.
INTERFACE_LIST = []
INTERFACE_LIST = list(D_INTERFACE_LIST.values())

CMD_FOR_FIELD = ["echo \'show int {} flows with arrival_rate > {} top {} by \
average_rate select distress geolocation autonomous_system \
retransmissions round_trip_time timeouts udp_jitter\' | {} \
{}:{}@localhost".format(interface, ARRIVAL_RATE, TOP_NUM_FOR_FIELD, STM_SCRIPT_PATH, USERNAME, PASSWORD)
for interface in INTERFACE_LIST]

CMD = []

for i in range(len(INTERFACE_LIST)):
        CMD.append("echo \'show int {} flows with arrival_rate > {} top {} by \
average_rate select distress geolocation autonomous_system \
retransmissions round_trip_time timeouts udp_jitter\' | \
{} {}:{}@localhost".format(INTERFACE_LIST[i], ARRIVAL_RATE,
                           TOP_NUM, STM_SCRIPT_PATH, USERNAME, PASSWORD))

# For several interfaces(eg. iterate logging for interfaces(stm01, stm02) sequently)
INTERVAL = 20
# Recording type selecting(0: only csv, 1: only txt, others:csv and txt)
RECORD_TYPE = 2
# Get current time
CURRENTTIME_INIT = datetime.today().strftime("%Y:%m:%d")

def main():
    while True:
        currenttime = datetime.today().strftime("%Y:%m:%d")
        foldername = parsedate(currenttime)
        logfolderpath = r'/var/log/flows/users/' + foldername[0] + foldername[1] + '/' + foldername[0] + foldername[1] + foldername[2] + r'-'
        for i in range(len(INTERFACE_LIST)):
            file_paths = get_filepaths(foldername, INTERFACE_LIST, TOP_NUM, i)   # Get list of filepaths[txtpath, csvpath]
            if RECORD_TYPE == 0:
                do_txt_log(file_paths[1], file_paths[0], foldername, curTime)   # cmd, path, foldername, date
            elif RECORD_TYPE == 1:
                do_csv_log(file_paths[3], file_paths[2], foldername, curTime)
            else:
                fr = Flowrecorder(CMD[i],
                                  D_INTERFACE_LIST,
                                  foldername,
                                  file_paths,
                                  logfolderpath,
                                  currenttime)
                fr.printall()
                fr.start_fr_txt()
                #fr.start_fr_by_host()
                #do_txt_log(file_paths[1], file_paths[0], foldername, curTime)  # cmd, path, foldername, date
                #logger(0, SCRIPT_MON_LOG_FILE, 'Flow info from interfaces {} is extracted to {} successfully!'.format(INTERFACE_LIST[i], file_paths[0]))
                ##do_csv_log(file_paths[3], file_paths[2], file_paths[4], foldername, curTime)
                #logger(0, SCRIPT_MON_LOG_FILE, 'Flow info from interfaces {} is extracted to {} successfully!'.format(INTERFACE_LIST[i], file_paths[2]))
            time.sleep(INTERVAL)
        time.sleep(30)

foldername_init = parsedate(CURRENTTIME_INIT)
create_folder(foldername_init)

if __name__ == "__main__":
    main()
