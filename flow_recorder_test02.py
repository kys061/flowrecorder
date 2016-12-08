#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Saisei Networks Inc. All rights reserved.

import sys
from datetime import datetime
import time
from collections import defaultdict
sys.path.append("/root/dev/flow_recorder_module")
from flow_recorder_mod import *
from SubnetTree import SubnetTree
################################################################################
# Env for (eg. echo 'show int stm3 flows top 100 by average_rate' | ./stm_cli.py
# admin:admin@localhost)
################################################################################
DD_INTERFACE_LIST = [
    ('external' , 'stm13'),
    ('external' , 'stm5'),
    ('internal' , 'stm14'),
    ('internal' , 'stm6')
]

# for cmd type is 1 :all users and do log for INCLUDE subnets
INCLUDE = [
    '101.250.160.0/19',
    '101.250.192.0/20',
    '101.250.208.0/20',
    '101.250.224.0/19',
    '103.11.44.0/22',
    '150.107.80.0/22',
    '115.126.192.0/18',
    '118.103.192.0/19',
    '183.91.192.0/18',
    '203.128.160.0/19',
    '203.212.96.0/19',
    '210.111.160.0/19',
    '211.112.64.0/19',
    '211.235.32.0/19',
    '211.238.64.0/19'
]
# for cmd type is 2 : one user by src or dst so host must be in internal iprange
HOST = [
    '211.112.79.145',
    '203.212.114.153'
]
TOP_NUM = '60'
ARRIVAL_RATE = '10'
DOMAIN = 'localhost'
USERNAME = 'admin'
PASSWORD = 'admin'
STM_SCRIPT_PATH = r'/opt/stm/target/pcli/stm_cli.py'
# For several interfaces(eg. iterate logging for interfaces(stm01, stm02) sequently)
INTERVAL = 5
TYPE_INTERVAL = 1 # for use if recording cmd type is 3(:all of them)
# Recording file type selecting(0: only csv, 1: only txt, 2:csv and txt)
RECORD_FILE_TYPE = 2
# Recording cmd type selecting(0: total, 1: all users, 2:one user by src or dst, 3: all of them)
'''
0(flows of total) : extracting all flows in cmd.
1(flows of all users) : extracting all flows and user's ip in INCLUDE.
2(flows of user by src or dst) : extracting user's ip in HOST.
3(all of them) : extracting all above explained
'''
RECORD_CMD_TYPE = 3
# Get current time
CURRENTTIME_INIT = datetime.today().strftime("%Y:%m:%d")
################################################################################

################################################################################
# For parse_fieldname in order to reduce cpu loads.
################################################################################
INTERFACE_LIST = []
#INTERFACE_LIST = list(D_INTERFACE_LIST.values())
#
D_INTERFACE_LIST = defaultdict(list)
for k,v in DD_INTERFACE_LIST:
    D_INTERFACE_LIST[k].append(v)

for i,v in enumerate(D_INTERFACE_LIST.values()):
    for j,val in enumerate(v):
        INTERFACE_LIST.append(val)
#INTERFACE_LIST = [ val for j, val in enumerate(v) for i, v in enumerate(D_INTERFACE_LIST.values()) ]

# RECORD_CMD_TYPE:0 and 1
CMD = []
for i in range(len(INTERFACE_LIST)):
    CMD.append("echo \'show int {} flows with arrival_rate > {} top {} by \
average_rate select distress geolocation autonomous_system \
retransmissions round_trip_time timeouts udp_jitter\' |sudo \
{} {}:{}@{}".format(INTERFACE_LIST[i], ARRIVAL_RATE,
                    TOP_NUM, STM_SCRIPT_PATH, USERNAME, PASSWORD, DOMAIN))
# RECORD_CMD_TYPE:2
CMD_BY_SOURCEHOST = []
for host in HOST:
    for intf in INTERFACE_LIST:
        for d_intf in D_INTERFACE_LIST['internal']:
            if intf == d_intf:
                CMD_BY_SOURCEHOST.append("echo \'show interface {} flows with source_host={} \
application=http arrival_rate > {} top {} by average_rate select geolocation \
autonomous_system  retransmissions round_trip_time timeouts' |sudo {} \
{}:{}@{}".format(intf, host, ARRIVAL_RATE, TOP_NUM, STM_SCRIPT_PATH,
                    USERNAME, PASSWORD, DOMAIN))
# RECORD_CMD_TYPE:2
CMD_BY_DESTHOST = []
for host in HOST:
    for intf in INTERFACE_LIST:
        for d_intf in D_INTERFACE_LIST['external']:
            if intf == d_intf:
                CMD_BY_DESTHOST.append("echo \'show interface {} flows with dest_host={} \
application=http arrival_rate > {} top {} by average_rate select geolocation \
autonomous_system  retransmissions round_trip_time timeouts' |sudo {} \
{}:{}@{}".format(intf, host, ARRIVAL_RATE, TOP_NUM, STM_SCRIPT_PATH,
                    USERNAME, PASSWORD, DOMAIN))
################################################################################

################################################################################
def main():
    while True:
        try:
            current_time = datetime.today().strftime("%Y:%m:%d")
            foldername = parsedate(current_time)
            #logfolderpath = r'/var/log/flows/users/' + foldername[0] + foldername[1] + '/' + foldername[0] + foldername[1] + foldername[2] + r'-'
            logfolderpath = r'/var/log/flows/users/' + foldername[0] + foldername[1] + '/'  # redmine #2
            #do total
            if RECORD_CMD_TYPE == 0:
                for i in range(len(INTERFACE_LIST)):
                    file_paths = get_filepaths(foldername, INTERFACE_LIST, TOP_NUM, i)   # Get list of filepaths[txtpath, csvpath]
                    if RECORD_FILE_TYPE == 0:
                        fr_total = Flowrecorder(CMD[i],
                                          D_INTERFACE_LIST,
                                          foldername,
                                          file_paths,
                                          logfolderpath,
                                          include_subnet_tree)
                        fr_total.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(INTERVAL)
                    elif RECORD_FILE_TYPE == 1:
                        fr_total = Flowrecorder(CMD[i],
                                          D_INTERFACE_LIST,
                                          foldername,
                                          file_paths,
                                          logfolderpath,
                                          include_subnet_tree)
                        fr_total.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(INTERVAL)
                    elif RECORD_FILE_TYPE == 2:
                        fr_total = Flowrecorder(CMD[i],
                                          D_INTERFACE_LIST,
                                          foldername,
                                          file_paths,
                                          logfolderpath,
                                          include_subnet_tree)
                        fr_total.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(INTERVAL)
                    else:
                        pass
            #do allusers
            elif RECORD_CMD_TYPE == 1:
                for j in range(len(INTERFACE_LIST)):
                    file_paths = get_filepaths(foldername, INTERFACE_LIST, TOP_NUM, j)
                    fr_allusers = Flowrecorder(CMD[j],
                                            D_INTERFACE_LIST,
                                            foldername,
                                            file_paths,
                                            logfolderpath,
                                            include_subnet_tree)
                    fr_allusers.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                    time.sleep(INTERVAL)
            #do one user
            elif RECORD_CMD_TYPE == 2:
                for k in range(len(CMD_BY_SOURCEHOST)):
                    file_paths = get_filepaths(foldername, INTERFACE_LIST, TOP_NUM, k)
                    if RECORD_FILE_TYPE == 0:
                        fr_by_srchost = Flowrecorder(CMD_BY_SOURCEHOST[k],
                                                     D_INTERFACE_LIST,
                                                     foldername,
                                                     file_paths,
                                                     logfolderpath,
                                                     include_subnet_tree)
                        fr_by_srchost.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(INTERVAL)
                    elif RECORD_FILE_TYPE == 1:
                        fr_by_srchost = Flowrecorder(CMD_BY_SOURCEHOST[k],
                                                     D_INTERFACE_LIST,
                                                     foldername,
                                                     file_paths,
                                                     logfolderpath,
                                                     include_subnet_tree)
                        fr_by_srchost.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(INTERVAL)
                    elif RECORD_FILE_TYPE == 2:
                        fr_by_srchost = Flowrecorder(CMD_BY_SOURCEHOST[k],
                                                     D_INTERFACE_LIST,
                                                     foldername,
                                                     file_paths,
                                                     logfolderpath,
                                                     include_subnet_tree)
                        fr_by_srchost.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(INTERVAL)
                    else:
                        pass
                for l in range(len(CMD_BY_DESTHOST)):
                    file_paths = get_filepaths(foldername, INTERFACE_LIST, TOP_NUM, l)
                    if RECORD_FILE_TYPE == 0:
                        fr_by_dsthost = Flowrecorder(CMD_BY_DESTHOST[l],
                                                     D_INTERFACE_LIST,
                                                     foldername,
                                                     file_paths,
                                                     logfolderpath,
                                                     include_subnet_tree)
                        fr_by_dsthost.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(INTERVAL)
                    elif RECORD_FILE_TYPE == 1:
                        fr_by_dsthost = Flowrecorder(CMD_BY_DESTHOST[l],
                                                     D_INTERFACE_LIST,
                                                     foldername,
                                                     file_paths,
                                                     logfolderpath,
                                                     include_subnet_tree)
                        fr_by_dsthost.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(INTERVAL)
                    elif RECORD_FILE_TYPE == 2:
                        fr_by_dsthost = Flowrecorder(CMD_BY_DESTHOST[l],
                                                     D_INTERFACE_LIST,
                                                     foldername,
                                                     file_paths,
                                                     logfolderpath,
                                                     include_subnet_tree)
                        fr_by_dsthost.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(INTERVAL)
                    else:
                        pass
            # all of them
            elif RECORD_CMD_TYPE == 3:
                # total
                for i in range(len(INTERFACE_LIST)):
                    file_paths = get_filepaths(foldername, INTERFACE_LIST, TOP_NUM, i)   # Get list of filepaths[txtpath, csvpath]
                    if RECORD_FILE_TYPE == 0:
                        fr_total = Flowrecorder(CMD[i],
                                          D_INTERFACE_LIST,
                                          foldername,
                                          file_paths,
                                          logfolderpath,
                                          include_subnet_tree)
                        fr_total.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(TYPE_INTERVAL)
                    elif RECORD_FILE_TYPE == 1:
                        fr_total = Flowrecorder(CMD[i],
                                          D_INTERFACE_LIST,
                                          foldername,
                                          file_paths,
                                          logfolderpath,
                                          include_subnet_tree)
                        fr_total.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(TYPE_INTERVAL)
                    elif RECORD_FILE_TYPE == 2:
                        fr_total = Flowrecorder(CMD[i],
                                          D_INTERFACE_LIST,
                                          foldername,
                                          file_paths,
                                          logfolderpath,
                                          include_subnet_tree)
                        fr_total.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(TYPE_INTERVAL)
                        fr_total.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(TYPE_INTERVAL)
                    else:
                        pass
                # all users
                for j in range(len(INTERFACE_LIST)):
                    file_paths = get_filepaths(foldername, INTERFACE_LIST, TOP_NUM, j)
                    fr_allusers = Flowrecorder(CMD[j],
                                            D_INTERFACE_LIST,
                                            foldername,
                                            file_paths,
                                            logfolderpath,
                                            include_subnet_tree)
                    fr_allusers.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                    time.sleep(TYPE_INTERVAL)
                #do one user
                for k in range(len(CMD_BY_SOURCEHOST)):
                    file_paths = get_filepaths(foldername, INTERFACE_LIST, TOP_NUM, k)
                    if RECORD_FILE_TYPE == 0:
                        fr_by_srchost = Flowrecorder(CMD_BY_SOURCEHOST[k],
                                                     D_INTERFACE_LIST,
                                                     foldername,
                                                     file_paths,
                                                     logfolderpath,
                                                     include_subnet_tree)
                        fr_by_srchost.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(TYPE_INTERVAL)
                    elif RECORD_FILE_TYPE == 1:
                        fr_by_srchost = Flowrecorder(CMD_BY_SOURCEHOST[k],
                                                     D_INTERFACE_LIST,
                                                     foldername,
                                                     file_paths,
                                                     logfolderpath,
                                                     include_subnet_tree)
                        fr_by_srchost.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(TYPE_INTERVAL)
                    elif RECORD_FILE_TYPE == 2:
                        fr_by_srchost = Flowrecorder(CMD_BY_SOURCEHOST[k],
                                                     D_INTERFACE_LIST,
                                                     foldername,
                                                     file_paths,
                                                     logfolderpath,
                                                     include_subnet_tree)
                        fr_by_srchost.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(TYPE_INTERVAL)
                        fr_by_srchost.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(TYPE_INTERVAL)
                    else:
                        pass
                for l in range(len(CMD_BY_DESTHOST)):
                    file_paths = get_filepaths(foldername, INTERFACE_LIST, TOP_NUM, l)
                    if RECORD_FILE_TYPE == 0:
                        fr_by_dsthost = Flowrecorder(CMD_BY_DESTHOST[l],
                                                     D_INTERFACE_LIST,
                                                     foldername,
                                                     file_paths,
                                                     logfolderpath,
                                                     include_subnet_tree)
                        fr_by_dsthost.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(TYPE_INTERVAL)
                    elif RECORD_FILE_TYPE == 1:
                        fr_by_dsthost = Flowrecorder(CMD_BY_DESTHOST[l],
                                                     D_INTERFACE_LIST,
                                                     foldername,
                                                     file_paths,
                                                     logfolderpath,
                                                     include_subnet_tree)
                        fr_by_dsthost.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(TYPE_INTERVAL)
                    elif RECORD_FILE_TYPE == 2:
                        fr_by_dsthost = Flowrecorder(CMD_BY_DESTHOST[l],
                                                     D_INTERFACE_LIST,
                                                     foldername,
                                                     file_paths,
                                                     logfolderpath,
                                                     include_subnet_tree)
                        fr_by_dsthost.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(TYPE_INTERVAL)
                        fr_by_dsthost.start(RECORD_FILE_TYPE, RECORD_CMD_TYPE)
                        time.sleep(TYPE_INTERVAL)
                    else:
                        pass
            else:
                pass
            time.sleep(30)
        except KeyboardInterrupt:
            print ("\r\nThe script is terminated by user interrupt!")
            print ("Bye!!")
            sys.exit()
################################################################################
foldername_init = parsedate(CURRENTTIME_INIT)
create_folder(foldername_init)
# make subnet tree for INCLUDE
try:
    include_subnet_tree = SubnetTree()
    for subnet in INCLUDE:
        include_subnet_tree[subnet] = str(subnet)
except Exception as e:
    pass
################################################################################
if __name__ == "__main__":
    main()
