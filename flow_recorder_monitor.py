#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Saisei Networks Inc. All rights reserved.

import os
import shutil
import subprocess
from datetime import datetime
import time
import logging

#
# Constant definitions
#
FLOW_LOG_FOLDER_PATH = r'/var/log/flows/'
SCRIPT_MON_LOG_FILE = r'/var/log/flow_recorder.log'
SCRIPT_MON_LOG_FOLDER = r'/var/log/'
SCRIPT_PATH = r'/etc/stmfiles/files/scripts/'
SCRIPT_FILENAME = r'flow_recorder_test.py'
RECORDER_SCRIPT_FILENAME = r'flow_recorder_test.py'
MONITOR_SCRIPT_FILENAME = r'flow_recorder_monitor.py'
MON_LOG_FILENAME = r'flow_recorder.log'
LOGSIZE = 50000000 # 1000 = 1Kbyte, 1000000 = 1Mbyte, 50000000 = 50Mbyte
CHECK_TIME = 60	# seconds

# logger setting
Logger = logging.getLogger('saisei.flow.monitor')
Logger.setLevel(logging.INFO)
handler = logging.FileHandler(SCRIPT_MON_LOG_FILE)
handler.setLevel(logging.INFO)
filter = logging.Filter('saisei.flow')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

handler.setFormatter(formatter)
handler.addFilter(filter)
Logger.addHandler(handler)
Logger.addFilter(filter)

# Excute command in shell.
def subprocess_open(command):
    try:
        popen = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (stdoutdata, stderrdata) = popen.communicate()
    except Exception as e:
        Logger.error("subprocess_open() cannot be executed, {}".format(e))
        pass
    return stdoutdata, stderrdata

# Find process with process name.
def find_process(process_name):
    try:
        cmd_getpid = "ps -ef |grep "+process_name+" | grep -v grep |wc -l"
        ps = subprocess_open(cmd_getpid)
    except Exception as e:
        Logger.error("find_process() cannot be executed, {}".format(e))
        pass
    return ps

# Parse current date as LIST.
def parsedate(today_date):
    try:
        parseDate = today_date.split(':')
        year = parseDate[0]
        month = parseDate[1]
        day = parseDate[2]
    except Exception as e:
        Logger.error("parsedate() cannot be executed, {}".format(e))
        pass
    return [year, month, day]

# Get logsize of var(SCRIPT_MON_LOG_FOLDER).
def get_logsize():
    try:
        #current = datetime.today().strftime("%Y:%m:%d")
        #foldername = parsedate(current)
        cmd_get_monitorlog_size = "ls -al " + SCRIPT_MON_LOG_FOLDER + " | egrep \'" + MON_LOG_FILENAME + "$\' |awk \'{print $5}\'"
        monlog_size = subprocess_open(cmd_get_monitorlog_size)
        monlog_size_int = int(monlog_size[0])
    except Exception as e:
        Logger.error("get_logsize() cannot be executed, {}".format(e))
        pass
    return monlog_size_int

#  Rotate logfile when logsize is bigger thant var(LOGSIZE).
def logrotate(logfilepath, logsize):
    try:
        if os.path.isfile(logfilepath+".5"):
            os.remove(logfilepath+".5")
        if os.path.isfile(logfilepath+".4"):
            shutil.copyfile(logfilepath + r'.4', logfilepath + r'.5')
        if os.path.isfile(logfilepath+".3"):
            shutil.copyfile(logfilepath + r'.3', logfilepath + r'.4')
        if os.path.isfile(logfilepath+".2"):
            shutil.copyfile(logfilepath + r'.2', logfilepath + r'.3')
        if os.path.isfile(logfilepath+".1"):
            shutil.copyfile(logfilepath + r'.1', logfilepath + r'.2')
        if os.path.isfile(logfilepath):
            os.rename(logfilepath, logfilepath + r'.1')
        if not os.path.isfile(logfilepath):
            err_file = open(logfilepath, 'w')
            err_file.close()
            Logger.info("File is generated again because of size({})".format(str(logsize)))
    except Exception as e:
        Logger.error("logrotate() cannot be executed, {}".format(e))
        pass

# Execute flow_recorder.py script.
def do_flow_recorder(script_name_path, curTime, process_name):
    try:
        cmd = script_name_path + " &"
        subprocess.Popen(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=True)
        Logger.info("{} process is restarting! check ps -ef |grep {}".format(process_name, process_name))
    except Exception as e:
        Logger.error("do_flow_recorder() cannot be executed, {}".format(e))
        pass

# Get current date as LIST(y:m:d, y/m/d h:m:s).
def get_nowdate():
    try:
        nowdate = datetime.today().strftime("%Y:%m:%d")
        nowdatetime = datetime.today().strftime("%Y/%m/%d %H:%M:%S")
    except Exception as e:
        Logger.error("get_nowdate() cannot be executed, {}".format(e))
        pass
    return [nowdate, nowdatetime]

# Get process count by process name.
def get_process_count(process_name):
    try:
        output = find_process(process_name)
        result = output[0]
    except Exception as e:
        Logger.error("get_process_count() cannot be executed, {}".format(e))
        pass
    return result

# Check if current process is working or not.
def compare_process_count(curTime, process_name, recorder_process_count, monitor_process_count):
    try:
        if recorder_process_count == "1\n":
            if not os.path.isfile(SCRIPT_MON_LOG_FILE):
                err_file = open(SCRIPT_MON_LOG_FILE, 'w')
                err_file.close()
                Logger.info("Flow {} script is started".format(SCRIPT_FILENAME))
            else:
                Logger.info("{} Process is running.".format(process_name))
                monlog_size = get_logsize()
                if monlog_size > LOGSIZE:
                    logrotate(SCRIPT_MON_LOG_FILE, monlog_size)
        elif recorder_process_count == "0\n":
            if not os.path.isfile(SCRIPT_MON_LOG_FILE):
                err_file = open(SCRIPT_MON_LOG_FILE, 'w')
                err_file.close()
                Logger.info("Flow {} script is not started".format(SCRIPT_FILENAME))
                Logger.info("Flow process is not started")
                do_flow_recorder(SCRIPT_PATH+SCRIPT_FILENAME, curTime[1], process_name)
                Logger.info("Flow {} script is started".format(SCRIPT_FILENAME))
                Logger.info("Flow {} Process was restarted.".format(SCRIPT_FILENAME))
            else:
                monlog_size = get_logsize()
                if monlog_size > LOGSIZE:
                    logrotate(monlog_size)
                Logger.info("Flow {} process is not running, will restart it".format(SCRIPT_FILENAME))
                do_flow_recorder(SCRIPT_PATH+SCRIPT_FILENAME, curTime[1], process_name)
                Logger.info("Flow {} process was restarted.".format(SCRIPT_FILENAME))
        else:
            pass
    except Exception as e:
        Logger.error("compare_process_count() cannot be executed, {}".format(e))
        pass

# Logging Helper.(type 0 is info, 1 is err)
def logger(type, path, contents):
    current = datetime.today().strftime("%Y/%m/%d %H:%M:%S")
    if not (os.path.isfile(path)):
        csv_file = open(path, 'w')
        csv_file.close()
    try:
        if type == 0:
            with open(path, "a") as fh:
                fh.write(current + " INFO : " + contents)
                fh.write('\r\n')
        elif type == 1:
            with open(path, "a") as fh:
                fh.write(current + " ERROR : " + contents)
                fh.write('\r\n')
    except Exception as e:
        with open(SCRIPT_MON_LOG_FILE, "a") as fh:
            fh.write("{} cannot logging; {}".format(current,e))
            pass

def main():
    while True:
        curTime = get_nowdate() # [y:m:d, y/m/d h:m:s]
        recorder_process_count = get_process_count(RECORDER_SCRIPT_FILENAME)
        monitor_process_count = get_process_count(MONITOR_SCRIPT_FILENAME)
        compare_process_count(curTime, SCRIPT_FILENAME, recorder_process_count, monitor_process_count)
        time.sleep(CHECK_TIME)

if __name__ == "__main__":
    main()
