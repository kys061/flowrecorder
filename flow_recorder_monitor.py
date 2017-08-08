#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Saisei Networks Inc. All rights reserved.

import sys
import time
#sys.path.append("/home/saisei/dev/flow_recorder_module")
from flow_recorder_mod import *

################################################################################
# Config
################################################################################
CHECK_TIME = 60	# seconds
has_archive = False
do_compress = False
archive_period = 3  # set month period
time.sleep(30)
################################################################################

################################################################################
def main():
    while True:
        try:
            curTime = get_nowdate() # [y:m:d, y/m/d h:m:s]
            recorder_process_count = get_process_count(RECORDER_SCRIPT_FILENAME)
            monitor_process_count = get_process_count(MONITOR_SCRIPT_FILENAME)
            compare_process_count(curTime, SCRIPT_FILENAME, recorder_process_count, monitor_process_count)
            archive_rotate(do_compress, archive_period)
            time.sleep(CHECK_TIME)
        except KeyboardInterrupt:
            print ("\r\nThe script is terminated by user interrupt!")
            print ("Bye!!")
            sys.exit()
#            stored_exception=sys.exc_info()

if __name__ == "__main__":
    main()
