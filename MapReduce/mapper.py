#!/usr/bin/python

import sys
import time
no_of_lines = 0
# input comes from STDIN (standard input)
print ("%s\t%s" % ('starttime', time.time()))
for index, line in enumerate(sys.stdin):
    # if index == 0:
    #     print ("%s\t%s" % (time.time(), 'value'))
    #     continue
    # if index < 6700:
    #     continue
    line = line.strip()
    line = line.split(";")
    if len(line) >= 2:
        if line[2] == '?':
            continue
        hour_of_day = line[1]
        hour_of_day = hour_of_day.strip()
        hour_of_day = hour_of_day.split(":")[0]
        active_power = line[2]
        no_of_lines+=1
        print '%s\t%s' % (hour_of_day, active_power)

print '%s\t%s' % ("noOfLines", no_of_lines)
