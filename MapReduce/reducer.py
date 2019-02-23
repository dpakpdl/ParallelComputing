#!/usr/bin/python
# Reducer.py
import sys
import time

time_power = {}

# Partitoner
for line in sys.stdin:
    line = line.strip()
    hour_of_day, power = line.split('\t')
    if hour_of_day=="starttime":
        starttime = power
        continue
    if hour_of_day =="noOfLines":
        num_of_lines = power
        continue
    if hour_of_day in time_power:
        time_power[hour_of_day].append(float(power))
    else:
        time_power[hour_of_day] = []
        time_power[hour_of_day].append(float(power))

# Reducer
for hour_of_day in time_power.keys():
    average_power = sum(time_power[hour_of_day]) * 1.0 /float(num_of_lines)
    print '%s\t%s' % (hour_of_day, average_power)

endtime = time.time()
# print ("Endtime: %s" %endtime)
print ("time taken: %s"%(endtime-float(starttime)))