final_data = dict()
no_of_lines = 0
with open("/Users/deepakpaudel/mycodes/ParallelComputing/dataset/household_power_consumption1.txt", 'r') as infile:
    for line in infile:
        line_ = line.strip()
        line__ = line_.split(";")
        hour = line__[1].split(":")[0]
        active_power = line__[2]
        if hour in final_data:
            final_data[hour].append(float(active_power))
        else:
            final_data[hour] = list()
            final_data[hour].append(float(active_power))
        no_of_lines+=1

print no_of_lines
for hour_of_day in final_data.keys():
    average_power = sum(final_data[hour_of_day]) * 1.0 / no_of_lines
    print '%s\t%s\t%s' % (hour_of_day, average_power, len(final_data[hour_of_day]))

