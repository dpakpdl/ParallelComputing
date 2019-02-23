
with open("/Users/deepakpaudel/mycodes/ParallelComputing/dataset/household_power_consumption.txt", 'r') as infile:
    lines = list()
    # max_length = 0
    for index, line in enumerate(infile):
        if index == 0:
            continue
        line_ = line.strip()
        line__ = line_.split(";")
        if len(line__) >= 2:
            if line__[2] == '?':
                continue
        # if max_length < len(line_):
        #     max_length = len(line_)
        #     print line_, max_length

        lines.append(line_)
    # print max_length

with open("/Users/deepakpaudel/mycodes/ParallelComputing/dataset/household_power_consumption1.txt", 'w') as f:
    for item in lines:
        no_spaces = 68 - len(item)
        print no_spaces
        if no_spaces>0:
            string_added = '@'*no_spaces
        else:
            print no_spaces
            string_added = ''
        f.write("%s%s\n" % (item, string_added))
        no_spaces = 0