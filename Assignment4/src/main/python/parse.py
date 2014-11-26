import re
from collections import defaultdict

def parse(file):
    input_file = open(file, 'r')
    out_file = open('output.txt', 'w')
    u_dict = open('uid_dict.txt', 'w')
    i_dict = open('iid_dict.txt', 'w')
    i_count = 1
    u_count = 1
    iid = defaultdict(int)
    uid = defaultdict(int)

    for line in input_file:
        values = re.split(',', line)

        if uid[values[1]] == 0:
            uid[values[1]] = u_count
            u_count += 1
            u_dict.write(values[1] + ',' + str(uid[values[1]]) + '\n')

        if iid[values[0]] == 0:
            iid[values[0]] = i_count
            i_count += 1
            i_dict.write(values[0] + ',' + str(iid[values[0]]) + '\n')

        out_file.write(str(uid[values[1]]) + ',' + str(iid[values[0]]) + ',' + values[2])

    input_file.close()
    out_file.close()
    u_dict.close()
    i_dict.close()

