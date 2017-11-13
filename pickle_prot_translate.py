import cPickle as pickle
import csv
import glob
import os
import re
import socket
import struct
import time
from datetime import datetime
import sys

##batch filerename

path = '/home/bulat/Downloads/wsp/*.CSV'


def prep(t):
    with open(t, 'rb') as lol:
        rdr = csv.reader(lol, delimiter=';')
        if not os.path.isfile(fname + '.NEW'):
            print "creating file:" + fname + '.NEW'
            os.mknod(fname + '.NEW')
            with open(fname + '.NEW', 'wb') as hah:
                wrt = csv.writer(hah, delimiter=';')
                for idx, row in enumerate(rdr):
                    if idx == 0:  # ignores first commentary row
                        pass
                    elif idx == 1:  # uncomment when dealing with files NOT FERM-*
                        pass
                    elif idx == 2:  # takes column labels and applies them as header into the output file
                        row = row[0:4]  # gets rid of trailing commas59
                        wrt.writerow(row)
            # print row
                    elif idx == 3:  # ignores units
                        pass
                    else:
                        timestamp = time.mktime(datetime.strptime(row[0], '%a %b %d %H:%M:%S %Y').timetuple())
                # timestamp = timestamp + 63072000 #offsets lack of locale for testlauf
                        row = [timestamp] + row[1:4]
                # print len(row)
                        wrt.writerow(row)
        else:
            try:
                line = "do you want to remove file" + fname + '.NEW' + '?' + '(default is yes/no):'
                yes = {'yes', 'y', 'ye', ''}
                no = {'no', 'n'}
                delete_decision = raw_input(line).lower()
                if delete_decision in yes:
                    os.remove(fname + '.NEW')
                    print "creating file:" + fname + '.NEW'
                    os.mknod(fname + '.NEW')
                    with open(fname + '.NEW', 'wb') as hah:
                        wrt = csv.writer(hah, delimiter=';')
                        for idx, row in enumerate(rdr):
                            if idx == 0:  # ignores first commentary row
                                pass
                            elif idx == 1:  # uncomment when dealing with files NOT FERM-*
                                pass
                            elif idx == 2:  # takes column labels and applies them as header into the output file
                                row = row[0:4]  # gets rid of trailing commas. Mind the number of columns!
                                wrt.writerow(row)
                                # print row
                            elif idx == 3:  # ignores units
                                pass
                            else:
                                timestamp = time.mktime(datetime.strptime(row[0], '%a %b %d %H:%M:%S %Y').timetuple())
                                # timestamp = timestamp + 63072000 #offsets lack of locale for testlauf
                                row = [timestamp] + row[1:4]
                                # print len(row)
                                wrt.writerow(row)
                if delete_decision in no:
                    raise SystemExit
            except OSError, e:  ## if failed, report it back to the user ##
                print "Error: %s - %s." % (e.filename, e.strerror)

            # print row


def urlify(x):  ### removes all non-numerical, non-alphabet characters
    x = re.sub(r"[^\w\s]", '_', x)
    x = re.sub(r"\s+", '_', x)
    x = suffix_graphite + '.' + x  # to enable reactor differentiation in grafana

    return x


def file_len(w):
    with open(w) as f:  # when using with, close() happens automatically
        for i, l in enumerate(f):
            pass
    return i + 1


def main(z, interval=0.3):
    with open(z, 'rb') as chunk:
        reading = csv.reader(chunk, delimiter=';')
        keys = next(reading)
        del keys[0]
        keys = [urlify(x) for x in keys]
        lol = []
        # pdb.set_trace()
        for inx, row in enumerate(reading):
            values = []
            for x in row[1:4]:
                values.append(x.replace(',', '.').replace('#INF', '0').replace('#IND', '0'))
            print values
            timestamps = []
            for x in row[0:1] * len(values):
                timestamps.append(float(x))
            test = zip(timestamps, values)
            ha = zip(keys, test)
            lol = lol + ha
            ha = []
            timestamps=[]
            values=[]
            #print "len lol at line 77: ",len(lol)
            if inx > 0:
                if inx % 20 == 0:
                    #print lol
                    payload = pickle.dumps(lol, 1)
                    header = struct.pack("!L", len(payload))
                    message = header + payload
                    size = len(payload)
                    print "size of payload is:",size,"bit"
                    print inx,"out of ",row_count
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect(('0.0.0.0', 2004))
                    s.sendall(message)
                    s.close()
                    time.sleep(0.3)
                    print "len lol of sent payload: ", len(lol)
                    lol =[]
                    print "len lol at line 93, after nullifying: ",len(lol)
                elif inx == row_count:
                    payload = pickle.dumps(lol, 1)
                    header = struct.pack("!L", len(payload))
                    message = header + payload
                    size = len(payload)
                    print "size of payload is:", size, "bit"
                    print inx, "out of ", row_count
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect(('0.0.0.0', 2004))
                    s.sendall(message)
                    s.close()
                    time.sleep(0.3)
                    print "len lol of sent payload: ", len(lol)
                    lol = []
                    print "len lol at line 93, after nullifying: ", len(lol)
                    raise SystemExit



for fname in glob.glob(path):
    suffix_graphite = str(os.path.splitext(os.path.basename(fname))[0])  ###only get the filename w/ path or extension
    row_count = file_len(fname)
    print "current file: ", suffix_graphite, " with: ", row_count, "rows"
    prep(fname)
    print "waiting 2 seconds to finish writing the file"
    time.sleep(2)
    main(fname + '.NEW')
