import numpy
from scipy.stats import scoreatpercentile
import matplotlib

logfile = open("load.log")
datafile = open("load.csv", "w+")

dataset = []

datafile.write("timestamp,responsetime\n")

for line in logfile:
    mysplit = line.split()
    if len(mysplit) > 5 and "ms" in mysplit[5]:
        timestamp = mysplit[0][1:]
        timestamp = timestamp.replace(",",".")
        responsetime = mysplit[5][:-2]
        datafile.write(timestamp+","+responsetime+"\n")

datafile.write("\n")
logfile.close()
datafile.close()

