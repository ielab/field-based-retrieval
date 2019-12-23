import time
import glob
import os
import json

path = '/users/jimmy/data/json2015/*'
outputFile = topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/fieldLength_Kresmoi.txt'

startTime = time.time()
lapTime = time.time()
indexTime = time.time()

files = glob.glob(path)
totalFiles = len(files)

i = 0.0

fw = open(outputFile,'w')
for f in files:
    i += 1
    with open(f, 'r') as fr:
        data = json.load(fr)
        fr.close()
    docId = os.path.basename(f)

    title = ""
    if data["title"]:
        title = data["title"]

    body = ""
    if data["body"]:
        body = data["body"]

    fw.write('{},{},{}\n'.format(docId, len(title.split()), len(body.split())))

    if i % 10000:
        print ("{0} submitted, ({1} seconds)".format(str(i), time.time() - lapTime))

fw.close()

