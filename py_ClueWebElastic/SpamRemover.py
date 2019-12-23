import gzip
from elasticsearch import Elasticsearch
import time, glob, os, lxml.html, re, io, sys

# local setting
spamRankPath = "/Volumes/Data/Phd/Data/clueweb12_spamrank/waterloo-spam-cw12-decoded"
es = Elasticsearch(urls='http://localhost', port=9200, timeout=600)
bulk_size = 5000 #bulk limit in documents
#spamListPath = "/Volumes/Data/Phd/Data/clueweb12_spamrank/"
spamTreshold = "90" # The spammiest documents have a score of 0, and the least spammy have a score of 99
docType = "clueweb"
indexName = "clueweb12_nospam90"
finishedFiles = []

# Server setting
spamRankPath = "/Volumes/ext/jimmy/data/waterloo-spam-cw12-decoded"
es = Elasticsearch(urls='http://localhost', port=9200, timeout=600)
bulk_size = 5000 #bulk limit in documents
#spamListPath = "/Volumes/Data/Phd/Data/clueweb12_spamrank/"
spamTreshold1 = "25" # The spammiest documents have a score of 0, and the least spammy have a score of 99
#spamTreshold2 = "75"
docType = "clueweb"
indexName1 = "clueweb12_nospam25"
#indexName2 = "clueweb12_nospam75"
finishedFiles = []

startTime = time.time()
lapTime = time.time()
docCount = 0
spamCount1 = 0
spamCount2 = 0
bulk_data1 = []
bulk_data2 = []

for f in glob.glob(spamRankPath + "/*"):
    print "Checking compressed file: " + f
    if f[-22:] not in finishedFiles:
        print "Processing compressed file: " + f
        with gzip.open(f, mode='rb') as gzf:
            for line in gzf:
                docCount += 1

                spamScore=line.split()[0]
                docId = line.split()[1]

                if int(spamScore) < int(spamTreshold1):
                    bulk_meta1 = {
                        "delete": {
                            "_index": indexName1,
                            "_type": docType,
                            "_id": docId
                        }
                    }
                    bulk_data1.append(bulk_meta1)
                    spamCount1 += 1

                '''
                if int(spamScore) < int(spamTreshold2):
                    bulk_meta2 = {
                        "delete": {
                            "_index": indexName2,
                            "_type": docType,
                            "_id": docId
                        }
                    }
                    bulk_data2.append(bulk_meta2)
                    spamCount2 += 1
                '''
                if docCount % bulk_size == 0:
                    if len(bulk_data1) > 0:
                        res = es.bulk(index=indexName1, doc_type=docType, body=bulk_data1, refresh=False)
                        bulk_data1 = []

                    if len(bulk_data2) > 0:
                        res = es.bulk(index=indexName2, doc_type=docType, body=bulk_data2, refresh=False)
                        bulk_data2 = []

                    print (f[-22:] + "Completed Documents: " + str(docCount) + " Current Spam 1 : " + str(spamCount1) + ", 2:" + str(spamCount2))

if len(bulk_data1)>0:
    res = es.bulk(index=indexName, doc_type=docType, body=bulk_data1, refresh=False)

if len(bulk_data2) > 0:
    res = es.bulk(index=indexName, doc_type=docType, body=bulk_data2, refresh=False)

print ("Total Documents: " + str(docCount) + " Total Spam 1: " + str(spamCount1) + ", 2:" + str(spamCount2))





