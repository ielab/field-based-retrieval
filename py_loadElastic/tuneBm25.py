import os

from elasticsearch import Elasticsearch
from queryBuilderB import b_query_builder

dataSet = "2015"

# local setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
if dataSet == "2015":
    queryFile = '/Volumes/Data/Phd/Data/Clef2015_eval/clef2015.test.queries-EN.txt'
    topPath = '/Volumes/Data/Phd/Data/Clef2015_localTopTune/'
    topPrefix = "clef2015_topTuned"
elif dataSet == "2014":
    queryFile = '/Volumes/Data/Phd/Data/Clef2014_eval/queries.clef2014ehealth.1-50.test.en.xml'
    topPath = '/Volumes/Data/Phd/Data/Clef2014_localTopTune/'
    topPrefix = "clef2014_topTuned"

'''
# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
if dataSet == "2015":
    queryFile = '/Volumes/ext/jimmy/tools/clef2015/clef2015.test.queries-EN.txt'
    topPath = '/Volumes/ext/jimmy/Data/clef2015_topTune/'
    topPrefix = "clef2015_topTuned"
elif dataSet == "2014":
    queryFile = '/Volumes/ext/Data/Clef2014/queries.clef2014ehealth.1-50.test.en.xml'
    topPath = '/Volumes/ext/jimmy/Data/clef2014_topTune/'
    topPrefix = "clef2014_topTuned"
'''

titleWeights = [1]
metaWeights = [1]
headersWeights = [1] # note: by default: the body will contain header once
bodyWeights = [1]

tieBreakers = [0.25]

bList = [0,1] # elasticsearch default for b: 0.75
kList = [1.2, 10] # elasticsearch default for k1: 1.2

docType = "cluewebcustom"
indexName = "clueweb12custom"



if not os.path.exists(topPath):
    os.makedirs(topPath)

i = 0.0
errorCount=0

for mw in titleWeights:
    for hw in metaWeights:
        for tw in headersWeights:
            for bw in bodyWeights:
                if tw + mw + hw + bw >= 1:
                    weights = format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02')

                    if es.indices.exists(indexName):
                        for tie in tieBreakers:
                            for b in bList:
                                for k in kList:
                                    #Closing the index, required before changing the index setting
                                    res = es.indices.close(index=indexName)
                                    print("Closed - response: '%s'" % res)

                                    #Setting the index (change the b and k1 of the bm25 simmilarities)
                                    request_body = {
                                        "settings": {
                                                "similarity": {
                                                    "my_bm25": {
                                                        "type": "BM25",
                                                        "b": b,
                                                        "k1": k
                                                    }
                                                }

                                        }
                                    }

                                    print("Configuring index {0} --> b: {1} , k1: {2}".format(indexName, b, k))
                                    res = es.indices.put_settings(index=indexName, body=request_body)
                                    print("Configured - response: '%s'" % res)

                                    res = es.indices.open(index=indexName)
                                    es.cluster.health(index=indexName, wait_for_status='green')
                                    print("Opened - response: '%s'" % res)


                                    fw = open(topPath + topPrefix + "_" + weights + "_b" + format(b*100) + "_k" + format(k*100), 'w')

                                    #for query in queries:
                                    query = "many red marks on legs after traveling from us"

                                    res = es.search(index=indexName, doc_type=docType, body=b_query_builder(query, tw, mw, hw, bw, tie))
                                    rank = 1

                                    for hit in res['hits']['hits']:
                                        fw.write(
                                             " 0 " + str(hit["_id"]) + " " + str(rank) + " " +
                                            str(hit["_score"]) + " " + indexName + "\n")
                                        rank += 1

                                    fw.close()