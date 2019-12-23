import time
import os
import re
import multiprocessing
from lxml import etree
from elasticsearch import Elasticsearch
from queryBuilderB import b_query_builder

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
queryFile = '/volumes/ext/data/clef2016/queries2016.xml'
topPath = '/volumes/ext/jimmy/data/clef2016Tuned_topFiles/'
topPrefix = "clef2016"
titleWeights = [0, 1, 3, 5]
metaWeights = [0]
headersWeights = [0]
bodyWeights = [0, 1, 3, 5]
tieBreakers = [0.25]

if not os.path.exists(topPath):
    os.makedirs(topPath)

# Index Setting
docType = "clueweb"
indexName = "clueweb12b_all"

# load queries
tree = etree.parse(queryFile)
topics = tree.getroot()

queries = []


for topic in topics.iter("query"):
    for detail in topic:
        if detail.tag == "id":
            queryNumber = detail.text
        elif detail.tag == "title":
            queryTerms = detail.text
            #print queryTerms
            queryTerms = re.sub(r'([^\s\w]|_)+', ' ', queryTerms)
            #print queryTerms

    if queryNumber == "129005":
        queryTerms = "craving salt     full body spasm    need 12 hrs sleep    can t maintain body temperature"
    queryData = {"queryNumber": queryNumber, "queryTerms": queryTerms}
    queries.append(queryData)
    #print queryNumber + " >> " + queryTerms

startTime = time.time()
indexLap = time.time()

tw = 0
mw = 0
hw = 0
bw = 0
tie = 0
fw = ""

def es_search(query):
    es1 = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
    res = es1.search(index=indexName, doc_type=docType, body=b_query_builder(query["queryTerms"], tw, mw, hw, bw, tie))

    rank = 1
    resultString = ""
    for hit in res['hits']['hits']:
        resultString = resultString + query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " + str(hit["_score"]) + " " + indexName + "\n"
        rank += 1
    print "finished query: " + query["queryNumber"]
    return resultString

for b in xrange(0, 101, 5):
    for k in xrange(2, 31, 2):
        # Flushing before closing
        res = es.indices.flush(index=indexName)
        print("Flushing - response: '%s'" % res)

        # Clear Cache
        res = es.indices.clear_cache(index=indexName)
        print("Clearing Cache - response: '%s'" % res)

        # Closing the index, required before changing the index setting
        res = es.indices.close(index=indexName)
        print("Closed - response: '%s'" % res)

        # Setting the index (change the b and k1 of the bm25 simmilarities)
        request_body = {
            "settings": {
                "similarity": {
                    "sim_title": {
                        "type": "BM25",
                        "b": (float(b) / 100),
                        "k1": (float(k) / 10)
                    },
                    "sim_meta": {
                        "type": "BM25",
                        "b": (float(b) / 100),
                        "k1": (float(k) / 10)
                    },
                    "sim_headers": {
                        "type": "BM25",
                        "b": (float(b) / 100),
                        "k1": (float(k) / 10)
                    },
                    "sim_body": {
                        "type": "BM25",
                        "b": (float(b) / 100),
                        "k1": (float(k) / 10)
                    }
                }
            }
        }

        print("Configuring index {0} --> b: {1} , k1: {2}".format(indexName, (float(b) / 100), (float(k) / 10)))
        res = es.indices.put_settings(index=indexName, body=request_body)
        print("Configured - response: '%s'" % res)

        # reopen index after configure the bm25 parameter
        res = es.indices.open(index=indexName)
        es.cluster.health(index=indexName, wait_for_status='green')  # wait until index ready
        print("Opened - response: '%s'" % res)


        for tw in titleWeights:
            for mw in metaWeights:
                for hw in headersWeights:
                    for bw in bodyWeights:
                        if tw + mw + hw + bw >= 1:
                            for tie in tieBreakers:
                                weights = format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02') + "_b" + format(float(b) / 100) + "_k" + format(float(k) / 10)

                                fw = open(topPath + topPrefix + "_" + weights , 'w')
                                '''
                                for query in queries:
                                    res = es.search(index=indexName, doc_type=docType, body=b_query_builder(query["queryTerms"], tw, mw, hw, bw, tie))
                                    rank = 1
                                    for hit in res['hits']['hits']:
                                        fw.write(query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " + str(hit["_score"]) + " " + indexName + "\n")
                                        rank += 1

                                    print(weights + " >> Query " + query["queryNumber"] + " : " + query["queryTerms"] )
                                fw.close()
                                '''
                                p = multiprocessing.Pool()
                                resultString = p.map(es_search, queries)
                                p.close()
                                p.join()
                                for res in resultString:
                                    fw.write(res)

                                print ("Weights: {0}, b: {2}, k1:{3} Completed, Duration: {1} seconds".format(weights, time.time() - indexLap, str(float(b) / 100),str(float(k) / 10)))
                                indexLap = time.time()

print (" Duration ", time.time()-startTime)