import time
import os
import re
import multiprocessing
from lxml import etree
from elasticsearch import Elasticsearch
from queryBuilderB import b_query_builder

dataSet = "2015"

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
if dataSet == "2015":
    queryFile = '/volumes/ext/jimmy/data/clef2015_eval/clef2015.test.queries-EN.txt'
    topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/clef2015_termWeight_topFiles/'
    topPrefix = "clef2015_top"
elif dataSet == "2014":
    queryFile = '/Volumes/ext/Data/Clef2014/queries.clef2014ehealth.1-50.test.en.xml'
    topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/clef2014_termWeight_topFiles/'
    topPrefix = "clef2014_top"


titleWeights = [0, 1, 3, 5]
metaWeights = [0, 1, 3, 5]
headersWeights = [0, 1, 3, 5]
bodyWeights = [0, 1, 3, 5]
tieBreakers = [0.25]

if not os.path.exists(topPath):
    os.makedirs(topPath)

# load queries
tree = etree.parse(queryFile)
topics = tree.getroot()

queries = []
if dataSet=="2015":
    for top in topics.iter("top"):
        for detail in top:
            if detail.tag == "num":
                queryNumber = detail.text.split(".")[2]

            if detail.tag == "query":
                queryTerms = detail.text
                queryTerms = re.sub(r'([^\s\w]|_)+', ' ', queryTerms)

        queryData = { "queryNumber": queryNumber, "queryTerms": queryTerms }
        queries.append(queryData)
elif dataSet=="2014":
    for top in topics.iter("topic"):
        for detail in top:
            if detail.tag == "id":
                queryNumber = detail.text.split(".")[1]

            if detail.tag == "title":
                queryTerms = detail.text
                queryTerms = re.sub(r'([^\s\w]|_)+', ' ', queryTerms)

        queryData = {"queryNumber": queryNumber, "queryTerms": queryTerms}
        queries.append(queryData)

# Index Setting
indexName = "kresmoi_all"
docType = "kresmoi"


# Closing the index, required before changing the index setting
res = es.indices.close(index=indexName)
print("Closed - response: '%s'" % res)

# Setting the index (change the b and k1 of the bm25 simmilarities)
request_body = {
    "settings": {
        "similarity": {
            "sim_title": {
                "type": "BM25",
                "b": float(0.75),
                "k1": float(1.2)
            },
            "sim_meta": {
                "type": "BM25",
                "b": float(0.75),
                "k1": float(1.2)
            },
            "sim_headers": {
                "type": "BM25",
                "b": float(0.75),
                "k1": float(1.2)
            },
            "sim_body": {
                "type": "BM25",
                "b": float(0.75),
                "k1": float(1.2)
            }
        }
    }
}

res = es.indices.put_settings(index=indexName, body=request_body)
print("Configured - response: '%s'" % res)

# reopen index after configure the bm25 parameter
res = es.indices.open(index=indexName)
es.cluster.health(index=indexName, wait_for_status='green')  # wait until index ready
print("Opened - response: '%s'" % res)

startTime = time.time()
indexLap = time.time()

tw = 0
mw = 0
hw = 0
bw = 0

def es_search(query):
    es1 = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
    res1 = es1.search(index=indexName, doc_type=docType, body=b_query_builder(query["queryTerms"], tw, mw, hw, bw, tie))

    rank = 1
    resultstring1 = ""
    for hit in res1['hits']['hits']:
        resultstring1 = resultstring1 + query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " + \
                       str(hit["_score"]) + " " + indexName + "\n"
        rank += 1

    return resultstring1

for tw in titleWeights:
    for mw in metaWeights:
        for hw in headersWeights:
            for bw in bodyWeights:
                if tw + mw + hw + bw >= 1:
                    for tie in tieBreakers:
                        res = es.indices.flush(index=indexName)
                        print("Flushing - response: '%s'" % res)

                        # Index Setting
                        weights = format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02')

                        fw = open(topPath + topPrefix + "_" + weights + "_" + format(tie*100, '03'), 'w')
                        '''
                        for query in queries:
                            res = es.search(index=indexName, doc_type=docType, body=b_query_builder(query["queryTerms"], tw, mw, hw, bw, tie))
                            rank = 1
                            for hit in res['hits']['hits']:
                                fw.write(query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " +
                                         str(hit["_score"]) + " " + indexName + "\n")
                                rank += 1
                        fw.close()
                        '''

                        p = multiprocessing.Pool()
                        resultString = p.map(es_search, queries)
                        p.close()
                        p.join()
                        for res in resultString:
                            fw.write(res)

                        print ("Weights: {0}, Tie: {2} Completed, Duration: {1} seconds".format(weights, time.time() - indexLap, str(tie)))
                        indexLap = time.time()

print (" Duration ", time.time()-startTime)
