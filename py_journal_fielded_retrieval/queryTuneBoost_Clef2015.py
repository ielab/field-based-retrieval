import time
import os
import re
from lxml import etree
from elasticsearch import Elasticsearch
from queryBuilder_normal import query_builder_normal
import multiprocessing
from functools import partial

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
queryFile = '/Volumes/ext/jimmy/tools/clef2015/clef2015.test.queries-EN.txt'
topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/'
topPrefix = "topTuneBoost_Clef2015"
tieBreakers = [1]
b = 0.75
k = 1.2

if not os.path.exists(topPath):
    os.makedirs(topPath)

# Index Setting
indexName = "kresmoi_all"
docType = "kresmoi"

# load queries
tree = etree.parse(queryFile)
topics = tree.getroot()

queries = []
for top in topics.iter("top"):
    for detail in top:
        if detail.tag == "num":
            queryNumber = detail.text.split(".")[2]

        if detail.tag == "query":
            queryTerms = detail.text
            queryTerms = re.sub(r'([^\s\w]|_)+', ' ', queryTerms)

    queryData = { "queryNumber": queryNumber, "queryTerms": queryTerms }
    queries.append(queryData)


startTime = time.time()
indexLap = time.time()


def es_search(p_alpha, p_tie, p_query):
    print "processing alpha: " + str(p_alpha) + " tie: " + str(p_tie) + " qNum: " + p_query["queryNumber"]
    es1 = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
    res1 = es1.search(index=indexName, doc_type=docType,
                      body=query_builder_normal(p_query["queryTerms"], p_alpha, p_tie))

    rank = 1
    resultstring = ""
    for hit in res1['hits']['hits']:
        resultstring = resultstring + p_query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " + \
                       str(hit["_score"]) + " " + indexName + "\n"
        rank += 1

    return resultstring

# Flushing before closing
rs = es.indices.flush(index=indexName)
print("Flushing - response: '%s'" % rs)

# Clear Cache
rs = es.indices.clear_cache(index=indexName)
print("Clearing Cache - response: '%s'" % rs)

# Closing the index, required before changing the index setting
rs = es.indices.close(index=indexName)
print("Closed - response: '%s'" % rs)

# Setting the index
request_body = {
    "settings": {
        "similarity": {
            "sim_title": {
                "type": "BM25",
                "b": b,
                "k1": k
            },
            "sim_body": {
                "type": "BM25",
                "b": b,
                "k1": k
            }
        }
    }
}

es.indices.put_settings(index=indexName, body=request_body)
# print("Configured - response: '%s'" % res)

# reopen index after configure the bm25 parameter
es.indices.open(index=indexName)
es.cluster.health(index=indexName, wait_for_status='green')  # wait until index ready
print("Opened index {0} --> b: {1}, k1: {2}".format(indexName, b, k))

# temporarily start from 0.5 as it previously stopped at 0.5
for t_alpha in xrange(0, 11, 1):
    for tie in tieBreakers:
        weights = "_alpha" + str(t_alpha)
        fw = open(topPath + topPrefix + weights, 'w')

        p = multiprocessing.Pool()
        func = partial(es_search, float(t_alpha)/10, tie)

        results = p.map(func, queries)
        p.close()
        p.join()
        for res in results:
            fw.write(res)

        print ("Scheme: {0} Completed, Duration: {1} seconds".format(weights, time.time() - indexLap))
        indexLap = time.time()
        fw.close()

print (" Duration ", time.time()-startTime)
