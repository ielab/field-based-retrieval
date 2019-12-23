import time
import os
import re
from lxml import etree
from elasticsearch import Elasticsearch
from queryBuilder_unifield import query_builder_unifield
import multiprocessing
from functools import partial

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
queryFile2013 = '/volumes/ext/jimmy/data/clueweb12/web2013.topics.txt'
queryFile2014 = '/volumes/ext/jimmy/data/clueweb12/trec2014-topics.xml'
topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/'
topPrefix = "topTuneBvaried_Web_unifield"


k = 1.2
tieBreakers = [0.25]

if not os.path.exists(topPath):
    os.makedirs(topPath)

# Index Setting
docType = "clueweb"
indexName = "clueweb12b_unifield"

queries = []

# load  Adhoc 2013 queries
with open(queryFile2013) as f:
    content = f.readline().rstrip()
    while content:
        contentList = content.split(':')
        queryNumber = contentList[0]
        queryTerms = re.sub(r'([^\s\w]|_)+', ' ', contentList[1])

        queryData = {"queryNumber": queryNumber, "queryTerms": queryTerms}
        queries.append(queryData)
        content = f.readline().rstrip()
f.close

# load  Adhoc 2014 queries
tree = etree.parse(queryFile2014)
webTrack = tree.getroot()
for topic in webTrack.iter("topic"):
    queryNumber = topic.attrib["number"]
    for detail in topic:
        if detail.tag == "query":
            queryTerms = re.sub(r'([^\s\w]|_)+', ' ', detail.text)

    queryData = {"queryNumber": queryNumber, "queryTerms": queryTerms}
    queries.append(queryData)

startTime = time.time()
indexLap = time.time()

fw = ""


def es_search(p_query):
    es1 = Elasticsearch(urls='http://localhost', port=9200, timeout=5000)
    res1 = es1.search(index=indexName, doc_type=docType,
                      body=query_builder_unifield(p_query["queryTerms"]))
    print(p_query["queryNumber"])
    rank = 1
    resultstring = ""
    for hit in res1['hits']['hits']:
        resultstring = resultstring + p_query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " + \
                       str(hit["_score"]) + " " + indexName + "\n"
        rank += 1

    return resultstring


for b in xrange(0, 101, 5):
    # Flushing before closing
    res = es.indices.flush(index=indexName)
    print("Flushing - response: '%s'" % res)

    # Clear Cache
    res = es.indices.clear_cache(index=indexName)
    print("Clearing Cache - response: '%s'" % res)

    # Closing the index, required before changing the index setting
    res = es.indices.close(index=indexName)
    print("Closed - response: '%s'" % res)

    # Setting the index (change the b for title and body field of the bm25 simmilarities)

    request_body = {
        "settings": {
            "similarity": {
                "bm25_uni": {
                    "type": "BM25",
                    "b": (float(b) / 100),
                    "k1": k
                }
            }
        }
    }

    res = es.indices.put_settings(index=indexName, body=request_body)

    # reopen index after configure the bm25 parameter
    res = es.indices.open(index=indexName)
    es.cluster.health(index=indexName, wait_for_status='green')  # wait until index ready
    print("Opened index {0} --> b: {1}".format(indexName, (float(b) / 100)))

    weights = "_b" + format(float(b) / 100) + "_k" + format(k)
    fw = open(topPath + topPrefix + weights, 'w')

    p = multiprocessing.Pool()
    results = p.map(es_search, queries)
    p.close()
    p.join()
    for res in results:
        fw.write(res)

    print ("Scheme: {0} Completed, Duration: {1} seconds".format(weights, time.time() - indexLap))
    indexLap = time.time()
    fw.close()

print (" Duration ", time.time()-startTime)