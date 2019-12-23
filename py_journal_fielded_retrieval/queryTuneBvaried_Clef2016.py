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
queryFile = '/volumes/ext/data/clef2016/queries2016.xml'
topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/'
topPrefix = "topTuneBvaried_Clef2016"

k = 1.2
tieBreakers = [1]

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

fw = ""


def es_search(p_alpha, p_tie, p_query):
    print("processing tie:{} alpha: {} qNum: {}".format(p_tie, p_alpha, p_query["queryNumber"]))
    es1 = Elasticsearch(urls='http://localhost', port=9200, timeout=5000)
    res1 = es1.search(index=indexName, doc_type=docType,
                      body=query_builder_normal(p_query["queryTerms"], p_alpha, p_tie))

    rank = 1
    resultstring = ""
    for hit in res1['hits']['hits']:
        resultstring = resultstring + p_query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " + \
                       str(hit["_score"]) + " " + indexName + "\n"
        rank += 1

    return resultstring

for bt in xrange(0, 101, 5):
    for bb in xrange(0, 101, 5):

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
                    "sim_title": {
                        "type": "BM25",
                        "b": (float(bt) / 100),
                        "k1": k
                    },
                    "sim_body": {
                        "type": "BM25",
                        "b": (float(bb) / 100),
                        "k1": k
                    }
                }
            }
        }

        res = es.indices.put_settings(index=indexName, body=request_body)

        # reopen index after configure the bm25 parameter
        res = es.indices.open(index=indexName)
        es.cluster.health(index=indexName, wait_for_status='green')  # wait until index ready
        print("Opened index {0} --> bt: {1}, bb: {2}".format(indexName, (float(bt) / 100), (float(bb) / 100)))

        for t_alpha in xrange(0, 11, 1):
            for tie in tieBreakers:
                weights = "_alpha" + str(t_alpha) + "_bt" + format(float(bt) / 100) + "_bb" + format(float(bb) / 100) \
                          + "_k" + format(k)
                fw = open(topPath + topPrefix + weights, 'w')

                p = multiprocessing.Pool()
                func = partial(es_search, float(t_alpha) / 10, tie)

                results = p.map(func, queries)
                p.close()
                p.join()
                for res in results:
                    fw.write(res)

                print ("Scheme: {0} Completed, Duration: {1} seconds".format(weights, time.time() - indexLap))
                indexLap = time.time()
                fw.close()

print (" Duration ", time.time()-startTime)
