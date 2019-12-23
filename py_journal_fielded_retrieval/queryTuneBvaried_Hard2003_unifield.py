import time
import os
from lxml import etree
from elasticsearch import Elasticsearch
from queryBuilder_unifield import query_builder_unifield
import multiprocessing
from functools import partial

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
queryFile = '/volumes/Data/Phd/Data/Hard2003_eval/03.topics.nometadata'
topPath = '/volumes/Data/Phd/Data/runs_ES/'
topPrefix = "topTuneBvaried_Hard2003_unifield"

k = 1.2

if not os.path.exists(topPath):
    os.makedirs(topPath)

# Index Setting
docType = "hard2003"
indexName = "hard2003_unifield"

# load queries
tree = etree.parse(queryFile)
root = tree.getroot()

queries = []

for top in root.findall('top'):
    queryNumber = top.find('num').text
    queryNumber = str(int(queryNumber))
    queryTerms = top.find('title').text

    queryData = { "queryNumber": queryNumber, "queryTerms": queryTerms }
    queries.append(queryData)

startTime = time.time()
indexLap = time.time()

fw = ""


def es_search(p_query):
    #print("processing tie:{} alpha: {} qNum: {}".format(p_tie, p_alpha, p_query["queryNumber"]))
    es1 = Elasticsearch(urls='http://localhost', port=9200, timeout=5000)
    res1 = es1.search(index=indexName, doc_type=docType,
                      body=query_builder_unifield(p_query["queryTerms"]))

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
                "sim_uni": {
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
    print("Opened index {0} --> bb: {1}".format(indexName, (float(b) / 100)))

    weights = "_b" + format(float(b) / 100) + "_k" + format(k)
    fw = open(topPath + topPrefix + weights, 'w')

    p = multiprocessing.Pool()
    func = partial(es_search)

    results = p.map(func, queries)
    p.close()
    p.join()
    for res in results:
        fw.write(res)

    print ("Scheme: {0} Completed, Duration: {1} seconds".format(weights, time.time() - indexLap))
    indexLap = time.time()
    fw.close()

print (" Duration ", time.time()-startTime)
