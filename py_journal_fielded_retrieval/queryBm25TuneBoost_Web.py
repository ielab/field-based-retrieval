import time
import os
import re
from lxml import etree
from elasticsearch import Elasticsearch
from queryBuilder_bm25f import bm25f_query_builder
import multiprocessing
from functools import partial
from elasticsearch import TransportError

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
queryFile2013 = '/volumes/ext/jimmy/data/clueweb12/web2013.topics.txt'
queryFile2014 = '/volumes/ext/jimmy/data/clueweb12/trec2014-topics.xml'
topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/'
topPrefix = "topBm25fTuneBoost_Web"
b = 0.75
k = 1.2
result_size = 1000

if not os.path.exists(topPath):
    os.makedirs(topPath)

# Index Setting
docType = "clueweb"
indexName = "clueweb12b_all"

queries = []
vocabString = ""
# load  Adhoc 2013 queries
with open(queryFile2013) as f:
    content = f.readline().rstrip()
    while content:
        contentList = content.split(':')
        queryNumber = contentList[0]
        queryTerms = re.sub(r'([^\s\w]|_)+', ' ', contentList[1])

        body_script = {
            "analyzer": "my_english",
            "text": queryTerms
        }

        res = es.indices.analyze(index=indexName, body=body_script)
        analysedTerms = ""
        for token in res["tokens"]:
            analysedTerms += " " + token["token"]

        queryData = {"queryNumber": queryNumber, "queryTerms": analysedTerms.strip()}
        vocabString += " " + analysedTerms
        queries.append(queryData)
        content = f.readline().rstrip()
f.close()

# load  Adhoc 2014 queries
tree = etree.parse(queryFile2014)
webTrack = tree.getroot()
for topic in webTrack.iter("topic"):
    queryTerms = ""
    queryNumber = topic.attrib["number"]
    for detail in topic:
        if detail.tag == "query":
            queryTerms = re.sub(r'([^\s\w]|_)+', ' ', detail.text)

            body_script = {
                "analyzer": "my_english",
                "text": queryTerms
            }

            res = es.indices.analyze(index=indexName, body=body_script)
            analysedTerms = ""
            for token in res["tokens"]:
                analysedTerms += " " + token["token"]

    queryData = {"queryNumber": queryNumber, "queryTerms": analysedTerms.strip()}
    vocabString += " " + analysedTerms
    queries.append(queryData)

startTime = time.time()
indexLap = time.time()
print("query loaded")

# build dictionary to store document frequency of each analyzed term
oriTerms = vocabString.split()
oriTerms = list(set(oriTerms)) # remove duplicate terms

terms_df = {}
for term in oriTerms:
    query_string = {
        "size": 0,
        "_source": False,
        "query": {
            "query_string": {
                "query": term,
                "fields": ["title", "body"]
            }
        }
    }

    res = es.search(index=indexName, doc_type=docType, body=query_string)
    # if analysed token is nothing then it is a stop word so ignore it
    print("{} : {}".format(term, res['hits']['total']))
    terms_df[term]= res['hits']['total']
print("Document Frequency  loaded")

#for key,value in terms_df.iteritems():
#    print("{}: {}".format(key, value))


def es_search(p_alpha, p_size, p_k1, p_bt, p_bb, p_avgTitle, p_avgBody, p_df, p_query):
    resultstring = ''
    print "processing alpha: " + str(p_alpha) + " qNum: " + p_query["queryNumber"]
    es1 = Elasticsearch(urls='http://localhost', port=9200, timeout=10000)

    querystring = bm25f_query_builder(p_query["queryTerms"], p_size, p_k1, p_alpha, p_bt, p_bb, p_avgTitle, p_avgBody,
                                      p_df)
    try:
        res1 = es1.search(index=indexName, doc_type=docType, body=querystring)
    except TransportError as e:
        print(e.info)

    rank = 1

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


# get title field average length
body_script = {
    "size": 0,
    "aggs": {
        "avg_title": {"avg": {"field": "title.length"}},
        "avg_body": {"avg": {"field": "body.length"}}
    }
}
res = es.search(index=indexName, doc_type=docType, body=body_script)
avgTitle = float(res['aggregations']['avg_title']['value'])
avgBody = float(res['aggregations']['avg_body']['value'])


for t_alpha in xrange(0, 11, 1):
    weights = "_alpha" + str(t_alpha)


    fw = open(topPath + topPrefix + weights, 'w')
    p = multiprocessing.Pool(processes=4)
    func = partial(es_search, float(t_alpha)/10, result_size, k, b, b, avgTitle, avgBody, terms_df)

    results = p.map(func, queries)
    p.close()
    p.join()
    for res in results:
        fw.write(res)

    print ("Scheme: {0} Completed, Duration: {1} seconds".format(weights, time.time() - indexLap))
    indexLap = time.time()
    fw.close()

print (" Duration ", time.time()-startTime)
