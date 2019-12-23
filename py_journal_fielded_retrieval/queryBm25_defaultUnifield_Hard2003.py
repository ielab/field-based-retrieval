import time
import os
from lxml import etree
from elasticsearch import Elasticsearch
from queryBuilder_bm25f_unifield import bm25f_query_builder_unifield
import multiprocessing
from functools import partial
from elasticsearch import TransportError

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
queryFile = '/Volumes/Data/Phd/Data/Hard2003_eval/03.topics.nometadata'
topPath = '/volumes/Data/Phd/Data/runs_ES/'
topPrefix = "topBm25f_unifield_Hard2003"
b = 0.75
k = 1.2
result_size = 1000

if not os.path.exists(topPath):
    os.makedirs(topPath)

# Index Setting
docType = "hard2003"
indexName = "hard2003_unifield"

# load queries
tree = etree.parse(queryFile)
root = tree.getroot()

queries = []
vocabString = ""
for top in root.findall('top'):
    queryNumber = top.find('num').text
    queryTerms = top.find('title').text.lower()
    queryTerms = queryTerms.replace('-', ' ').replace('.', ' ').replace(',', ' ').replace("'", ' ')


    body_script = {
        "analyzer": "my_english",
        "text": queryTerms
    }

    res = es.indices.analyze(index=indexName, body=body_script)
    analysedTerms = ""
    for token in res["tokens"]:
        analysedTerms += " " + token["token"]

    queryData = { "queryNumber": queryNumber, "queryTerms": analysedTerms.strip() }
    vocabString += " " + analysedTerms
    queries.append(queryData)

startTime = time.time()
indexLap = time.time()

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
                "fields": ["titlebody"]
            }
        }
    }

    res = es.search(index=indexName, doc_type=docType, body=query_string)
    terms_df[term]= res['hits']['total']


for key,value in terms_df.iteritems():
    print("{}: {}".format(key, value))


def es_search(p_size, p_k1, p_b, p_avgTitleBody, p_df, p_query):
    resultstring = ''
    print "processing qNum: " + p_query["queryNumber"]
    es1 = Elasticsearch(urls='http://localhost', port=9200, timeout=500)

    querystring = bm25f_query_builder_unifield(p_query["queryTerms"], p_size, p_k1, p_b, p_avgTitleBody, p_df)
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
            "sim_uni": {
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
        "avg_titlebody": {"avg": {"field": "titlebody.length"}}
    }
}
res = es.search(index=indexName, doc_type=docType, body=body_script)
print(res['aggregations']['avg_titlebody']['value'])
avgTitleBody = float(res['aggregations']['avg_titlebody']['value'])

fw = open(topPath + topPrefix, 'w')
p = multiprocessing.Pool()
func = partial(es_search, result_size, k, b, avgTitleBody, terms_df)

results = p.map(func, queries)
p.close()
p.join()
for res in results:
    fw.write(res)

print ("Scheme: Completed, Duration: {} seconds".format(time.time() - indexLap))
indexLap = time.time()
fw.close()

print (" Duration ", time.time()-startTime)