import time
import os
import multiprocessing
from lxml import etree
from elasticsearch import Elasticsearch
from queryBuilderAquaint import query_builder_aquaint


# local setting
queryFile = '/volumes/data/phd/data/aquaint_eval/2005_HardTrack.topics.txt'
topPath = '/volumes/data/phd/data/aquaintTuned_localtopfiles/'

#Server Setting
queryFile = '/volumes/ext/data/Aquaint_hardtrack_2005/2005_HardTrack.topics.txt'
topPath = '/volumes/ext/jimmy/data/aquaintTuned_topfiles/'

titleWeights = [0, 1, 3, 5] #previously Headline
bodyWeights = [0, 1, 3, 5] #previously Text
tieBreakers = [0.25]

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)

if not os.path.exists(topPath):
    os.makedirs(topPath)

# Index Setting
docType = "aquaint"
indexName = "aquaint_all"

# load queries
tree = etree.parse(queryFile)
root = tree.getroot()

queries = []

for top in root.findall('top'):
    queryNumber = top.find('num').text.split(":")[1]
    queryTerms = top.find('title').text

    queryData = { "queryNumber": queryNumber, "queryTerms": queryTerms }
    queries.append(queryData)

startTime = time.time()
indexLap = time.time()


tw = 0
bw = 0
tie = 0


def es_search(query):
    es1 = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
    res1 = es1.search(index=indexName, doc_type=docType, body=query_builder_aquaint(query["queryTerms"], tw, bw, tie))

    rank = 1
    resultstring1 = ""
    for hit in res1['hits']['hits']:
        resultstring1 = resultstring1 + query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " + \
                       str(hit["_score"]) + " " + indexName + "\n"
        rank += 1

    return resultstring1


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
            for bw in bodyWeights:
                if tw + bw >= 1:
                    for tie in tieBreakers:
                        # Index Setting
                        # indexName format: aquaint_aabb. aa=headline weight, bb=title weight
                        weights = format(tw, '02') + format(bw, '02')

                        fw = open(topPath + "aquaint" + "_" + weights + "_b" + format(float(b)/100) + "_k" +format(float(k)/10) , 'w')

                        '''
                        for query in queries:
                            res = es.search(index=indexName, doc_type=docType, body=query_builder_aquaint(query["queryTerms"], tw, bw, tie))
                            rank = 1
                            for hit in res['hits']['hits']:
                                fw.write(query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " +
                                         str(hit["_score"]) + " " + indexName + "\n")
                                rank += 1

                        '''

                        p = multiprocessing.Pool()
                        resultString = p.map(es_search, queries)
                        p.close()
                        p.join()
                        for res in resultString:
                            fw.write(res)

                        fw.close()

                        print ("Weights: {0}, b: {2}, k1:{3} Completed, Duration: {1} seconds".format(weights, time.time() - indexLap, str(float(b)/100), str(float(k)/10)))
                        indexLap = time.time()

print (" Duration ", time.time()-startTime)