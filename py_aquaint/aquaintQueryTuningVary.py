import time, os
from lxml import etree
from elasticsearch import Elasticsearch
from queryBuilderAquaint import query_builder_aquaint
import multiprocessing

# local setting
queryFile = '/volumes/data/phd/data/aquaint_eval/2005_HardTrack.topics.txt'
topPath = '/volumes/data/phd/data/aquaintTunedVary_localtopfiles/'

#Server Setting
queryFile = '/volumes/ext/data/Aquaint_hardtrack_2005/2005_HardTrack.topics.txt'
topPath = '/volumes/ext/jimmy/data/aquaintTunedVary_topfiles/'

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
fw = ""


def es_search(query):
    es1 = Elasticsearch(urls='http://localhost', port=9200, timeout=500)

    res = es1.search(index=indexName, doc_type=docType, body=query_builder_aquaint(query["queryTerms"], tw, bw, tie))
    rank = 1
    resultString = ""

    for hit in res['hits']['hits']:
        resultString = resultString + query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " + str(hit["_score"]) + " " + indexName + "\n"
        rank += 1

    return resultString


bulk_data = []
indexSetting = { 'index': indexName, 'type': docType }

for bt in xrange(0, 101, 5):
    for bb in xrange(0, 101, 5):
        for k in xrange(26, 27, 2):  # best K1 value obtained after tuning with uniform b for HARD 2005 is 2.6
            # Flushing before closing
            res = es.indices.flush(index=indexName)
            #print("Flushing - response: '%s'" % res)

            # Clear Cache
            res = es.indices.clear_cache(index=indexName)
            #print("Clearing Cache - response: '%s'" % res)

            # Closing the index, required before changing the index setting
            res = es.indices.close(index=indexName)
            print("Closed - response: '%s'" % res)

            # Setting the index (change the b and k1 of the bm25 simmilarities)
            request_body = {
                "settings": {
                    "similarity": {
                        "bm25_title": {
                            "type": "BM25",
                            "b": (float(bt) / 100),
                            "k1": (float(k) / 10)
                        },
                        "bm25_body": {
                            "type": "BM25",
                            "b": (float(bb) / 100),
                            "k1": (float(k) / 10)
                        }
                    }
                }
            }

            #print("Configuring index {0} --> bt: {1}, bb: {2} , k1: {3}".format(indexName, (float(bt) / 100), (float(bb) / 100), (float(k) / 10)))
            res = es.indices.put_settings(index=indexName, body=request_body)
            #print("Configured - response: '%s'" % res)

            # reopen index after configure the bm25 parameter
            res = es.indices.open(index=indexName)
            es.cluster.health(index=indexName, wait_for_status='green')  # wait until index ready
            print("Opened index {0} --> bt: {1}, bb: {2} , k1: {3}".format(indexName, (float(bt) / 100),(float(bb) / 100), (float(k) / 10)))
            #print("Opened - response: '%s'" % res)

            for tw in titleWeights:
                for bw in bodyWeights:
                    if tw + bw >= 1:
                        for tie in tieBreakers:
                            # Index Setting
                            # indexName format: aquaint_aabb. aa=headline weight, bb=title weight
                            weights = format(tw, '02') + "0000" + format(bw, '02')

                            fw = open(topPath + "aquaint" + "_" + weights + "_bt" + format(float(bt)/100)+ "_bb" + format(float(bb)/100) + "_k" +format(float(k)/10) , 'w')


                            p = multiprocessing.Pool()
                            resultString = p.map(es_search, queries)
                            p.close()
                            p.join()
                            for res in resultString:
                                fw.write(res)


                            '''
                            #bulk_data.append(indexSetting)
                            for query in queries:
                                bulk_data.append(query_builder_aquaint(query["queryTerms"], tw, bw, tie))

                            responses = es.msearch(body=bulk_data,index=indexName,doc_type=docType)

                            for res in responses['responses']:
                                rank = 1
                                #print res
                                for hit in res['hits']['hits']:
                                    fw.write(query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " + str(hit["_score"]) + " " + indexName + "\n")
                                    rank += 1


                            for query in queries:
                                res = es.search(index=indexName, doc_type=docType, body=query_builder_aquaint(query["queryTerms"], tw, bw, tie))
                                rank = 1
                                for hit in res['hits']['hits']:
                                    fw.write(query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " +
                                             str(hit["_score"]) + " " + indexName + "\n")
                                    rank += 1
                            '''
                            fw.close()

            print ("Parameter b-Title: {0}, b-Body: {1}, k1:{2} Completed, Duration: {3} seconds".format(str(float(bt)/100), str(float(bb)/100), str(float(k)/10), time.time() - indexLap))
            indexLap = time.time()

print (" Duration ", time.time()-startTime)