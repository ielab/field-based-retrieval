import time, os
from lxml import etree
from elasticsearch import Elasticsearch
from queryBuilderAquaint import query_builder_aquaint
import multiprocessing

# local setting
#queryFile = '/volumes/data/phd/data/aquaint_eval/2005_HardTrack.topics.txt'
#topPath = '/volumes/data/phd/data/aquaint_localtopfiles/'

#Server Setting
queryFile = '/volumes/ext/data/Aquaint_hardtrack_2005/2005_HardTrack.topics.txt'
topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/hard2005_termWeight_topFiles/'


headlineWeights = [0, 1, 3, 5]
textWeights = [0, 1, 3, 5]
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

hw = 0
tw = 0
tie = 0

def es_search(query):
    es1 = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
    res1 = es1.search(index=indexName, doc_type=docType, body=query_builder_aquaint(query["queryTerms"], hw, tw, tie))

    rank = 1
    resultstring1 = ""
    for hit in res1['hits']['hits']:
        resultstring1 = resultstring1 + query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " + \
                       str(hit["_score"]) + " " + indexName + "\n"
        rank += 1

    return resultstring1

for hw in headlineWeights:
    for tw in textWeights:
        if hw + tw >= 1:
            for tie in tieBreakers:
                # Flush index
                res = es.indices.flush(index=indexName)
                print("Flushing - response: '%s'" % res)

                # indexName format: aquaint_aabb. aa=headline weight, bb=title weight
                weights = format(hw, '02') + "00" + "00" + format(tw, '02')
                fw = open(topPath + "hard2005_top" + "_" + weights + "_" + format(tie*100, '03'), 'w')

                '''
                for query in queries:
                    res = es.search(index=indexName, doc_type=docType, body=query_builder_aquaint(query["queryTerms"], hw, tw, tie))
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


                print ("Weights: {0}, Tie: {2} Completed, Duration: {1} seconds".format(weights, time.time() - indexLap, str(tie)))
                indexLap = time.time()

print (" Duration ", time.time()-startTime)
