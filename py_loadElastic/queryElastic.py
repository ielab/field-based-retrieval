import time
import os
import multiprocessing
from lxml import etree
from elasticsearch import Elasticsearch
from queryBuilder import query_builder

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=600)
queryFile = '/Volumes/ext/jimmy/tools/clef2015/clef2015.test.queries-EN.txt'
topPath = '/Volumes/ext/jimmy/tools/clef2015/topFiles/'
titleWeights = [0, 1, 3, 5]
metaWeights = [0, 1, 3, 5]
headersWeights = [0, 1, 3, 5] # note: by default: the body will contain header once
bodyWeights = [0, 1, 3, 5]
tieBreakers = [0.25]

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

    queryData = { "queryNumber": queryNumber, "queryTerms": queryTerms }
    queries.append(queryData)

startTime = time.time()
indexLap = time.time()

for tw in titleWeights:
    for mw in metaWeights:
        for hw in headersWeights:
            for bw in bodyWeights:
                if tw + mw + hw + bw >= 1:
                    for tie in tieBreakers:
                        # Index Setting
                        # indexName format: clef2015_aabbccdd. aa=title weight, bb=meta weight, cc=headers weight, dd=body weight
                        indexName = "clef2015_" + format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02')

                        res = es.indices.open(index=indexName)
                        fw = open(topPath + indexName, 'w')

                        for query in queries:
                            res = es.search(index=indexName, doc_type=docType, body=query_builder(query["queryTerms"]))
                            rank = 1
                            for hit in res['hits']['hits']:
                                fw.write(query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " + str(hit["_score"]) + " " + indexName + "\n")
                                rank += 1

                        fw.close()
                        res = es.indices.close(index=indexName)

                        print ("Index {0} Completed, Duration: {1} seconds".format(indexName, time.time() - indexLap))
                        indexLap = time.time()

print (" Duration ", time.time()-startTime)
