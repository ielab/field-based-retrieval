import time, os, re
from lxml import etree
from elasticsearch import Elasticsearch
from queryBuilderB import b_query_builder

# local setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
#queryFile2012 = '/volumes/data/phd/data/clueweb12_eval/queries.151-200.txt'
queryFile2013 = '/volumes/data/phd/data/clueweb12_eval/web2013.topics.txt'
queryFile2014 = '/volumes/data/phd/data/clueweb12_eval/trec2014-topics.xml'
topPath = '/volumes/data/phd/data/clueweb12_eval/clueWeb_local_topFiles/'
topPrefix = "adhocNav_2013-2014_top"
titleWeights = [0, 1, 3, 5]
metaWeights = [0, 1, 3, 5]
headersWeights = [0, 1, 3, 5]
bodyWeights = [0, 1, 3, 5]
tieBreakers = [0.25]

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
#queryFile2012 = '/volumes/ext/jimmy/data/clueweb12/queries.151-200.txt'
queryFile2013 = '/volumes/ext/jimmy/data/clueweb12/web2013.topics.txt'
queryFile2014 = '/volumes/ext/jimmy/data/clueweb12/trec2014-topics.xml'
topPath = '/volumes/ext/jimmy/data/clueweb12/AdHocNav_2013-2014_topFiles/'
topPrefix = "adhocNav_2013-2014_top"
titleWeights = [0, 1, 3, 5]
metaWeights = [0, 1, 3, 5]
headersWeights = [0, 1, 3, 5]
bodyWeights = [0, 1, 3, 5]
tieBreakers = [0.25]

if not os.path.exists(topPath):
    os.makedirs(topPath)

# Index Setting
docType = "clueweb"
indexName = "clueweb12"

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

for tw in titleWeights:
    for mw in metaWeights:
        for hw in headersWeights:
            for bw in bodyWeights:
                if tw + mw + hw + bw >= 1:
                    for tie in tieBreakers:
                        weights = format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02')

                        fw = open(topPath + topPrefix + "_" + weights + "_" + format(tie*100, '03'), 'w')

                        for query in queries:
                            res = es.search(index=indexName, doc_type=docType, body=b_query_builder(query["queryTerms"], tw, mw, hw, bw, tie))
                            rank = 1
                            for hit in res['hits']['hits']:
                                fw.write(query["queryNumber"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " +
                                         str(hit["_score"]) + " " + indexName + "\n")
                                rank += 1

                            print(weights + " >> Query " + query["queryNumber"] )
                        fw.close()

                        print ("Weights: {0}, Tie: {2} Completed, Duration: {1} seconds".format(weights, time.time() - indexLap, str(tie)))
                        indexLap = time.time()

print (" Duration ", time.time()-startTime)
