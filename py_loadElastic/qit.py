import time, os
from lxml import etree
from elasticsearch import Elasticsearch
from queryBuilderQit import b_query_builder

# local setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
baselineFile = '/volumes/data/phd/data/QIT/clef2015_BaseLine_01010001_25'
queryFile = '/users/n9546031/tools/clef2015/clef2015.test.queries-EN.txt'
resultFile = '/volumes/data/phd/data/QIT/QIT_clef2015'


# server setting
#es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
#queryFile = '/Volumes/ext/jimmy/tools/clef2015/clef2015.test.queries-EN.txt'
#topPath = '/Volumes/ext/jimmy/tools/clef2015/b6_topFiles/'

# Index Setting
indexName = ["b4_clef2015"]
docType = "clef"

limits = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
maxLimit = 100

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

for query in queries:
    with open(baselineFile,"r") as f:
        for line in f:
            queryId=0
            docId=[]

            content=line.spit(',')
            if content== query["queryNumber"] and int(content[2]) <= maxLimit:
                if queryId!=0 and queryId==content[0]:
                    queryId=content[0]
                else:
                    #count QUT for the prev

tw=1
mw=0
hw=0
bw=0
tie=0.25

DocId = ['bupa.2183_12_000621','bupa.2183_12_000632','first1181_12_000830','stret4575_12_000073',
         'stret4575_12_000461','first1181_12_000404','women1681_12_000705','stret4575_12_000021',
         'stret4575_12_000122','stret4575_12_000267','stret4575_12_000136','stret4575_12_000159',
         'stret4575_12_000206','stret4575_12_000253','mayoc3579_12_005119','first1181_12_000200',
         'first1181_12_000175','skinc4434_12_001183','first1181_12_000059','stret4575_12_000042',
         'stret4575_12_000070','stret4575_12_000199','heart3138_12_000039','nhslo3844_12_006773',
         'nhslo3844_12_006778','first1181_12_000191','lymph3532_12_000914','lymph3532_12_001146',
         'stret4575_12_000400','first1181_12_000055','stret4575_12_000055','yorky5007_12_000392',
         'first1181_12_000033','psori4158_12_000080','skinc4434_12_001488','first1181_12_000192',
         'first1181_12_000421','stret4575_12_000056','stret4575_12_000087','first1181_12_000035',
         'first1181_12_000032','first1181_12_000841','first1181_12_000398','first1181_12_000034',
         'healt3100_12_001422','healt3132_12_001238','healt3090_12_001013','aging1838_12_000748',
         'first1181_12_000067']

res = es.search(index=indexName, doc_type=docType, body=b_query_builder("dentist skin cancer", tw, mw, hw, bw, tie, DocId))
rank = 1

print "Total:" + str( res['hits']['total'])
