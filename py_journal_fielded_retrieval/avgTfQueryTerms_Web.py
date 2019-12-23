import os
from lxml import etree
from elasticsearch import Elasticsearch
import re
from queryBuilderB import b_query_builder

# server setting
queryFile2013 = '/volumes/ext/jimmy/data/clueweb12/web2013.topics.txt'
queryFile2014 = '/volumes/ext/jimmy/data/clueweb12/trec2014-topics.xml'
stopwordFile = "/volumes/ext/indeces/elasticsearch-5.1.1/config/stopwords/terrier-stop.txt"
dataFolder = "/volumes/ext/jimmy/experiments/journal_KB_Expansion/data/"
outputFile = "avgTF_web.txt"

docType = "clueweb"
indexName = "clueweb12b_all"

# load stopwords and remove stopwords from queries
stopwords=[]
if not os.path.exists(stopwordFile):
    raise NameError('Stopword File Not Found')
else:
    with open(stopwordFile, 'r') as f:
        for line in f:
            stopwords.append(line.strip())

# load queries
queries = []

# load  Adhoc 2013 queries
with open(queryFile2013) as f:
    content = f.readline().rstrip()
    while content:
        contentList = content.split(':')
        queryNumber = contentList[0]
        queryTerms = re.sub(r'([^\s\w]|_)+', ' ', contentList[1])

        queryData = {"queryNumber": queryNumber, "queryTerms": queryTerms, "sumTfTitle": 0, "sumTfBody": 0}
        queries.append(queryData)
        content = f.readline().rstrip()

# load  Adhoc 2014 queries
tree = etree.parse(queryFile2014)
webTrack = tree.getroot()
queryTerms = ""
for topic in webTrack.iter("topic"):
    queryNumber = topic.attrib["number"]
    for detail in topic:
        if detail.tag == "query":
            queryTerms = re.sub(r'([^\s\w]|_)+', ' ', detail.text)

    queryData = {"queryNumber": queryNumber, "queryTerms": queryTerms, "sumTfTitle": 0, "sumTfBody": 0}
    queries.append(queryData)

# Index Setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=100000)

fw = open(dataFolder + outputFile, 'w')
fw.write("QueryNum, avgTtfTitle, avgTtfBody \n")
for query in queries:
    print('QNum {} : {}'.format(query['queryNumber'],query['queryTerms']))

    # ****** Search for a document with TITLE contain the current term
    requestVector_title = {
        "doc": {"title": query['queryTerms']},
        "fields": ["title"],
        "offsets": False,
        "positions": False,
        "term_statistics": True,
        "field_statistics": False
    }
    res = es.termvectors(index=indexName, doc_type=docType, body=requestVector_title)

    termsCount_title = len(res["term_vectors"]["title"]["terms"])

    for term, stats in res["term_vectors"]["title"]["terms"].items():
        #print("term: {} : {}".format(term, stats["ttf"]))
        if "ttf" in stats:
            query["sumTfTitle"] += stats["ttf"]
        else:
            termsCount_title -= 1


    # ****** Search for a document with BODY contain the current term
    requestVector_body = {
        "doc": {"body": query['queryTerms']},
        "fields": ["body"],
        "offsets": False,
        "positions": False,
        "term_statistics": True,
        "field_statistics": False
    }
    res = es.termvectors(index=indexName, doc_type=docType, body=requestVector_body)

    termsCount_body = len(res["term_vectors"]["body"]["terms"])

    for term, stats in res["term_vectors"]["body"]["terms"].items():
        #print("term: {} : {}".format(term, stats["ttf"]))
        if "ttf" in stats:
            query["sumTfBody"] += stats["ttf"]
        else:
            termsCount_body -= 1

    avgTTF_Title = float(query["sumTfTitle"])/termsCount_title
    avgTTF_Body = float(query["sumTfBody"])/termsCount_body
    fw.write(query["queryNumber"] + "," + str(avgTTF_Title) + "," + str(avgTTF_Body) + "\n")
    print("finish:{} avgTTF_Title:{} avgTTF_Body:{}".format(query["queryNumber"], avgTTF_Title, avgTTF_Body))

fw.close()