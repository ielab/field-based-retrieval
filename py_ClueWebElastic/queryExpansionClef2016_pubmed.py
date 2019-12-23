import os, re, json, commands
import argparse
from elasticsearch import Elasticsearch
from queryExpansionBuilder import build_query_expansion

#top file legend
#1 use all fields
#2 use original query, terms in title, alias From Matching Title, and categories From Matching Title
#3 use original query only
#4 use original query and alias from matching title
#5 use original query and terms in title
#6 use original query and Related terms From Matching Title
#7 use original query and Title from martching alias
#8 use original query and Title from matching link
#9 use original query and Title contain query term
parser = argparse.ArgumentParser()
parser.add_argument("-s",
                    "--scheme",
                    help="1=all fields \n 2=terms in title, alias From Matching Title, and categories From Matching Title\n 3=original query only \n 4=original query and alias from matching title\n 5=original query and terms in title]\n 6=original query and categories From Matching Title\n 7=original query and Title from martching alias\n 8=original query and Title from matching link\n 9=original query and Title contain query term",
                    type=int,
                    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
args = parser.parse_args()

expScheme = args.scheme
maxLength = 6

# server setting
jsonQueryExpFile = '/volumes/ext/jimmy/data/clef2016_eval/queriesExp_pubmed_len' + str(maxLength) + '_Clef2016.json'
topPath = '/volumes/ext/jimmy/data/expPubMed_Clef2016_topFiles/'

# Best weigthing scheme after tuning the b
titleWeight = 1
metaWeight = 0
headersWeight = 0
bodyWeight = 3
bt = 75
bb = 75
k = 12
weightScheme = str(titleWeight).zfill(2) + str(metaWeight).zfill(2) + str(headersWeight).zfill(2) + str(bodyWeight).zfill(2)


topFileName = "topExpPubMed_" + weightScheme + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)

if not os.path.exists(topPath):
    os.makedirs(topPath)

with open(jsonQueryExpFile, 'r') as fr:
    queries = json.load(fr)
    fr.close()

# Index Setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
docType = "clueweb"
indexName = "clueweb12_full"

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

# print("Configuring index {0} --> b: {1} , k1: {2}".format(indexName, (float(b) / 100), (float(k) / 10)))
res = es.indices.put_settings(index=indexName, body=request_body)
# print("Configured - response: '%s'" % res)

# reopen index after configure the bm25 parameter
res = es.indices.open(index=indexName)
es.cluster.health(index=indexName, wait_for_status='green')  # wait until index ready
print("Opened index {0} --> bt: {1}, bb: {2} , k1: {3}".format(indexName, (float(bt) / 100), (float(bb) / 100),
                                                               (float(k) / 10)))

pattern = re.compile('[\W_]+')

fw = open(topPath + topFileName, 'w')
for query in queries:
    expandedQueries = []

    # terms in initial query is treated as separated terms (OR)
    if expScheme in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
        #print "expand original query"
        for term in query["query"].split():
            expandedQueries.append(pattern.sub(' ',term).strip())

    if expScheme in [1, 9]:
        #print "expand Title contain query terms"
        for term in query["TitleContainTerm"]:
            expandedQueries.append(pattern.sub(' ', term).strip())

    if expScheme in [1, 2, 5, 10]:
        #print "expand terms in Title"
        for term in query["termsInTitle"]:
            expandedQueries.append(pattern.sub(' ', term).strip())

    if expScheme in [1, 2, 4]:
        #print "expand Alias from Matching Title"
        for term in query["aliasFromMatchingTitle"]:
            expandedQueries.append(pattern.sub(' ', term).strip())

    if expScheme in [1, 2, 6, 10]:
        #print "expand Related terms From matching Title"
        for term in query["relatedTermFromMatchingTitle"]:
            expandedQueries.append(pattern.sub(' ', term).strip())

    if expScheme in [1, 7, 10]:
        #print "expand Title From Matching Alias"
        for term in query["titleFromMatchingAlias"]:
            expandedQueries.append(pattern.sub(' ', term).strip())

    if expScheme in [1, 8]:
        #print "expand Title From Matching Link"
        for term in query["titleFromMatchingLink"]:
            expandedQueries.append(pattern.sub(' ', term).strip())



    queryString = build_query_expansion(expandedQueries, titleWeight, metaWeight, headersWeight, bodyWeight, 0.25)

    #print queryString

    res = es.search(index=indexName, doc_type=docType, body=queryString)
    #print res

    rank = 1
    resultString = ""
    for hit in res['hits']['hits']:
        resultString = query["queryId"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " + str(hit["_score"]) + " " + indexName + "\n"
        rank += 1
        fw.write(resultString)

    print "Expansion Scheme:" + str(expScheme) + " Max Exp Length: " + str(maxLength) + " Completed Query Number: " + query["queryId"]

fw.close()

print "Finished Querying"
