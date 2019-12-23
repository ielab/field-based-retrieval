import os, re, json, commands
from elasticsearch import Elasticsearch
from queryExpansionBuilder import build_query_expansion


rankTreshold = 20
scoreTreshold = 8
maxLength = 3

titleWeight = 1
metaWeight = 0
headersWeight = 0
bodyWeight = 3
bt = 75
bb = 75
k = 12
weightScheme = str(titleWeight).zfill(2) + str(metaWeight).zfill(2) + str(headersWeight).zfill(2) + str(bodyWeight).zfill(2)


# local setting
jsonQueryExpFile = '/volumes/data/phd/data/clef2016_eval/queriesSingleExpansion_rank' + str(rankTreshold).zfill(2) + '_Clef2016.json'
topPath = '/volumes/data/phd/data/singleExpWiki_Clef2016_local_topFiles/'

# server setting
jsonQueryExpFile = '/volumes/ext/jimmy/data/clef2016_eval/queriesExpansion_rank' + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + '_Clef2016.json'
topPath = "/volumes/ext/jimmy/data/singleExpWiki_Clef2016_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength)  + "topFiles/"

topFileName = "topSingleExpWiki_Clef2016"

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


for query in queries:
    originalQueries = []

    # load original queries
    for term in query["query"].split():
        originalQueries.append(pattern.sub(' ',term).strip())

    # get list of unique expansion terms from all sources
    expansionTerms = []
    for term in query["TitleContainTerm"]:
        expansionTerms.append(pattern.sub(' ', term).strip())

    for term in query["termsInTitle"]:
        expansionTerms.append(pattern.sub(' ', term).strip())

    for term in query["aliasFromMatchingTitle"]:
        expansionTerms.append(pattern.sub(' ', term).strip())

    for term in query["categoriesFromMatchingTitle"]:
        expansionTerms.append(pattern.sub(' ', term).strip())

    for term in query["titleFromMatchingAlias"]:
        expansionTerms.append(pattern.sub(' ', term).strip())

    for term in query["titleFromMatchingLink"]:
        expansionTerms.append(pattern.sub(' ', term).strip())

    #remove duplicate terms
    expansionTerms = list(set(expansionTerms))

    # expand query by combining original query with each term
    expandedQueries=[]
    for term in expansionTerms:
        fw = open(topPath + topFileName + "_" + query["queryId"]  + "_" + term, 'w')
        #copy the original query as initial terms in the expanded queries
        expandedQueries = list(originalQueries)

        expandedQueries.append(term)

        queryString = build_query_expansion(expandedQueries, titleWeight, metaWeight, headersWeight, bodyWeight, 0.25)

        #print queryString

        res = es.search(index=indexName, doc_type=docType, body=queryString)

        rank = 1
        resultString = ""
        for hit in res['hits']['hits']:
            resultString = query["queryId"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " + str(hit["_score"]) + " " + indexName + "\n"
            rank += 1
            fw.write(resultString)

        fw.close()
        print "Completed Query Number: " + query["queryId"] + " exp term: " + term



print "Finished Querying"
