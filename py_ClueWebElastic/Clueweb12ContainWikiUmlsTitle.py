from elasticsearch import Elasticsearch
import multiprocessing
import re

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
wikiUmlsTitleFile = "/volumes/ext/knowledgeBase/wikipedia/wikiUmlsTitle.txt"

maxRank = 1000

resultFile = "/volumes/ext/jimmy/experiments/knowledgeGraph/clueweb12Contain_wikiumls_Rank" + str(maxRank)

indexName = "clueweb12b_all"
docType = "clueweb"


#load titles and aliases the load as one list: wikiTerms

fr = open(wikiUmlsTitleFile, 'r')
wikiTitles = fr.readlines()
wikiTitles = [line.strip() for line in wikiTitles]
fr.close()

print "Total Wiki titles: " + str(len(wikiTitles))


def searchDocs(wikiTitle):
    docIds = []
    res = es.search(index=indexName, doc_type=docType,
                    body={
                        "from": 0,
                        "size": maxRank,
                        "_source": False,
                        "query": {
                            "bool": {
                                "must": {
                                    "match": {
                                        "body": {
                                            "query": wikiTitle,
                                            "minimum_should_match": "100%"
                                        }
                                    }
                                }
                            }
                        }
                    })

    for hit in res['hits']['hits']:
        parts=hit["_id"].split('-')
        docIds.append(parts[1] + "-" + parts[2] + "-" + parts[3])

    print wikiTitle
    return docIds, 1


countProcessed = 0


docs = []

p = multiprocessing.Pool()
searchResults = p.map(searchDocs, wikiTitles)
for searchResults, processed in searchResults:
    countProcessed += 1
    for result in searchResults:
        docs.append(result)

    if countProcessed % 10 == 0:
        print "Processed: " + str(countProcessed)
p.close()
p.join()

print "Total related Docs with duplicates: " + str(len(docs))
#remove duplicates
docs = list(set(docs))

print "Total unique related Docs : " + str(len(docs))

fw = open(resultFile, 'w')
for doc in docs:
    fw.write(doc + "\n")
fw.close()



