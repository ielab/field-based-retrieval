from elasticsearch import Elasticsearch
import multiprocessing

# server setting
wikiTitleFile = "/volumes/ext/knowledgebase/wikipedia/wikiInfoBoxHealthLinksTitle.txt"
wikiAliasFile = "/volumes/ext/knowledgebase/wikipedia/wikiInfoBoxHealthLinksRedirects_health.txt"
maxRank = 1000

resultFile = "/volumes/ext/jimmy/experiments/knowledgeGraph/clueweb12Contain_wikiInfoboxHealthLinks_Rank" + str(maxRank)

indexName = "clueweb12b_all"
docType = "clueweb"


# load list of titles and aliases into array
# load titles
wikiTitleAlias = []
with open(wikiTitleFile, "r") as fr:
    for line in fr:
        title=line.strip().lower()
        wikiTitleAlias.append(title)
fr.close()

# append aliases
with open(wikiAliasFile, "r") as fr:
    for line in fr:
        redirectTitle, aliasId, aliasTitle = line.split("~")
        aliasTitle=aliasTitle.strip().lower()
        wikiTitleAlias.append(aliasTitle)
fr.close()


#remove duplicates
wikiTitleAlias = list(set(wikiTitleAlias))

#sort the list
wikiTitleAlias.sort()
totalTitles = len(wikiTitleAlias)

print "Total Wiki titles: " + str(totalTitles)


def searchDocs(wikiTitle):
    docIds = []
    es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
    res = es.search(index=indexName, doc_type=docType,
                    body={
                        "from": 0,
                        "size": maxRank,
                        "_source": False,
                        "query": {
                            "bool": {
                                "must": {
                                    "match": {
                                        "title": {
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
searchResults = p.map(searchDocs, wikiTitleAlias)
for searchResults, processed in searchResults:
    countProcessed += 1
    for result in searchResults:
        docs.append(result)

    if countProcessed % 100 == 0:
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
