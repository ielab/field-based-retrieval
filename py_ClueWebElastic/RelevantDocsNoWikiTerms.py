import collections
from elasticsearch import Elasticsearch

es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)

healthIdFile = "/volumes/ext/jimmy/experiments/knowledgeGraph/clueweb12Contain_wikipubmed_Rank1000"
qrelPath = "/volumes/ext/jimmy/data/clef2016_eval/task1.qrels.30Aug"

indexName = "clueweb12_title"
docType = "clueweb"


# load list of documents ID that contain health terms into array of dictionaries
fr = open(healthIdFile, 'r')
rows = fr.readlines()
rows = [line.strip() for line in rows]
healthIds = collections.defaultdict(list)
for row in rows:
    parts=row.split('-')
    healthIds[parts[0]].append(parts[1]+parts[2])
fr.close()

# load list of Document ID that are judged as relevant as in the QREL file
fr = open(qrelPath, 'r')
rows = fr.readlines()
rows = [line.strip() for line in rows]

for row in rows:
    parts=row.split(' ')
    if parts[3] != '0':
        idParts = parts[2].split('-')
        docId = idParts[2] + idParts[3]

        if docId not in healthIds[idParts[1]]:
            res = es.search(index=indexName, doc_type=docType,
                            body={
                                "_source": ["title"],
                                "query": {
                                     "terms": {
                                         "_id": [parts[2]]
                                     }
                                }
                            })

        for hit in res['hits']['hits']:
            print "Not Found Query Num:" + parts[0] + " Rel Score: " + parts[3] + " Relevant Doc: " + parts[2] + " - " + hit["_source"]["title"]
fr.close()