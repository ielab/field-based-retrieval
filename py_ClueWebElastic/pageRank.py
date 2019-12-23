from elasticsearch import Elasticsearch

# Server setting
pageRankFile = "/datadrive/pagerank.docNameOrder"
es = Elasticsearch(urls='http://localhost', port=9200, timeout=600)
bulk_size = 1000 #bulk limit in documents
docType = "clueweb"
indexName = "clueweb12_docs"
portSetting = 9200

docCount = 0
lineCount = 0
bulk_data = []
with open(pageRankFile, "r") as inFile:
    for line in inFile:
        lineCount += 1
        if lineCount % 100000 == 0:
            print "Line processed: " + str(lineCount)

        parts = line.split()
        docId = parts[0]
        pageRank = float(parts[1])

        #Currently we do not index the twitter collection
        if "tw-" not in docId:
            # find whether the docId is indexed. because the index only contains part B not the whole collection
            res = es.get(index=indexName, id=docId, ignore=404)
            #print docId + " : " + str(res['found'])

            # only update if the document id is found
            if str(res['found']) == "True":
                #print ("Found: " + docId)
                docCount += 1
                bulk_meta = {
                    "update": {
                        "_id": docId,
                        "_index": indexName,
                        "_type": docType
                    }
                }

                bulk_content = {
                    "doc": {
                        "page_rank": pageRank
                    }
                }

                bulk_data.append(bulk_meta)
                bulk_data.append(bulk_content)

                if docCount % bulk_size == 0:
                    res = es.bulk(index=indexName, doc_type=docType, body=bulk_data, refresh=False, ignore=[404])
                    bulk_data = []
                    print ("Completed Documents: " + str(docCount) + "Last Id: " + docId)

# update remainder documents
if len(bulk_data) > 0:
    res = es.bulk(index=indexName, doc_type=docType, body=bulk_data, refresh=False)
    print ("finished update Remainder Documents: " + str(docCount))
