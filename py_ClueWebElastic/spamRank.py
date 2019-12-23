import gzip
from elasticsearch import Elasticsearch
import time, glob
import multiprocessing

# Server setting
spamRankPath = "/datadrive/waterloo-spam-cw12-decoded"
es = Elasticsearch(urls='http://localhost', port=9200, timeout=600)
bulk_size = 5000 #bulk limit in documents
docType = "clueweb"
indexName = "clueweb12_docs"
portSetting = 9200

finishedFiles = []


def es_update(fname):
    if fname[-22:] not in finishedFiles:
        docCount = 0
        bulk_data = []

        print "Processing compressed file: " + fname
        with gzip.open(fname, mode='rb') as gzf:

            for line in gzf:
                docCount += 1

                spamScore = int(line.split()[0])
                docId = line.split()[1]

                #find whether the docId is indexed. because the index only contains part B not the whole collection
                res = es.get(index=indexName, id=docId, ignore=404)

                #only update if the document id is found
                if str(res['found']) == "True":
                    bulk_meta = {
                        "update" : {
                            "_id" : docId,
                            "_index" : indexName,
                            "_type" : docType
                        }
                    }

                    bulk_content = {
                        "doc" : {
                            "spam_rank" : spamScore
                        }
                    }

                    bulk_data.append(bulk_meta)
                    bulk_data.append(bulk_content)

                    if docCount % bulk_size == 0:
                        res = es.bulk(index=indexName, doc_type=docType, body=bulk_data, refresh=False, ignore=[404])
                        bulk_data = []
                        print (fname[-22:] + "Completed Documents: " + str(docCount) + "Last Id: " + docId)

            # update remainder documents
            if len(bulk_data) > 0:
                res = es.bulk(index=indexName, doc_type=docType, body=bulk_data, refresh=False)
                print (fname[-22:] + " finished update Remainder Documents: " + str(docCount))
    return


startTime = time.time()
lapTime = time.time()


p = multiprocessing.Pool()
resultString = p.map(es_update, glob.glob(spamRankPath + "/*"))
p.close()
p.join()
