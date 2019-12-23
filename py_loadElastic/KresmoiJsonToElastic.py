import time, glob, os, json, sys

from elasticsearch import Elasticsearch

#local setting
es = Elasticsearch(urls='http://localhost', port=9200)
path = '/applications/mamp/htdocs/json_30k/*'
errorPath = '/applications/mamp/htdocs/b_error_30k/'
bulk_size = 5000 #in KiloBytes

#server setting

es = Elasticsearch(urls='http://localhost', port=9200)
path = '/users/jimmy/data/json2015/*'
errorPath = '/volumes/ext/jimmy/data/error_JsonElastic_5_1/*'
bulk_size = 5000 #in KiloBytes

if not os.path.exists(errorPath):
    os.makedirs(errorPath)


indexName = "cleff_full"
docType = "clef"

# Create Index
request_body = {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0,
      "analysis": {
        "analyzer": {
            "my_english": {
                "tokenizer": "standard",
                "filter": ["lowercase", "terrier_stopwords", "porter_stem"]
            }
        },
        "filter": {
          "terrier_stopwords": {
              "type": "stop",
              "stopwords_path": "stopwords/terrier-stop.txt"
          }
        }
      },
      "similarity": {
        "my_bm25": {
            "type": "BM25",
            "b": 0.75,
            "k1": 1.2
        }
      }
    },
    "mappings": {
      docType: {
        "_source": {
            "enabled": False
            },
        "properties": {
            "title": {
                 "type": "text",
                 "similarity": "my_bm25",
                 "analyzer": "my_english",
                "fields": {
                    "length": {
                        "type": "token_count",
                        "store": "yes",
                        "analyzer": "whitespace"
                    }
                }
            },
            "meta": {
                "type": "text",
                "similarity": "my_bm25",
                "analyzer": "my_english",
                "fields": {
                    "length": {
                        "type": "token_count",
                        "store": "yes",
                        "analyzer": "whitespace"
                    }
                }
            },
            "headers": {
                "type": "text",
                "similarity": "my_bm25",
                "analyzer": "my_english",
                "fields": {
                    "length": {
                        "type": "token_count",
                        "store": "yes",
                        "analyzer": "whitespace"
                    }
                }
            },
            "body": {
                "type": "text",
                "similarity": "my_bm25",
                "analyzer": "my_english",
                "fields": {
                    "length": {
                        "type": "token_count",
                        "store": "yes",
                        "analyzer": "whitespace"
                    }
                }
            }
         }
        }
    }
}

startTime = time.time()
bulk_data = []
lapTime = time.time()
indexTime = time.time()

files = glob.glob(path)
totalFiles = len(files)

i = 0.0
errorCount=0
docId = "-"


# Delete if exist
if es.indices.exists(indexName):
    print("deleting '%s' index..." % indexName)
    res = es.indices.delete(index=indexName)
    print(" response: '%s'" % res)

# Create index
print ("creating ", indexName, " index, start at ", startTime)
res = es.indices.create(index=indexName, body=request_body)
print(" response: '%s'" % res)

totalSize = 0

for f in files:
    with open(f, 'r') as fr:
        data = json.load(fr)
        fr.close()
    docId = os.path.basename(f)

    title = "-"
    if data["title"]:
        title = data["title"]

    meta = "-"
    if data["meta"]:
        meta = data["meta"]

    headers = "-"
    if data["headers"]:
        headers = data["headers"]

    body = "-"
    if data["body"]:
        body = data["body"]

    content = title + meta + headers + body
    bulk_meta = {
        "index": {
            "_index": indexName,
            "_type": docType,
            "_id": docId
        }
    }

    bulk_content = {
        'title': title,
        'meta': meta,
        'headers': headers,
        'body': body
    }

    totalSize += (sys.getsizeof(content) / 1024)  # convert from bytes to KiloBytes
    bulk_data.append(bulk_meta)
    bulk_data.append(bulk_content)

    i += 1
    if totalSize > bulk_size:
        res = es.bulk(index=indexName, doc_type=docType, body=bulk_data, refresh=False)
        bulk_data = []
        print ("{0} Completed, ({1} seconds), current bulk size: {2}, Error Count: {3}".format(str(i), time.time() - lapTime, totalSize, errorCount))
        lapTime = time.time()
        totalSize = 0

# load the remainder files
res = es.bulk(index=indexName, doc_type=docType, body=bulk_data, refresh=False)
bulk_data = []
print ("{0} Completed, ({1} seconds), current bulk size: {2}, Error Count: {3}".format(str(i), time.time() - lapTime,
                                                                     totalSize, errorCount))
lapTime = time.time()
i = 0
totalSize=0

print ("Index {0} Completed, Duration: {1} seconds". format(indexName, time.time()- indexTime))
indexTime = time.time()

