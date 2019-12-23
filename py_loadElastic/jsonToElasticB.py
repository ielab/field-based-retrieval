import time, glob, os, json, sys

from elasticsearch import Elasticsearch

#local setting
es = Elasticsearch(urls='http://localhost', port=9200)
path = '/applications/mamp/htdocs/json_30k/*'
errorPath = '/applications/mamp/htdocs/b_error_30k/'
bulk_size = 5000 #in KiloBytes

#server setting

#es = Elasticsearch(urls='http://localhost', port=9200)
#path = '/users/jimmy/data/json2015/*'
#errorPath = '/volumes/ext/jimmy/data/b4_error_JsonElastic_2015/*'
#bulk_size = 5000 #in KiloBytes

docType = "clef"

if not os.path.exists(errorPath):
    os.makedirs(errorPath)

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
      }
    },
    "mappings": {
      docType: {
        "_source": {
            "enabled": False
            },
        "properties": {
            "title": {
                 "type": "string",
                 "similarity": "BM25",
                 "analyzer": "my_english"
            },
            "meta": {
                "type": "string",
                "similarity": "BM25",
                "analyzer": "my_english"
            },
            "headers": {
                "type": "string",
                "similarity": "BM25",
                "analyzer": "my_english"
            },
            "body": {
                "type": "string",
                "similarity": "BM25",
                "analyzer": "my_english"
            },

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

#try:
# Index Setting
# indexName format: clef2015_aabbccdd. aa=title weight, bb=meta weight, cc=headers weight, dd=body weight
indexName = "cleff_full"

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
#except Exception as e:
#    print("unexpected error:", indexName, docId, e)
#    errorCount += 1
#    with open(errorPath + "_" + indexName + "_" + str(errorCount), 'w') as fw:
#        json.dump(bulk_data, fw)
#        fw.close()

