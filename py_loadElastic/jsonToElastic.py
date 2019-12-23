import time, glob, os, json, sys

from elasticsearch import Elasticsearch

#local setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=300)
path = '/applications/mamp/htdocs/json_1k/*'
errorPath = '/applications/mamp/htdocs/error_1k/'
bulk_size = 5000 #in KiloBytes
buk_count = 1000 #in number of files
titleWeights = [0, 5, 10]
metaWeights = [0, 5, 10]
headersWeights = [1, 5, 10] # headers could not be separated from the body. thus headers min weight is 1 unless the body weight = 0
bodyWeights = [0, 5, 10]

#server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=600)
path = '/users/jimmy/data/json2015/*'
errorPath = '/volumes/ext/jimmy/data/error_JsonElastic_2015/*'
bulk_size = 5000 #in KiloBytes
buk_count = 1000 #in number of files
titleWeights = [0, 1, 3, 5]
metaWeights = [0, 1, 3, 5]
headersWeights = [0, 1, 3, 5] # note: by default: the body will contain header once
bodyWeights = [0, 1, 3, 5]

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
            "content": {
                 "type": "string",
                 "similarity": "BM25",
                 "analyzer": "my_english"
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

for mw in titleWeights:
    for hw in metaWeights:
        for tw in headersWeights:
            for bw in bodyWeights:
                if tw + mw + hw + bw >= 1:
                    try:
                        # Index Setting
                        # indexName format: clef2015_aabbccdd. aa=title weight, bb=meta weight, cc=headers weight, dd=body weight
                        indexName = "clef2015_" + format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02')
                        if indexName in ('clef2015_05050005','clef2015_03050005','clef2015_01050005',
                                         'clef2015_05050003','clef2015_01050003','clef2015_03050003'):
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

                                content = " a " # need to put something to make sure there is a word in a content
                                contentSymbol = ""
                                t = 1
                                while t <= tw:
                                    content = content + " " + data["title"]
                                    contentSymbol += "T"
                                    t += 1

                                m = 1
                                while m <= mw:
                                    content = content + " " + data["meta"]
                                    contentSymbol += "M"
                                    m += 1

                                h = 1
                                while h <= hw:
                                    content = content + " " + data["headers"]
                                    contentSymbol += "H"
                                    h += 1

                                b = 1
                                while b <= bw:
                                    content = content + " " + data["body"]
                                    contentSymbol += "B"
                                    b += 1

                                bulk_meta = {
                                    "index": {
                                        "_index": indexName,
                                        "_type": docType,
                                        "_id": docId
                                    }
                                }

                                bulk_content = {
                                    'content': content
                                }

                                totalSize += (sys.getsizeof(content) / 1024)  # convert from bytes to KiloBytes
                                bulk_data.append(bulk_meta)
                                bulk_data.append(bulk_content)

                                i += 1
                                if totalSize > bulk_size or i % buk_count == 0:
                                    res = es.bulk(index=indexName, doc_type=docType, body=bulk_data, refresh=False)
                                    bulk_data = []
                                    print ("{0} Completed, ({1} seconds), current bulk size: {2}, Error Count: {3}".format(str(i), time.time() - lapTime, totalSize, errorCount))
                                    lapTime = time.time()
                                    totalSize = 0

                            # load the remainder files
                            res = es.bulk(index=indexName, doc_type=docType, body=bulk_data, refresh=False)

                            print ("{0} Completed remainder, ({1} seconds), current bulk size: {2}, Error Count: {3}".format(str(i), time.time() - lapTime,
                                                                                                 totalSize, errorCount))
                            bulk_data = []
                            i = 0
                            totalSize = 0
                            lapTime = time.time()

                            print ("Index {0} Completed, Duration: {1} seconds, Content: {2}". format(indexName, time.time()- indexTime, contentSymbol))
                            indexTime = time.time()
                    except Exception as e:
                        print("unexpected error:", indexName, e)
                        errorCount += 1
                        with open(errorPath + "_" + indexName + "_" + str(errorCount), 'w') as fw:
                            json.dump(bulk_data, fw)
                            fw.close()

