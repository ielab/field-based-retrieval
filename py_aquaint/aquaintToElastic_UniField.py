import gzip
import time
import glob
import re
import sys
from elasticsearch import Elasticsearch
from lxml import etree

# Server setting
#docPath = "/volumes/ext/data/aquaint/"
docPath = "/volumes/Data/Phd/Data/aquaint_docs/"
finishedFolders = []

es = Elasticsearch(urls='http://localhost', port=9200, timeout=600)

# bulk size in size and file count
bulk_size = 4000
bulk_count = 1000

docType = "aquaint"
indexName = "aquaint_unifield"

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
        "bm25_uni": {
            "type": "BM25",
            "b": 0.75,
            "k1": 1.2
        }
      }
    },
    "mappings": {
      docType: {
        "_source": {
            "enabled": True
            },
        "properties": {
            "titlebody": {
                 "type": "text",
                 "similarity": "bm25_uni",
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

# Delete if exist
if es.indices.exists(indexName):
    print("deleting '%s' index..." % indexName)
    res = es.indices.delete(index=indexName)
    print(" response: '%s'" % res)

# Create index
if not es.indices.exists(indexName):
    print ("creating ", indexName, " index, start at ", startTime)
    res = es.indices.create(index=indexName, body=request_body)
    print(" response: '%s'" % res)

i = 0
totalSize = 0
docNo = ""
headline = ""
text = ""

sourceFolders = glob.glob(docPath + "/*")
for sourceFold in sourceFolders:
    print "****************   Processing Source: " + sourceFold
    yearFolders = glob.glob(sourceFold + "/*")
    for yearFold in yearFolders:
        print "Processing Year folder: " + yearFold
        if yearFold not in finishedFolders:
            print "Processing folder: " + yearFold
            for f in glob.glob(yearFold + "/*"):
                print "Processing file: " + f
                with gzip.open(f, mode='rb') as gzf:
                    temp = gzf.read()
                    temp = re.sub(r'&\w{2,6};', '', temp)
                    temp = temp.replace("<P>", " ").replace("</P>", " ")
                    content = "<ROOT>" + temp + "</ROOT>"
                    try:
                        root = etree.fromstring(content)

                        for doc in root.findall('DOC'):
                            docNo = ""
                            headline = ""
                            text = ""

                            docNo = doc.find('DOCNO').text.strip()
                            body = doc.find('BODY')

                            try:
                                headline = body.find('HEADLINE').text
                            except:
                                headline = "a"

                            try:
                                text = body.find('TEXT').text
                            except:
                                text = "a"

                            content = headline + text
                            bulk_meta = {
                                "index": {
                                    "_index": indexName,
                                    "_type": docType,
                                    "_id": docNo
                                }
                            }

                            bulk_content = {
                                'titlebody': headline + " " + text
                            }

                            bulk_data.append(bulk_meta)
                            bulk_data.append(bulk_content)
                            totalSize += (sys.getsizeof(content) / 1024)  # convert from bytes to KiloBytes

                            i += 1
                            if totalSize > bulk_size or i % bulk_count == 0:
                                res = es.bulk(index=indexName, doc_type=docType, body=bulk_data, refresh=False)
                                bulk_data = []
                                print ("{0} Completed, ({1} seconds), bulk size: {2}".format(str(i), time.time() -
                                                                                             lapTime, totalSize))
                                lapTime = time.time()
                                totalSize = 0
                    except:
                        print "~~~" + docNo
                        print content
                        raise

if len(bulk_data) > 0:
    # load the remainder files
    res = es.bulk(index=indexName, doc_type=docType, body=bulk_data, refresh=False)
    bulk_data = []
    print ("{0} Remainder Completed, ({1} seconds), bulk size: {2}".format(str(i), time.time() - lapTime, totalSize))
