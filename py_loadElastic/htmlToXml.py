import lxml.html, time, glob, os

from progressBar import update_progress
from elasticsearch import Elasticsearch
from lxml.html.clean import Cleaner

es = Elasticsearch(urls='http://localhost',port=9200)
path = '/applications/mamp/htdocs/clef2015_30k/*'

#Index Setting
indexName = "hif30k"
docType = "clef"

#Delete if exist
if es.indices.exists(indexName):
    print("deleting '%s' index..." % (indexName))
    res = es.indices.delete(index=indexName)
    print(" response: '%s'" % (res))

#Create Index
request_body = {
    "settings": {
      "number_of_shards": 2,
      "number_of_replicas": 1
   },
    "mappings": {
      docType: {
        "_source": {
            "enabled": False
            },
        "properties": {
            "file": {
               "type": "string",
               "index": "not_analyzed"
            },
            "content": {
                 "type": "string",
                 "similarity": "BM25"
            }
         }
        }
    }
}


startTime = time.time()
print ("creating ", indexName, " index, start at ", startTime)
res = es.indices.create(index=indexName, body=request_body)
print(" response: '%s'" % res)


cleaner = Cleaner()
cleaner.javascript = True
cleaner.style = True
cleaner.scripts = True
cleaner.comments = True

files = glob.glob(path)
totalFiles = len(files)

i = 0.0
bulk_size = 100
bulk_data= []

for f in files:
    fr = open(f, 'r')

    fContent = fr.read().replace('\n',' ')
    fContent = ' '.join(fContent.split())

    html = lxml.html.document_fromstring(fContent.lower())

    docId = os.path.basename(fr.name)

    # the html.xpath return an array so need to convert it to string with join method
    title = " ".join(html.xpath('//title/text()'))

    # populate meta by searching for keywords and description
    meta = ""
    for key in html.xpath('//meta[@name="keywords"][@name="description"]'):
        meta = meta.join(key.get("content"))

    # populate headers by searching for h1, h2, h3, h4, h5 and h6
    headers = " ".join(
        html.xpath('//body/*[self::h1 or self::h2 or self::h3 or self::h4 or self::h5 or self::h6]/text()'))

    body = " ".join(html.xpath('//body/text()'))

    content = title + " " + meta + " " + headers + " " + body

    bulk_meta = {
        "index": {
            "_index": indexName,
            "_type": docType,
            "_id": docId
        }
    }

    bulk_content = {
        'file':docId,
        'content': content
    }

    bulk_data.append(bulk_meta)
    bulk_data.append(bulk_content)

    fr.close()

    i += 1
    if i % bulk_size == 0:
        res = es.bulk(index=indexName, doc_type=docType, body=bulk_data, refresh=False)
        bulk_data = []
        print (i, " completed")

print (" Duration ", time.time()-startTime)