import lxml.html, time, glob, os

from progressBar import update_progress
from elasticsearch import Elasticsearch
from lxml.html.clean import Cleaner

es = Elasticsearch(urls='http://localhost',port=9200)
path = '/applications/mamp/htdocs/clef2015_30k/*'

#Index Setting
indexName = "hif"
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
               "similarity": "BM25"
            },
            "title": {
                 "type": "string",
                 "similarity": "BM25"
            },
            "author": {
                 "type": "string",
                 "similarity": "BM25"
            }
            ,
            "description": {
                 "type": "string",
                 "similarity": "BM25"
            },
            "keywords": {
                 "type": "string",
                 "similarity": "BM25"
            },
            "h1": {
                 "type": "string",
                 "similarity": "BM25"
            },
            "h2": {
                 "type": "string",
                 "similarity": "BM25"
            },
            "id": {
                 "type": "string",
                 "similarity": "BM25"
            },
            "h3": {
                 "type": "string",
                 "similarity": "BM25"
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
for f in files:
    fr = open(f, 'r')

    fContent = fr.read().replace('\n','')
    fContent = ' '.join(fContent.split())

    html = lxml.html.document_fromstring(fContent.lower())

    docId = os.path.basename(fr.name)

    title = " ".join(html.xpath('//title/text()'))

    # Parse multiple keywords
    keywords = []
    for key in html.xpath('//meta[@name="keywords"]'):
        keywords.append(key.get("content"))

    # Parse multiple authors
    authors = []
    for key in html.xpath('//meta[@name="authors"]'):
        authors.append(key.get("content"))

    # Parse multiple descriptions
    descriptions = []
    for key in html.xpath('//meta[@name="description"]'):
        descriptions.append(key.get("content"))

    h1 = " ".join(html.xpath('//h1/text()'))
    h2 = " ".join(html.xpath('//h2/text()'))
    h3 = " ".join(html.xpath('//h3/text()'))
    html = cleaner.clean_html(html)
    content = html.text_content()
    content = ' '.join(content.split())

    doc = {
        'file':docId,
        'title': title,
        'author': authors,
        'description': descriptions,
        'keyword': keywords,
        'h1': h1,
        'h2': h2,
        'h3': h3,
        'content': content
    }

    res = es.index(index=indexName, doc_type=docType, id=docId, body=doc)

    fr.close()

    i += 1
    update_progress((i/totalFiles), i)

endTime = time.time()
print ("completed at ", endTime, " Duration ", endTime-startTime)







