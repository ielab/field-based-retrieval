import gzip
import warc
import time, glob, os, lxml.html, re, io, sys
from elasticsearch import Elasticsearch
import multiprocessing


# server setting
warcPath = "/datadrive/DiskB/"
warcFolder = [ 'ClueWeb12_18', 'ClueWeb12_19']

finishedFolders = []

#errorPath = '/Volumes/Data/Phd/Data/Clueweb12/warcToElasticError/'
es0 = Elasticsearch(urls='http://localhost', port=9200, timeout=600)
bulk_size = 2000 #bulk limit in KiloBytes
bulk_count = 1000 #bulk limit in number of files

docType = "clueweb"
indexName = "clueweb12_docs"



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
        "bm25_title": {
            "type": "BM25",
            "b": 0.75,
            "k1": 1.2
        },
        "bm25_body": {
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
            "title": {
                 "type": "text",
                 "similarity": "bm25_title",
                 "analyzer": "my_english"
            },
            "body": {
                "type": "text",
                "similarity": "bm25_body",
                "analyzer": "my_english"
            },

         }
        }
    }
}

startTime = time.time()
# indexTime = time.time()

# Delete if exist
#if es0.indices.exists(indexName):
#    print("deleting '%s' index..." % indexName)
#    res = es.indices.delete(index=indexName)
#    print(" response: '%s'" % res)

# Create index
if not es0.indices.exists(indexName):
    print ("creating ", indexName, " index, start at ", startTime)
    res = es0.indices.create(index=indexName, body=request_body)
    print(" response: '%s'" % res)


docId = "-"

def es_index(fname):
    i = 0
    totalSize = 0
    bulk_data = []
    lapTime = time.time()
    es = Elasticsearch(urls='http://localhost', port=9200, timeout=600)

    print "Processing file: " + fname
    with gzip.open(fname, mode='rb') as gzf:
        WarcTotalDocuments = 0
        EmptyDocuments = 0
        for record in warc.WARCFile(fileobj=gzf):
            if record.header.get('WARC-Type').lower() == 'warcinfo':
                WarcTotalDocuments = record.header.get('WARC-Number-Of-Documents')

            if record.header.get('WARC-Type').lower() == 'response':
                docId = record.header.get('WARC-Trec-ID')
                docString = record.payload.read()

                htmlStart = 0
                htmlStart = docString.find('<html')
                if htmlStart < 1:
                    htmlStart = docString.find('<HTML')
                if htmlStart < 1:
                    htmlStart = docString.find('<Html')

                if htmlStart < 1:
                    EmptyDocuments += 1
                else:
                    htmlString = docString[htmlStart:]

                    htmlString = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\xff]', '', htmlString)
                    htmlString = re.sub(r'&\w{4,6};', '', htmlString)
                    htmlString = htmlString.replace(",", " ").replace("-", " ").replace(".", " ")

                    fContent = io.BytesIO(str(htmlString.decode("utf-8", "ignore")))

                    try:
                        htmlDoc = lxml.html.parse(fContent)

                        # the html.xpath return an array so need to convert it to string with join method
                        title = " ".join(htmlDoc.xpath('//title/text()'))

                        # populate meta by searching for keywords and description
                        #meta = " ".join(htmlDoc.xpath('//meta[@name="keywords" or @name="description"]/@content'))

                        # populate headers by searching for h1, h2, h3, h4, h5 and h6
                        '''
                        raw = htmlDoc.xpath(
                            '//body//*[self::h1 or self::h2 or self::h3 or self::h4 or self::h5 or self::h6]')

                        headers = ""
                        for h in raw:
                            headers = headers + " " + h.text_content()

                        # Remove non alphanumeric words
                        headers = " ".join(word for word in headers.split() if word.isalnum())
                        '''

                        rootClean = htmlDoc.getroot()

                        try:
                            body = " a "
                            body = rootClean.body.text_content()
                            body = ' '.join(word for word in body.split() if word.isalnum())
                        except:
                            body = " a "

                        content = title + body
                        bulk_meta = {
                            "index": {
                                "_index": indexName,
                                "_type": docType,
                                "_id": docId
                            }
                        }

                        bulk_content = {
                            'title': title,
                            'body': body
                        }

                        bulk_data.append(bulk_meta)
                        bulk_data.append(bulk_content)
                        totalSize += (sys.getsizeof(content) / 1024)  # convert from bytes to KiloBytes

                        i += 1
                        if totalSize > bulk_size or i % bulk_count == 0:
                            res = es.bulk(index=indexName, doc_type=docType, body=bulk_data, refresh=False)
                            bulk_data = []
                            #print ("{0} Completed, ({1} seconds), current bulk size: {2}".format(str(i), time.time() - lapTime,totalSize))
                            #lapTime = time.time()
                            totalSize = 0
                    except:
                        print "~~~" + docId
                        print htmlString
                        raise

        if len(bulk_data) > 0:
            # load the remainder files
            res = es.bulk(index=indexName, doc_type=docType, body=bulk_data, refresh=False)
            bulk_data = []
            #print ("{0} Remainder Completed, ({1} seconds), current bulk size: {2}".format(str(i), time.time() - lapTime,totalSize))

        print ("File {0} Completed, Duration: {1} sec, Total: {2}, Processed: {3}, Empty: {4}, Variance: {5}".
               format(fname, time.time() - lapTime, WarcTotalDocuments, str(i), str(EmptyDocuments),str(int(WarcTotalDocuments) - i - EmptyDocuments)))
        #indexTime = time.time()
        #i = 0
        #WarcTotalDocuments = 0


for warcFold in warcFolder:
    if not os.path.exists(warcPath + warcFold + "/"):
        print "warc folder not found: " + warcPath + warcFold
    folders = glob.glob(warcPath + warcFold + "/*")
    print "****************   Processing Path: " + warcFold
    for fold in folders:
        print "Checking folder: " + fold
        if fold[-2:] == "wb" and fold[-6:] not in finishedFolders:
            print "Processing folder: " + fold
            #for f in glob.glob(fold + "/*"):
            p = multiprocessing.Pool()
            resultString = p.map(es_index, glob.glob(fold + "/*"))
            p.close()
            p.join()