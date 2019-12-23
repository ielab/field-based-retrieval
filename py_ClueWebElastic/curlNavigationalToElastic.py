import urllib2
import time, lxml.html, re, io, sys
from elasticsearch import Elasticsearch
import base64


#local Setting
qrelPath = "/volumes/data/phd/data/AdHoc2013-2014_eval/qrels.adhoc2013-2014.txt"

#Server Setting
qrelPath = "/volumes/ext/jimmy/data/clueweb12/qrels.adhoc2013-2014.txt"

urlPrefix = "http://boston.lti.cs.cmu.edu/Services/clueweb12_render/renderpage.cgi?id="

navDocs = []
# load  Adhoc 2013-2014 QREL
with open(qrelPath) as f:
    content = f.readline().rstrip()
    while content:
        contentList = content.split(' ')
        documentId = contentList[2]
        score = contentList[3]
        if score == "4": # score = 4 means Navigational Document
            navDocs.append(documentId)
        content = f.readline().rstrip()
f.close

es = Elasticsearch(urls='http://localhost', port=9200, timeout=600)
bulk_size = 4000 #bulk limit in KiloBytes
bulk_count = 1000 #bulk limit in number of files

docType = "clueweb"
indexName = "clueweb12b_all"

startTime = time.time()
bulk_data = []
lapTime = time.time()
indexTime = time.time()

i = 0
totalSize = 0
docId = "-"

#try:
for docId in navDocs:
        print str(i) + " - Processing document Id: " + docId
        request = urllib2.Request(urlPrefix + docId)
        base64string = base64.b64encode('%s:%s' % ('xxxx', 'xxx')) #define password as assigned
        request.add_header("Authorization", "Basic %s" % base64string)
        response = urllib2.urlopen(request)
        #response = urllib2.urlopen(urlPrefix + docId)
        htmlString = response.read()

        htmlString = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\xff]', '', htmlString)
        htmlString = re.sub(r'&\w{4,6};', '', htmlString)
        htmlString = htmlString.replace(",", " ").replace("-", " ").replace(".", " ")

        fContent = io.BytesIO(str(htmlString.decode("utf-8", "ignore")))
        htmlDoc = lxml.html.parse(fContent)

        # the html.xpath return an array so need to convert it to string with join method
        title = " ".join(htmlDoc.xpath('//title/text()'))

        # populate meta by searching for keywords and description
        meta = " ".join(htmlDoc.xpath('//meta[@name="keywords" or @name="description"]/@content'))

        # populate headers by searching for h1, h2, h3, h4, h5 and h6
        raw = htmlDoc.xpath('//body//*[self::h1 or self::h2 or self::h3 or self::h4 or self::h5 or self::h6]')

        headers = ""
        for h in raw:
            headers = headers + " " + h.text_content()

        # Remove non alphanumeric words
        headers = " ".join(word for word in headers.split() if word.isalnum())

        rootClean = htmlDoc.getroot()

        body = " a "
        try:
            body = rootClean.body.text_content()
            body = ' '.join(word for word in body.split() if word.isalnum())
        except:
            body = " a "

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

        bulk_data.append(bulk_meta)
        bulk_data.append(bulk_content)
        totalSize += (sys.getsizeof(content) / 1024)  # convert from bytes to KiloBytes

        i += 1


        '''
        print "********* " + docId
        print "title : "
        print title

        print "meta : "
        print meta

        print "headers : "
        print headers

        print "body : "
        print body
        '''


res = es.bulk(index=indexName, doc_type=docType, body=bulk_data, refresh=False)
bulk_data = []
print (
    "{0} Completed, ({1} seconds), current bulk size: {2}".format(str(i), time.time() - lapTime, totalSize))
lapTime = time.time()
totalSize = 0

#except:
#    print "~~~" + docId
#    print htmlString
#    raise