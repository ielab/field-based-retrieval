import urllib2

qrelPath = "/volumes/data/phd/data/AdHoc2013-2014_eval/qrels.adhoc2013-2014.txt"
urlPrefix = "http://boston.lti.cs.cmu.edu/Services/clueweb12_render/renderpage.cgi?id="
outputPath = "/Volumes/Data/Github/ipm2017_fielded_retrieval/NavigationalPages/"


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


i = 1
totalSize = 0
docId = "-"

for docId in navDocs:
    print str(i) + " - Processing document Id: " + docId
    i+=1
    response = urllib2.urlopen(urlPrefix + docId)
    htmlString = response.read()
    fw = open(outputPath + docId , 'w')
    fw.write(htmlString)
    fw.close()