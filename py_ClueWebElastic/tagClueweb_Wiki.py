from elasticsearch import Elasticsearch
import multiprocessing
import re

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
titleFile = "/volumes/ext/knowledgeBase/wikipedia/wikiTitles.txt"
aliasFile = "/volumes/ext/knowledgeBase/wikipedia/wikiAliases.txt"

indexName = "clueweb12_title"
docType = "clueweb"

targetIndex = "clueweb12_full"

docsPerPage = 10000
maxWindowSize = 3
minChar = 3 #minimum character in a gram to be considered as health term
#totalDocs = int(es.count(index=indexName,doc_type=docType)["count"])

def countLetters(term):
    count = 0
    for char in term:
        if char.isalpha():
            count += 1
    return count

#load titles and aliases the load as one list: wikiTerms
fr = open(titleFile, 'r')
wikiTerms = fr.readlines()
wikiTerms = [line.strip() for line in wikiTerms]
fr.close()


fr = open(aliasFile, 'r')
aliases = fr.readlines()
aliases = [line.strip() for line in aliases]
fr.close()

#combined the titles to the wiki terms list
wikiTerms.extend(aliases)
aliases = [] #empty the aliases

#remove duplicates
wikiTerms = list(set(wikiTerms))

#print "total" + str(len(wikiTerms))

pattern = re.compile('[\W_]+')

def tag_documents(hit):
    #for hit in hits:

    docId = hit['_id']
    docTitle = pattern.sub(' ', hit["_source"]["title"]).lower().strip()

    # print docId + " : " + docTitle
    hasHealthTerm = 0
    qWords = docTitle.split()
    for windowSize in xrange(maxWindowSize, 0, -1):
        nGrams = []
        if len(qWords) >= windowSize:
            temp = zip(*[qWords[i:] for i in range(windowSize)])
            for t in temp:
                nGrams.append(" ".join(t))

            for gram in nGrams:
                if countLetters(gram) >= minChar:
                    if gram in wikiTerms:
                        #print "Found in: " + docId + " doc Title: " + docTitle + " grams: " + gram
                        '''
                        es1 = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
                        es1.update(index=targetIndex,
                                          doc_type=docType,
                                          id=docId,
                                          body={"doc": {"wikihealth": 1}})
                        '''
                        return 1, 1

    return 1, 0



# need to search use scroll method since we expect large results (>10.000)
res = es.search(index=indexName,
                doc_type=docType,
                scroll='2m',
                sort='_doc',
                size=docsPerPage,
                body={
                    "query": {
                        "match_all": {}
                    }
                })

sid = res['_scroll_id'] #grab the initial scroll id
scroll_size = res['hits']['total']

#start scrolling
jobs = []
numProcessed = 0
numTagged = 0
while (scroll_size>0):


    #clone the results list to a new instance of hits list
    hits = list(res['hits']['hits'])

    p = multiprocessing.Pool()
    temp = p.map(tag_documents, hits)
    for processed, tagged in temp:
        numProcessed += processed
        numTagged += tagged
        if numProcessed % 1000 == 0:
            print "Processed: " + str(numProcessed) + " Tagged: " + str(numTagged)
    p.close()
    p.join()

    res = es.scroll(scroll_id=sid, scroll='2m')
    sid = res['_scroll_id']  # get the updated scroll id
    scroll_size = len(res['hits']['hits'])  # check number of results in current scroll


    '''
    p = multiprocessing.Process(target=tag_documents, args=(hits,))
    jobs.append(p)
    p.start()
    '''





'''
def tagClueweb(startFrom):
    res = es.search(index=indexName, doc_type=docType,
                    body={
                        "from": startFrom*docsPerPage,
                        "size": docsPerPage,
                        "query": {
                            "match_all": {}
                        }
                    })

    for hit in res['hits']['hits']:
        docId = hit['_id']
        docTitle = pattern.sub(' ', hit["_source"]["title"]).lower().strip()

        #print docId + " : " + docTitle

        qWords = docTitle.split()
        for windowSize in xrange(maxWindowSize, 0, -1):
            nGrams = []
            if len(qWords) >= windowSize:
                temp = zip(*[qWords[i:] for i in range(windowSize)])
                for t in temp:
                    nGrams.append(" ".join(t))

                for grams in nGrams:
                    if grams in wikiTerms:
                        print "Found in: " + docId + " grams: " + grams



p = multiprocessing.Pool()
resultString = p.map(tagClueweb, range(0, (totalDocs/docsPerPage)+1))
p.close()
p.join()
'''
