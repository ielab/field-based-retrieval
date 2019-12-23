# this script count titlestat_rel as proposed by
# Chris Buckley, Darrin Dimmick, Ian Soboroff, and Ellan Voorhess, "Bias and the Limits of Pooling", SIGIR 2006

import os
from lxml import etree
import re
from elasticsearch import Elasticsearch
from collections import defaultdict
import numpy

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
queryFile = '/volumes/ext/data/Aquaint_hardtrack_2005/2005_HardTrack.topics.txt'
resultPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/'
stopwordFile = "/volumes/ext/indeces/elasticsearch-5.1.1/config/stopwords/terrier-stop.txt"
qrelPath = "/volumes/ext/data/aquaint_eval/TREC2005.qrels.txt"

# Index Setting
docType = "aquaint"
indexName = "aquaint_all"

# load stopwords and remove stopwords from queries
stopwords = []
if not os.path.exists(stopwordFile):
    raise NameError('Stopword File Not Found')
else:
    with open(stopwordFile, 'r') as f:
        for line in f:
            stopwords.append(line.strip())


# load queries to dictionary and remove stopwords
tree = etree.parse(queryFile)
root = tree.getroot()

queries = {}
for top in root.findall('top'):
    queryNumber = top.find('num').text.split(":")[1].strip()
    queryTerms = top.find('title').text.strip()
    queryTerms = re.sub(r'([^\s\w]|_)+', ' ', queryTerms)
    queryTerms = " ".join([word for word in queryTerms.split() if word not in stopwords])

    queries[queryNumber] = queryTerms.lower()

# load relevant documents' title (relevant score >= 1) and title words (unique words in titles)
titleWords = defaultdict(list)
titles = defaultdict(list)
with open(qrelPath, 'r') as f:
    for line in f:
        queryId, temp, docId, relScore = line.split()

        # if relevant
        if int(relScore) >= 1:
            # print('{} - {}'.format(queryId, queries[queryId]))
            words = queries[queryId].split()

            res = es.get(index= indexName, doc_type= docType, id= docId)
            title = res['_source']['title'].lower().replace('\n',' ')
            title = re.sub(r'([^\s\w]|_)+', ' ', title)
            title = title.strip()

            titles[queryId].append(title)
            print('query:{}; rel doc:{} - {}'.format(queryId, docId, title))

            for word in title.split():
                if word not in titleWords[queryId] and word not in stopwords:
                    titleWords[queryId].append(word)

# count fraction of relevant documents that contain each title word
fracTitleContainTitleWord = defaultdict(list)
for queryId in queries.keys():
    # print('Query: {}-{}'.format(queryId, queries[queryId]))
    for word in titleWords[queryId]:
        countTitleContainTitleWord = 0
        for title in titles[queryId]:
            if word in title:
                countTitleContainTitleWord += 1

        print('Query: {}; TitleWord: {}; countTitleContainTitleWord:{}; fraction:{}'.
              format(queryId, word, countTitleContainTitleWord, float(countTitleContainTitleWord)/len(titles[queryId])))

        fracTitleContainTitleWord[queryId].append(float(countTitleContainTitleWord)/len(titles[queryId]))


# normalize by dividing each fractionTitleContainTitleWord with the largest fraction
normFracTitleContainTitleWord = defaultdict(list)
for queryId in queries.keys():
    maxFraction = max(fracTitleContainTitleWord[queryId])
    for fraction in fracTitleContainTitleWord[queryId]:
        normFracTitleContainTitleWord[queryId].append(fraction/maxFraction)
        print('query:{}; max Frac:{}; Frac:{}; normalized Frac:{}'.
              format(queryId, maxFraction, fraction, fraction/maxFraction))

# Average of fraction per query
avgNormFracTitleContainTitleWord = []
for queryId in queries.keys():
    meanFrac = numpy.mean(normFracTitleContainTitleWord[queryId])
    print('query:{}; mean normalized Frac:{}'.format(queryId, meanFrac))
    avgNormFracTitleContainTitleWord.append(meanFrac)

# Average of fraction for collection
meanFrac = numpy.mean(avgNormFracTitleContainTitleWord)
print('avg:{}'.format(meanFrac))


'''
# where titlestat_rel is average relevant documents with query terms in title divided by total number of relevant docs
# stop word should firstly removed from the query.

import os
from lxml import etree
import re
from elasticsearch import Elasticsearch

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=500)
queryFile = '/volumes/ext/data/Aquaint_hardtrack_2005/2005_HardTrack.topics.txt'
resultPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/'
stopwordFile = "/volumes/ext/indeces/elasticsearch-5.1.1/config/stopwords/terrier-stop.txt"
qrelPath = "/volumes/ext/data/aquaint_eval/TREC2005.qrels.txt"

# Index Setting
docType = "aquaint"
indexName = "aquaint_all"

# load stopwords and remove stopwords from queries
stopwords = []
if not os.path.exists(stopwordFile):
    raise NameError('Stopword File Not Found')
else:
    with open(stopwordFile, 'r') as f:
        for line in f:
            stopwords.append(line.strip())


# load queries to dictionary and remove stopwords
tree = etree.parse(queryFile)
root = tree.getroot()

queries = {}
numRelDocs = {}
numRelDocsContainMention = {}
for top in root.findall('top'):
    queryNumber = top.find('num').text.split(":")[1].strip()
    queryTerms = top.find('title').text.strip()
    queryTerms = re.sub(r'([^\s\w]|_)+', ' ', queryTerms)
    queryTerms = " ".join([word for word in queryTerms.split() if word not in stopwords])

    queries[queryNumber] = queryTerms.lower()
    numRelDocs[queryNumber] = 0
    numRelDocsContainMention[queryNumber] = 0
    #print('{} - {}'.format(queryNumber, queries[queryNumber]))


with open(qrelPath, 'r') as f:
    for line in f:
        queryId, temp, docId, relScore = line.split()

        # if relevant
        if int(relScore) >= 1:

            # print('{} - {}'.format(queryId, queries[queryId]))
            words = queries[queryId].split()

            res = es.get(index= indexName, doc_type= docType, id= docId)
            title = res['_source']['title'].lower().replace('\n',' ')
            title = re.sub(r'([^\s\w]|_)+', ' ', title)
            title = title.strip()
            # print('document title: {}'.format(title))
            #print('{} - {}'.format(queryId, queries[queryId]))
            #print('document title: {}'.format(title))


            numRelDocs[queryId] += 1
            numWordsInTitle = 0
            for word in words:
                if word in title:
                    numWordsInTitle += 1

            if numWordsInTitle > 0:
                numRelDocsContainMention[queryId] += 1

            print('Query:{}; Doc:{}-{}; Title Length:{}; Words in Title:{}'.
                  format(queries[queryId], docId, title, len(title.split()),numWordsInTitle))

sum = 0
for queryId in queries.keys():
    print('{} - Rel Docs:{} - Rel Docs with Mentions:{}'.
          format(queries[queryId],numRelDocs[queryId],numRelDocsContainMention[queryId]))
    sum += float(numRelDocsContainMention[queryId])/numRelDocs[queryId]

print(sum)
print('average: {}'.format(float(sum)/len(queries)))
'''