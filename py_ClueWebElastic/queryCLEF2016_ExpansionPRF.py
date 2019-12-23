import os, re, json, commands
import argparse
import multiprocessing
from elasticsearch import Elasticsearch
from queryExpansionBuilder import build_query_expansion
import collections
import operator

#10 use original query
parser = argparse.ArgumentParser()
parser.add_argument("-s",
                    "--scheme",
                    help="1=all fields; "
                         "2=terms in title, alias From Matching Title, and categories From Matching Title; "
                         "3=original only; "
                         "4=original and alias from matching title; "
                         "5=original and terms in title]; "
                         "6=original and categories From Matching Title; "
                         "7=original and Title from martching alias; "
                         "8=original and Title from matching link; "
                         "9=original and Title contain query term; "
                         "10=original, terms in title, categories from matching title, title from matching alias and body and PRF; "
                         "11= original and title from matching body; "
                         "12= original and alias from matching body;",
                    type=int,
                    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
args = parser.parse_args()

expScheme = args.scheme
rankTreshold = 20
scoreTreshold = 8
maxLength = 3
targetResults = 1000
maxPage = 10
resultsPerPage =1000

# local setting
#jsonQueryExpFile = '/volumes/data/phd/data/clef2016_eval/queriesExpansion_rank' + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + '_Clef2016.json'
#topPath = '/volumes/data/phd/data/expWiki_Clef2016_local_topFiles/'

# server setting
#jsonQueryExpFile = '/volumes/ext/jimmy/data/clef2016_eval/queriesExpansion_rank' + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + '_Stem_WikiUmls_Clef2016.json'
#jsonQueryExpFile = '/volumes/ext/jimmy/data/clef2016_eval/queriesExpansion_rank' + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + '_Stem_Clef2016.json'
#jsonQueryExpFile = '/volumes/ext/jimmy/data/clef2016_eval/queriesExpansionManualLink_rank' + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + '_Stem_Clef2016.json'



#topPath = '/volumes/ext/jimmy/data/expWikiUmls_Clef2016_topFiles/'
#topPath = '/volumes/ext/jimmy/data/expWiki_Clef2016_topFiles/'

# Best weigthing scheme after tuning the b
titleWeight = 1
metaWeight = 0
headersWeight = 0
bodyWeight = 3
bt = 75
bt = 75
bb = 75
k = 12
weightScheme = str(titleWeight).zfill(2) + str(metaWeight).zfill(2) + str(headersWeight).zfill(2) + str(bodyWeight).zfill(2)

#healthIdFile = "/volumes/ext/jimmy/experiments/knowledgeGraph/clueweb12Contain_wikipubmed_Rank1000"
#prfFile = "prfExpWikiHealth_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)
#prfFile = "prfExpWikiHealthManualLink_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)

#topFileName = "topExpWikiHealth_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)
#topFileName = "topExpWikiHealthManualLink_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)


#prfFile = "prfExpWikiUmlsHealth_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)
#topFileName = "topExpWikiUmlsHealth_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)

# Setup for expansion using wikipedia based on infobox with health type and health links
jsonQueryExpFile = '/volumes/ext/jimmy/data/clef2016_eval/queriesExpansion_rank' + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + '_Stem_WikiInfoboxHealthLinks_Clef2016.json'
topPath = '/volumes/ext/jimmy/data/expWiki_Clef2016_topFiles/'
prfFile = "prfExpWikiInfoboxHealthLinks_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)
topFileName = "topExpWikiInfoboxHealthLinks_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)
healthIdFile = "/volumes/ext/jimmy/experiments/knowledgeGraph/clueweb12Contain_wikiInfoboxHealthLinks_Rank1000"
wikiTitleFile_stem = "/volumes/ext/knowledgeBase/wikipedia/wikiInfoBoxHealthLinksTitle_stem.txt"
wikiAliasFile_stem = "/volumes/ext/knowledgeBase/wikipedia/wikiInfoBoxHealthLinksAlias_stem.txt"
#pubmedTitleFile_stem = "/volumes/ext/knowledgebase/pubmed/pubmedTitles_stem.txt"


if not os.path.exists(topPath):
    os.makedirs(topPath)

if not os.path.exists(jsonQueryExpFile):
    raise "JSON Query Expansion File not found"

with open(jsonQueryExpFile, 'r') as fr:
    queries = json.load(fr)
    fr.close()

# Index Setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=100000)
docType = "clueweb"
indexName = "clueweb12b_all"

# Closing the index, required before changing the index setting
res = es.indices.close(index=indexName)
print("Closed - response: '%s'" % res)

# Setting the index (change the b and k1 of the bm25 simmilarities)

request_body = {
    "settings": {
        "similarity": {
            "sim_title": {
                "type": "BM25",
                "b": (float(bt) / 100),
                "k1": (float(k) / 10)
            },
            "sim_body": {
                "type": "BM25",
                "b": (float(bb) / 100),
                "k1": (float(k) / 10)
            }
        }
    }
}

'''
request_body = {
    "settings": {
        "similarity": {
            "bm25_title": {
                "type": "KLDivergence",
                "mu": float(2400),
                "ad": float(8) #based on title average length
                #"type": "LMDirichlet",
                #"mu": float(2400),
            },
            "bm25_body": {
                "type": "KLDivergence",
                "mu": float(500),
                "ad": float(700) #based on title average length
                #"type": "LMDirichlet",
                #"mu": float(700),
            }
        }
    }
}
'''

# print("Configuring index {0} --> b: {1} , k1: {2}".format(indexName, (float(b) / 100), (float(k) / 10)))
res = es.indices.put_settings(index=indexName, body=request_body)
# print("Configured - response: '%s'" % res)

# reopen index after configure the bm25 parameter
res = es.indices.open(index=indexName)
es.cluster.health(index=indexName, wait_for_status='green')  # wait until index ready
print("Opened index {0} --> bt: {1}, bb: {2} , k1: {3}".format(indexName, (float(bt) / 100), (float(bb) / 100),
                                                               (float(k) / 10)))

pattern = re.compile('[\W_]+')

# load list of documents ID that contain health terms into array of dictionaries
fr = open(healthIdFile, 'r')
rows = fr.readlines()
rows = [line.strip() for line in rows]
healthIds = collections.defaultdict(list)
for row in rows:
    parts=row.split('-')
    healthIds[parts[0]].append(parts[1]+parts[2])
fr.close()


usePrf = True
prfTerms = collections.defaultdict(list)


def es_search(query):
    es1 = Elasticsearch(urls='http://localhost', port=9200, timeout=100000)
    countExpansion = 0
    expandedQueries = []

    # terms in initial query is treated as separated terms (OR)
    if expScheme in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]:
        # print "expand original query"
        for term in query["query"].split():
            expandedQueries.append(pattern.sub(' ', term).strip())

    if expScheme in [1, 9, 10]:
        print "expand Title contain query terms"
        for term in query["TitleContainTerm"]:
            expandedQueries.append(pattern.sub(' ', term).strip())
            countExpansion += 1

    if expScheme in [1, 2, 5, 10]:
        print "expand terms in Title"
        for term in query["termsInTitle"]:
            expandedQueries.append(pattern.sub(' ', term).strip())
            countExpansion += 1

    if expScheme in [1, 2, 4]:
        # print "expand Alias from Matching Title"
        for term in query["aliasFromMatchingTitle"]:
            expandedQueries.append(pattern.sub(' ', term).strip())
            countExpansion += 1

    if expScheme in [1, 2, 6]:
        # print "expand Categories From matching Title"
        for term in query["categoriesFromMatchingTitle"]:
            expandedQueries.append(pattern.sub(' ', term).strip())
            countExpansion += 1

    if expScheme in [1, 7, 10]:
        print "expand Title From Matching Alias"
        for term in query["titleFromMatchingAlias"]:
            expandedQueries.append(pattern.sub(' ', term).strip())
            countExpansion += 1

    if expScheme in [1, 8]:
        # print "expand Title From Matching Link"
        for term in query["titleFromMatchingLink"]:
            expandedQueries.append(pattern.sub(' ', term).strip())
            countExpansion += 1

    if expScheme in [1, 11, 10]:
        print "expand Title From Matching Body"
        for term in query["titleFromMatchingBody"]:
            expandedQueries.append(pattern.sub(' ', term).strip())
            countExpansion += 1

    if expScheme in [1, 12]:
        # print "expand Alias From Matching Body"
        for term in query["aliasFromMatchingBody"]:
            expandedQueries.append(pattern.sub(' ', term).strip())
            countExpansion += 1

    if usePrf:
        # print "expand Alias From Pseudo Relevance Feedback"
        for term in prfTerms[query["queryId"]]:
            expandedQueries.append(term)
            countExpansion += 1


     #print "Expanded queries:" + ', '.join(expandedQueries)
     #print query["queryId"]


    # search until target number of results reached or maximum page reached
    curPage = 1
    rank = 0
    resultString = ""
    while rank < targetResults and curPage <= maxPage:
        queryString = build_query_expansion(expandedQueries, titleWeight, metaWeight, headersWeight, bodyWeight, 0.25, curPage, resultsPerPage)

        # print queryString

        res = es1.search(index=indexName, doc_type=docType, body=queryString)
        # print res

        for hit in res['hits']['hits']:
            # only use the shortlisted health pages if there is expansion
            #if countExpansion > 0:
            parts = hit["_id"].split('-')
            docId = parts[2] + parts[3]
            if docId in healthIds[parts[1]]:
                rank += 1
                resultString += query["queryId"] + " 0 " + str(hit["_id"]) + " " + str(rank) + " " + str(hit["_score"]) + " " + indexName + "\n"

                if rank == targetResults:
                    break

        #print "complete search page: " + str(curPage) + " health results found: " + str(rank)
        curPage += 1

    print "Expansion Scheme:" + str(expScheme) + " Max Exp Length: " + str(maxLength) + \
          " Completed Query Number: " + query["queryId"] + " Health Results found: " + str(rank) + \
          " Page: " + str(curPage - 1)

    return resultString


# Search with expansion queries only, NO Pseudo Relevance Feedback
fw = open(topPath + topFileName, 'w')

usePrf = False
p = multiprocessing.Pool()
resultString = p.map(es_search, queries)
for res in resultString:
    fw.write(res)
    print res
p.close()
p.join()


fw.close()

print "****************** Finished Querying Base Expansion"



# Generate Pseudo Relevance Feedback
#load titles and aliases the load as one list: wikiTerms
fr = open(wikiTitleFile_stem, 'r')
wikiTitles = fr.readlines()
wikiTitles = [line.strip() for line in wikiTitles]
fr.close()

fr = open(wikiAliasFile_stem, 'r')
aliases = fr.readlines()
aliases = [line.strip() for line in aliases]
fr.close()

#combined the titles to the wiki terms list
wikiTitles.extend(aliases)
aliases = [] #empty the aliases


# get total number of documents in the index. this will be used to rank terms based on TF.IDF
res = es.count(index=indexName, doc_type=docType)
docCount = int(res['count'])

'''
# load titles from the pubmed health
fr = open(pubmedTitleFile_stem, 'r')
pubmedTitles = fr.readlines()
pubmedTitles = [line.strip() for line in pubmedTitles]
fr.close()

#combined the pubmed titles to the wiki terms list
wikiTitles.extend(pubmedTitles)
pubmedTitles = [] #empty the pubmed titles
'''
#remove duplicates
wikiTitles = list(set(wikiTitles))

maxDocRank = 3
numTermsUsed = 10

# body for requesting term vector on the field body only
requestVector_body = {
   "fields": ["body"],
  "offsets": False,
  "positions": False,
  "term_statistics": True,
  "field_statistics": False
}

fw = open (prfFile, 'w')
with open(topPath + topFileName) as rows:
    for row in rows:
        row = row.strip()
        parts = row.split(' ')
        if int(parts[3]) <= maxDocRank:
            print "Query: " + parts[0] + " rank: " + parts[3] + " Doc Id:" + parts[2]

            res = es.termvectors(index=indexName, doc_type=docType, id=parts[2], body=requestVector_body)

            terms = {}
            for term in res['term_vectors']['body']['terms']:

                term_frequency = res['term_vectors']['body']['terms'][term]['term_freq']
                doc_frequency = res['term_vectors']['body']['terms'][term]['doc_freq']

                # evaluate term based on tf.idf
                terms[term] = term_frequency * (docCount / doc_frequency)

                # evaluate term based on term frequency only
                #terms[term] =  term_frequency

                #print term + " : " + str(res['term_vectors']['body']['terms'][term]['term_freq'])

            terms = sorted(terms.items(), key=operator.itemgetter(1), reverse=True)

            i = 0
            termFound = 0
            while i < len(terms) and termFound < numTermsUsed:
                if terms[i][0] in wikiTitles:
                    # print terms[i][0]
                    fw.write(parts[0] + " " + terms[i][0] + "\n")
                    termFound += 1
                i += 1

fw.close()

print "****************** Finished generating Pseudo Relevance Feedback"



# load pseudo relevance feedback terms to a dictionary
if not os.path.exists(prfFile):
    raise "Pseudo Relevance Feedbak File not found"


with open(prfFile, 'r') as fr:
    for line in fr:
        parts = line.split(" ")
        prfTerms[parts[0]].append(parts[1].strip())

# Search with expansion queries only AND Pseudo Relevance Feedback
fw = open(topPath + topFileName + "_prf", 'w')

usePrf = True
p = multiprocessing.Pool()
resultString = p.map(es_search, queries)
for res in resultString:
    fw.write(res)
p.close()
p.join()


fw.close()

print "****************** Finished Querying Expansion and PRF"
