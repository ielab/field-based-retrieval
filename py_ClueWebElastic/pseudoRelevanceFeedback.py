from elasticsearch import Elasticsearch

import operator

expScheme = 10

titleWeight = 1
metaWeight = 0
headersWeight = 0
bodyWeight = 3
bt = 75
bb = 75
k = 12
weightScheme = str(titleWeight).zfill(2) + str(metaWeight).zfill(2) + str(headersWeight).zfill(2) + str(bodyWeight).zfill(2)

rankTreshold = 20
scoreTreshold = 8
maxLength = 3

#baseResultsFile = "/Volumes/Data/Phd/Data/clef2016_eval/CLEF2016_Results/baselineindriTFIDF_EN_Run1.txt"
topPath = '/volumes/ext/jimmy/data/expWiki_Clef2016_topFiles/'
baseResultsFile = "topExpWikiHealth_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)
prfFile = "/volumes/ext/jimmy/experiments/knowledgeGraph/clueweb12prf_result03_term10"

indexName = "clueweb12_full"
docType = "clueweb"
portSetting = 9200

#load titles and aliases the load as one list: wikiTerms
wikiTitleFile_stem = "/volumes/ext/knowledgeBase/wikipedia/wikiTitles_stem.txt"
wikiAliasFile_stem = "/volumes/ext/knowledgeBase/wikipedia/wikiAliases_stem.txt"
pubmedTitleFile_stem = "/volumes/ext/knowledgebase/pubmed/pubmedTitles_stem.txt"

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


# load titles from the pubmed health
fr = open(pubmedTitleFile_stem, 'r')
pubmedTitles = fr.readlines()
pubmedTitles = [line.strip() for line in pubmedTitles]
fr.close()

#combined the pubmed titles to the wiki terms list
wikiTitles.extend(pubmedTitles)
pubmedTitles = [] #empty the pubmed titles

#remove duplicates
wikiTitles = list(set(wikiTitles))


maxDocRank = 3
numTermsUsed = 10

es = Elasticsearch(urls='http://localhost', port=portSetting, timeout=600)
fw = open (prfFile, 'w')
with open(topPath + baseResultsFile) as rows:
    for row in rows:
        row = row.strip()
        parts = row.split(' ')
        if int(parts[3]) <= maxDocRank:
            print "Query: " + parts[0] + " rank: " + parts[3] + " Doc Id:" + parts[2]

            res = es.termvectors(index=indexName, doc_type=docType, id=parts[2])
            print res
            terms = {}
            for term in res['term_vectors']['body']['terms']:
                terms[term] = res['term_vectors']['body']['terms'][term]['term_freq']
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

