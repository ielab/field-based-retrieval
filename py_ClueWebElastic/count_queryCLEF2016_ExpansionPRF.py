import os
import re
import json

rankTreshold = 20
scoreTreshold = 8
maxLength = 3
targetResults = 1000
maxPage = 10
resultsPerPage =1000

# local setting
jsonQueryExpFile = '/volumes/data/phd/data/clef2016_eval/queriesExpansion_rank' + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + '_Clef2016.json'
topPath = '/volumes/data/phd/data/expWiki_Clef2016_local_topFiles/'

# server setting
jsonQueryExpFile = '/volumes/ext/jimmy/data/clef2016_eval/queriesExpansion_rank' + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + '_Stem_Clef2016.json'
topPath = '/volumes/ext/jimmy/data/expWiki_Clef2016_topFiles/'
resultFileName = 'count_queriesExpansion_rank' + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + '_Stem_Clef2016_'

# Best weigthing scheme after tuning the b
titleWeight = 1
metaWeight = 0
headersWeight = 0
bodyWeight = 3
bt = 75
bb = 75
k = 12
weightScheme = str(titleWeight).zfill(2) + str(metaWeight).zfill(2) + str(headersWeight).zfill(2) + str(bodyWeight).zfill(2)

if not os.path.exists(topPath):
    os.makedirs(topPath)

if not os.path.exists(jsonQueryExpFile):
    raise "JSON Query Expansion File not found"

with open(jsonQueryExpFile, 'r') as fr:
    queries = json.load(fr)
    fr.close()

pattern = re.compile('[\W_]+')


for expScheme in [4, 5, 6, 7, 8, 9, 10, 11, 12]:
    fw = open(topPath + resultFileName + str(expScheme), 'w')

    for query in queries:
        countExpansion = 0
        if expScheme in [1, 9, 10]:
            # print "expand Title contain query terms"
            for term in query["TitleContainTerm"]:
                expTerm = pattern.sub(' ', term).strip()
                if len(expTerm) > 0:
                    countExpansion += 1

        if expScheme in [1, 2, 5, 10]:
            # print "expand terms in Title"
            for term in query["termsInTitle"]:
                expTerm = pattern.sub(' ', term).strip()
                if len(expTerm) > 0:
                    countExpansion += 1

        if expScheme in [1, 2, 4]:
            # print "expand Alias from Matching Title"
            for term in query["aliasFromMatchingTitle"]:
                expTerm = pattern.sub(' ', term).strip()
                if len(expTerm) > 0:
                    countExpansion += 1

        if expScheme in [1, 2, 6]:
            # print "expand Categories From matching Title"
            for term in query["categoriesFromMatchingTitle"]:
                expTerm = pattern.sub(' ', term).strip()
                if len(expTerm) > 0:
                    countExpansion += 1

        if expScheme in [1, 7, 10]:
            # print "expand Title From Matching Alias"
            for term in query["titleFromMatchingAlias"]:
                expTerm = pattern.sub(' ', term).strip()
                if len(expTerm) > 0:
                    countExpansion += 1

        if expScheme in [1, 8]:
            # print "expand Title From Matching Link"
            for term in query["titleFromMatchingLink"]:
                expTerm = pattern.sub(' ', term).strip()
                if len(expTerm) > 0:
                    countExpansion += 1

        if expScheme in [1, 11, 10]:
            # print "expand Title From Matching Body"
            for term in query["titleFromMatchingBody"]:
                expTerm = pattern.sub(' ', term).strip()
                if len(expTerm) > 0:
                    countExpansion += 1

        if expScheme in [1, 12]:
            # print "expand Alias From Matching Body"
            for term in query["aliasFromMatchingBody"]:
                expTerm = pattern.sub(' ', term).strip()
                if len(expTerm) > 0:
                    countExpansion += 1
        fw.write(query["queryId"] + " " + str(countExpansion) + "\n")

    fw.close()
    print "finished scheme " + str(expScheme)

