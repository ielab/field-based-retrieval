import os, re, json
from lxml import etree
from elasticsearch import Elasticsearch


def combineTermsScore(termsInTitle, wTermsInTitle, titleContainTerm, wTitleContainTerm, matchingTitleAlias, wMatchingTitleAlias, matchingTitleCategories,
                  wMatchingTitleCategories, titleFromMatchingAlias, wTitleFromMatchingAlias, titleFromMatchingLink,
                  wTitleFromMatchingLink, titleFromMatchingBody, wTitleFromMatchingBody, aliasFromMatchingBody, wAliasFromMatchingBody):
    terms = []

    #Get list of all terms from all sources
    for term in termsInTitle:
        terms.append(term["term"])

    for term in titleContainTerm:
        terms.append(term["term"])

    for term in matchingTitleAlias:
        terms.append(term["term"])

    for term in matchingTitleCategories:
        terms.append(term["term"])

    for term in titleFromMatchingAlias:
        terms.append(term["term"])

    for term in titleFromMatchingLink:
        terms.append(term["term"])

    for term in titleFromMatchingBody:
        terms.append(term["term"])

    for term in aliasFromMatchingBody:
        terms.append(term["term"])

    # remove duplicate terms
    terms = list(set(terms))

    # Calculate score for each term
    termsScore = []
    for term in terms:
        termScore = 0
        for t in termsInTitle:
            if term == t["term"]:
                termScore += t["score"] * wTermsInTitle
                #print "terms in Title: " + term + " = " + str(t["score"] * wTermsInTitle)

        for t in titleContainTerm:
            if term == t["term"]:
                termScore += t["score"] * wTitleContainTerm

        for t in matchingTitleAlias:
            if term == t["term"]:
                termScore += t["score"] * wMatchingTitleAlias
                #print "matching title alias: " + term + " = " + str(t["score"] * wMatchingTitleAlias)


        for t in matchingTitleCategories:
            if term == t["term"]:
                termScore += t["score"] * wMatchingTitleCategories
                #print "matching title Categories: " + term + " = " + str(t["score"] * wMatchingTitleCategories)

        for t in titleFromMatchingAlias:
            if term == t["term"]:
                termScore += t["score"] * wTitleFromMatchingAlias
                #print "title from matching alias: " + term + " = " + str(t["score"] * wTitleFromMatchingAlias)

        for t in titleFromMatchingLink:
            if term == t["term"]:
                termScore += t["score"] * wTitleFromMatchingLink
                #print "title from matching links: " + term + " = " + str(t["score"] * wTitleFromMatchingAlias)

        for t in titleFromMatchingBody:
            if term == t["term"]:
                termScore += t["score"] * wTitleFromMatchingBody
                # print "title from matching links: " + term + " = " + str(t["score"] * wTitleFromMatchingAlias)

        for t in aliasFromMatchingBody:
            if term == t["term"]:
                termScore += t["score"] * wAliasFromMatchingBody
                # print "title from matching links: " + term + " = " + str(t["score"] * wAliasFromMatchingAlias)

        temp = {"term": term, "fScore": termScore}
        termsScore.append(temp)

    return termsScore

maxWindowSize = 3
rankTreshold = 20
scoreTreshold = 8
maxLength = 3

# local setting
queryFile = '/volumes/data/phd/data/clef2016_eval/queries2016.xml'
stopwordFile = "/Volumes/Data/Tools/elasticsearch-5.1.1/config/stopwords/terrier-stop.txt"
jsonQueryExpFile = '/volumes/data/phd/data/clef2016_eval/queriesExpansion_rank' + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + '_Stem_Clef2016.json'
scoreFile = '/volumes/data/phd/data/expWiki_Clef2016/expansionScore_rank' + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + '_Stem_Clef2016.txt'


# server setting
queryFile = '/volumes/ext/data/clef2016/queries2016.xml'
stopwordFile = "/Volumes/ext/indeces/elasticsearch-5.1.1/config/stopwords/terrier-stop.txt"
jsonQueryExpFile = '/volumes/ext/jimmy/data/clef2016_eval/queriesExpansion_rank' + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + '_Stem_WikiInfoboxHealthLinks_Clef2016.json'

scoreFile = '/volumes/ext/jimmy/data/clef2016_eval/expansionScore_rank' + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + '_Stem_WikiInfoboxHealthLinks_Clef2016.txt'


#indexName = "wikipedia"
indexName = "wikiinfoboxlinks"
docType = "wiki"

es = Elasticsearch(urls='http://localhost', port=9200, timeout=600)

#pattern = re.compile('[\W_]+', re.UNICODE)
pattern = re.compile('[\W_]+')


# load queries
tree = etree.parse(queryFile)
topics = tree.getroot()

# load stopwords and remove stopwords from queries
stopwords=[]
if not os.path.exists(stopwordFile):
    raise NameError('Stopword File Not Found')
else:
    with open(stopwordFile, 'r') as f:
        for line in f:
            stopwords.append(line.strip())

# load queries
queries = []
if not os.path.exists(queryFile):
    raise NameError('Query File Not Found')
else:
    for topic in topics.iter("query"):
        for detail in topic:
            if detail.tag == "id":
                queryNumber = detail.text
            elif detail.tag == "title":
                queryTerms = detail.text
                queryTerms = re.sub(r'([^\s\w]|_)+', ' ', queryTerms)

        if queryNumber == "129005":
            queryTerms = "craving salt     full body spasm    need 12 hrs sleep    can t maintain body temperature"

        queryTerms = " ".join([word for word in queryTerms.split() if word not in stopwords])

        queryData = {"queryNumber": queryNumber, "queryTerms": queryTerms}
        queries.append(queryData)
        #print queryNumber + " >> " + queryTerms


# extract health query terms: generate nGrams and match grams to wikipedia aliases
jsonList = []
termsScore = []
for query in queries:
    qWords = query["queryTerms"].split()
    termsInTitle = []
    titleContainTerm = []
    matchingTitleAlias = []
    matchingTitleCategories = []
    titleFromMatchingAlias = []
    titleFromMatchingLink = []
    titleFromMatchingBody = []
    aliasFromMatchingBody = []

    t_termsInTitle = []
    t_titleContainTerm = []
    t_matchingTitleAlias = []
    t_matchingTitleCategories = []
    t_titleFromMatchingAlias = []
    t_titleFromMatchingLink = []
    t_titleFromMatchingBody = []
    t_aliasFromMatchingBody = []

    #print query["queryNumber"]
    for windowSize in xrange(maxWindowSize, 0, -1):
        nGrams=[]
        if len(qWords) >= windowSize:
            temp = zip(*[qWords[i:] for i in range(windowSize)])
            for t in temp:
                nGrams.append(" ".join(t))


            for grams in nGrams:
                # search terms in Title

                res = es.search(index=indexName, doc_type=docType,
                                body={
                                    "from": 0,
                                    "size": rankTreshold,
                                    "_source": ["title", "aliases","categories"],
                                    "min_score": scoreTreshold,
                                    "query": {
                                        "bool": {
                                            "must": {
                                                "match": {
                                                    "title": {
                                                        "query": grams.lower(),
                                                        "operator": "and"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                })

                for hit in res['hits']['hits']:
                    if len(hit["_source"]["title"].split()) <= maxLength:
                        t_term = pattern.sub(' ', hit["_source"]["title"]).lower().strip()
                        expData = {"queryNumber": query["queryNumber"], "queryTerms": query["queryTerms"], "term": t_term,
                                   "score": hit["_score"], "source": "titleContainTerm"}
                        termsScore.append(expData)
                        t_titleContainTerm.append(expData)
                        titleContainTerm.append(t_term)

                    if hit["_source"]["title"].lower() == grams.lower():
                        # Terms in title = entity mentions (unigram, bigram, trigram) mactch an entity
                        if len(hit["_source"]["title"].split()) <= maxLength:
                            t_term = pattern.sub(' ', hit["_source"]["title"]).lower().strip()
                            expData = {"queryNumber": query["queryNumber"], "queryTerms": query["queryTerms"], "term": t_term, "score": hit["_score"], "source": "termsInTitle"}
                            termsScore.append(expData)
                            t_termsInTitle.append(expData)
                            termsInTitle.append(t_term)
                            #termsInTitle.append(hit["_source"]["title"])

                        for alias in hit["_source"]["aliases"]:
                            if len(alias.split()) <= maxLength:
                                t_term = pattern.sub(' ', alias).lower().strip()
                                expData = {"queryNumber": query["queryNumber"], "queryTerms": query["queryTerms"], "term": t_term, "score": hit["_score"], "source": "matchingTitleAlias"}
                                termsScore.append(expData)
                                t_matchingTitleAlias.append(expData)
                                matchingTitleAlias.append(t_term)
                                #matchingTitleAlias.append(alias)

                        for category in hit["_source"]["categories"]:
                            if len(category.split()) <= maxLength:
                                t_term = pattern.sub(' ', category).lower().strip()
                                expData = {"queryNumber": query["queryNumber"], "queryTerms": query["queryTerms"], "term": t_term, "score": hit["_score"], "source": "matchingTitleCategories"}
                                termsScore.append(expData)
                                t_matchingTitleCategories.append(expData)
                                matchingTitleCategories.append(t_term)
                                #matchingTitleCategories.append(category)

                # search terms in Aliases
                res = es.search(index=indexName, doc_type=docType,
                                body={
                                    "from": 0,
                                    "size": rankTreshold,
                                    "_source": ["title", "aliases"],
                                    "min_score": scoreTreshold,
                                    "query": {
                                        "bool": {
                                            "must": {
                                                "match": {
                                                    "aliases": {
                                                        "query": grams.lower(),
                                                        "operator": "and"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                })
                resultString = ""

                for hit in res['hits']['hits']:
                    #if grams.lower() in hit["_source"]["title"].lower():
                        # print  hit["_source"]["title"]

                    for alias in hit["_source"]["aliases"]:
                        if grams.lower() == alias.lower():
                            if len(hit["_source"]["title"].split()) <= maxLength:
                                t_term = pattern.sub(' ', hit["_source"]["title"]).lower().strip()
                                expData = {"queryNumber": query["queryNumber"], "queryTerms": query["queryTerms"], "term": t_term, "score": hit["_score"], "source": "titleFromMatchingAlias"}
                                termsScore.append(expData)
                                t_titleFromMatchingAlias.append(expData)
                                titleFromMatchingAlias.append(t_term)
                                #titleFromMatchingAlias.append(hit["_source"]["title"])
                                break;

                # search terms in links
                res = es.search(index=indexName, doc_type=docType,
                                body={
                                    "from": 0,
                                    "size": rankTreshold,
                                    "_source": ["title", "links"],
                                    "min_score": scoreTreshold,
                                    "query": {
                                        "bool": {
                                            "must": {
                                                "match": {
                                                    "links": {
                                                        "query": grams.lower(),
                                                        "operator": "and"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                })
                resultString = ""

                for hit in res['hits']['hits']:
                    # if grams.lower() in hit["_source"]["title"].lower():
                    # print  hit["_source"]["title"]
                    for link in hit["_source"]["links"]:
                        if grams.lower() == link.lower():
                            if len(hit["_source"]["title"].split()) <= maxLength:
                                t_term = pattern.sub(' ',hit["_source"]["title"]).lower().strip()
                                expData = {"queryNumber": query["queryNumber"], "queryTerms": query["queryTerms"], "term": t_term, "score": hit["_score"], "source": "titleFromMatchingLink"}
                                termsScore.append(expData)
                                t_titleFromMatchingLink.append(expData)
                                titleFromMatchingLink.append(t_term)
                                #titleFromMatchingLink.append(hit["_source"]["title"])
                                break;


    # search whole query terms in wikipedia body
    res = es.search(index=indexName, doc_type=docType,
                    body={
                        "from": 0,
                        "size": 1,
                        "_source": ["title", "aliases"],
                        "query": {
                            "query_string": {
                                "query": query["queryTerms"],
                                "fields": ["body", "title"],
                                "use_dis_max": True,
                                "tie_breaker": 0.25
                            }
                        }
                    })

    #print "  ***  " + query["queryTerms"]
    for hit in res['hits']['hits']:
        #print hit["_source"]["title"]

        #Extract title from matching body
        if len(hit["_source"]["title"].split()) <= maxLength:
            t_term = pattern.sub(' ', hit["_source"]["title"]).lower().strip()
            expData = {"queryNumber": query["queryNumber"], "queryTerms": query["queryTerms"], "term": t_term,
                       "score": hit["_score"], "source": "titleFromMatchingBody"}
            termsScore.append(expData)
            t_titleFromMatchingBody.append(expData)
            titleFromMatchingBody.append(t_term)

        #Extract alias from matching Body
        for alias in hit["_source"]["aliases"]:
            if len(alias.split()) <= maxLength:
                t_term = pattern.sub(' ', alias).lower().strip()
                expData = {"queryNumber": query["queryNumber"], "queryTerms": query["queryTerms"], "term": t_term,
                           "score": hit["_score"], "source": "aliasFromMatchingBody"}
                termsScore.append(expData)
                t_aliasFromMatchingBody.append(expData)
                aliasFromMatchingBody.append(t_term)

    jsonData = {'queryId': query["queryNumber"], 'query': query["queryTerms"], 'termsInTitle': termsInTitle,
                'TitleContainTerm': titleContainTerm,
                'aliasFromMatchingTitle': matchingTitleAlias, 'categoriesFromMatchingTitle': matchingTitleCategories,
                'titleFromMatchingAlias': titleFromMatchingAlias, 'titleFromMatchingLink': titleFromMatchingLink,
                'titleFromMatchingBody': titleFromMatchingBody, 'aliasFromMatchingBody': aliasFromMatchingBody}
    jsonList.append(jsonData)

    # calculate the combined score
    combineScores = combineTermsScore(t_termsInTitle, 1, t_titleContainTerm, 1, t_matchingTitleAlias, 1,
                                      t_matchingTitleCategories, 1, t_titleFromMatchingAlias, 1,
                                      t_titleFromMatchingLink, 1, t_titleFromMatchingBody, 1,
                                      t_aliasFromMatchingBody, 1)
    for score in combineScores:
        expData = {"queryNumber": query["queryNumber"], "queryTerms": query["queryTerms"], "term": score["term"], "score": score["fScore"],
                   "source": "combined"}
        termsScore.append(expData)


    print "Completed: " + query["queryNumber"] + " Query: " + query["queryTerms"]
    #print "Terms in title: " + ', '.join(termsInTitle)
    #print "Alias from matching title:" + ', '.join(matchingTitleAlias)
    #print "Categories from matching title:" + ', '.join(matchingTitleCategories)
    #print "Titles of matching alias: " + ', '.join(titleFromMatchingAlias)
    #print "Titles of matching links: " + ', '.join(titleFromMatchingLink) + "\n"


with open(jsonQueryExpFile, 'w') as fw:
    json.dump(jsonList, fw)
    fw.close()

with open(scoreFile, 'w') as fw:
    fw.write("queryNumber,queryTerms,source,term,score\n")
    for score in termsScore:
        fw.write(score["queryNumber"] + "," + score["queryTerms"] + "," + score["source"] + "," + score["term"].encode('utf-8') + "," + str(score["score"]) + "\n")

fw.close()
