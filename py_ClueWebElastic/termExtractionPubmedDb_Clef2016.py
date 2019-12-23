import os, re, json
from lxml import etree
import mysql.connector

maxWindowSize = 3
maxLength = 6

# local setting
queryFile = '/volumes/data/phd/data/clef2016_eval/queries2016.xml'
stopwordFile = "/Volumes/Data/Tools/elasticsearch-5.1.1/config/stopwords/terrier-stop.txt"
jsonQueryExpFile = '/volumes/data/phd/data/clef2016_eval/queriesExp_pubmed' + '_len' + str(maxLength) + '_Clef2016.json'
scoreFile = '/volumes/data/phd/data/expWiki_Clef2016/expScore_pubmed' + '_len' + str(maxLength) + '_Clef2016.txt'
portSetting=9200

dbName = "pubmed"
dbCon = mysql.connector.connect(user='root', password='root', host="localhost", port="3306", database= dbName)


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


# extract health query terms: generate nGrams and match grams to terms in pub med database
jsonList = []

for query in queries:
    qWords = query["queryTerms"].split()
    termsInTitle = []
    titleContainTerm = []
    matchingTitleAlias = []
    matchingTitleRelatedTerm = []
    titleFromMatchingAlias = []
    titleFromMatchingLink = []

    for windowSize in xrange(maxWindowSize, 0, -1):
        nGrams=[]
        if len(qWords) >= windowSize:
            temp = zip(*[qWords[i:] for i in range(windowSize)])
            for t in temp:
                nGrams.append(" ".join(t))


            for grams in nGrams:
                grams = grams.lower()

                # search query's n-grams in KB Title
                dbCursor = dbCon.cursor()
                dbCom = "SELECT topicId, title  FROM topic " \
                        "WHERE title LIKE '%" + grams + "%'"

                dbCursor.execute(dbCom)
                rows = dbCursor.fetchall()
                dbCursor.close()

                for (topicId, title) in rows:
                    if title == grams:
                        termsInTitle.append(title)

                        # search aliases from matching title
                        dbCursor = dbCon.cursor()
                        dbCom = "SELECT alias  FROM alias " \
                                "WHERE topicId = '" + topicId + "'"
                        dbCursor.execute(dbCom)
                        rowAliases = dbCursor.fetchall()
                        dbCursor.close()

                        for alias, in rowAliases:
                            matchingTitleAlias.append(alias)


                        # search related terms from matching title and append related terms to aliases list
                        dbCursor = dbCon.cursor()
                        dbCom = "SELECT term  FROM relatedTerm " \
                                "WHERE topicId = '" + topicId + "'"
                        dbCursor.execute(dbCom)
                        rowTerms = dbCursor.fetchall()
                        dbCursor.close()

                        for term, in rowTerms:
                            matchingTitleAlias.append(term)

                        # search related term (term to know) from matching title
                        dbCursor = dbCon.cursor()
                        dbCom = "SELECT o.title  " \
                                "FROM termToKnow e INNER JOIN topic o ON e.termToKnowId=o.topicId " \
                                "WHERE e.topicId = '" + topicId + "'"

                        dbCursor.execute(dbCom)
                        rowTerms = dbCursor.fetchall()
                        dbCursor.close()

                        for term, in rowTerms:
                            matchingTitleRelatedTerm.append(term)

                        # search related condition (term to know) from matching title
                        dbCursor = dbCon.cursor()
                        dbCom = "SELECT o.title  " \
                                "FROM relatedCondition e INNER JOIN topic o ON e.relatedConditionId=o.topicId " \
                                "WHERE e.topicId = '" + topicId + "'"

                        dbCursor.execute(dbCom)
                        rowTerms = dbCursor.fetchall()
                        dbCursor.close()

                        for term, in rowTerms:
                            matchingTitleRelatedTerm.append(term)

                    else:
                        titleContainTerm.append(title)

                # search title from matching alias
                dbCursor = dbCon.cursor()
                dbCom = "SELECT t.title  " \
                        "FROM  topic t NATURAL JOIN alias a " \
                        "WHERE a.alias = '" + grams + "'"

                dbCursor.execute(dbCom)
                rowTerms = dbCursor.fetchall()
                dbCursor.close()

                for term, in rowTerms:
                    titleFromMatchingAlias.append(term)

                # search title from matching summaryTag and append to titleFromMatchingLink
                dbCursor = dbCon.cursor()
                dbCom = "SELECT t.title  " \
                        "FROM  topic tag INNER JOIN summaryTag sum ON tag.topicId=sum.summaryTagId " \
                        "INNER JOIN topic t ON sum.TopicId = t.topicId " \
                        "WHERE tag.title = '" + grams + "'"

                dbCursor.execute(dbCom)
                rowTerms = dbCursor.fetchall()
                dbCursor.close()

                for term, in rowTerms:
                    titleFromMatchingLink.append(term)

                # search title from matching descriptionTag and append to titleFromMatchingLink
                dbCursor = dbCon.cursor()
                dbCom = "SELECT t.title  " \
                        "FROM  topic tag INNER JOIN descriptionTag des ON tag.topicId=des.descriptionTagId " \
                        "INNER JOIN topic t ON des.TopicId = t.topicId " \
                        "WHERE tag.title = '" + grams + "'"

                dbCursor.execute(dbCom)
                rowTerms = dbCursor.fetchall()
                dbCursor.close()

                for term, in rowTerms:
                    titleFromMatchingLink.append(term)

    jsonData = {'queryId': query["queryNumber"], 'query': query["queryTerms"],
                'termsInTitle': termsInTitle,
                'TitleContainTerm': titleContainTerm,
                'aliasFromMatchingTitle': matchingTitleAlias,
                'relatedTermFromMatchingTitle': matchingTitleRelatedTerm,
                'titleFromMatchingAlias': titleFromMatchingAlias,
                'titleFromMatchingLink': titleFromMatchingLink}
    jsonList.append(jsonData)


    print "Completed: " + query["queryNumber"] + " Query: " + query["queryTerms"]
    #print "Terms in title: " + ', '.join(termsInTitle)
    #print "Topic contain query terms: " + ', '.join(titleContainTerm)
    #print "Alias from matching title: " + ', '.join(matchingTitleAlias)
    #print "Term to know: " + ', '.join(matchingTitleRelatedTerm)
    #print "Title from matching alias: " + ', '.join(titleFromMatchingAlias)
    #print "Title from matching link: " + ', '.join(titleFromMatchingLink)
    #print "~~"

with open(jsonQueryExpFile, 'w') as fw:
    json.dump(jsonList, fw)
    fw.close()