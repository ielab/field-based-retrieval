import urllib2
import json

url = 'http://localhost:9200/clueweb12b_all/_analyze'

def stemFile(inputFile, outputFile):
    fr = open(inputFile, 'r')
    terms = fr.readlines()
    terms = [line.strip() for line in terms]
    fr.close()

    termString = ""
    for term in terms:
        termString += term + " "

    termString = ' '.join(set(termString.split()))

    data = '{"text": "' + termString + '", "analyzer": "my_english"}'
    req = urllib2.Request(url, data)
    response = urllib2.urlopen(req)
    jsonres = json.loads(response.read())

    terms = []
    for token in jsonres["tokens"]:
        if len(token["token"]) > 2 and not token["token"].isdigit():
            terms.append(token["token"])

    terms = list(set(terms))

    fw = open(outputFile, 'w')
    for term in terms:
        fw.write(term + "\n")
    fw.close()

# Stem wiki titles
#wikiTitleFile = "/volumes/ext/knowledgeBase/wikipedia/wikiTitles.txt"
#wikiTitleFile_stem = "/volumes/ext/knowledgeBase/wikipedia/wikiTitles_stem.txt"


# setup for wiki infobox based on health types and health links
wikiTitleFile = "/volumes/ext/knowledgebase/wikipedia/wikiInfoBoxHealthLinksTitle.txt"
wikiTitleFile_stem = "/volumes/ext/knowledgeBase/wikipedia/wikiInfoBoxHealthLinksTitle_stem.txt"

stemFile(wikiTitleFile, wikiTitleFile_stem)

# Stem wiki aliases
#wikiAliasFile = "/volumes/ext/knowledgeBase/wikipedia/wikiAliases.txt"
#wikiAliasFile_stem = "/volumes/ext/knowledgeBase/wikipedia/wikiAliases_stem.txt"

# setup for wiki infobox based on health types and health links
wikiAliasFile = "/volumes/ext/knowledgebase/wikipedia/wikiInfoBoxHealthLinksRedirects_health.txt"
wikiAliasFile_stem = "/volumes/ext/knowledgeBase/wikipedia/wikiInfoBoxHealthLinksAlias_stem.txt"
stemFile(wikiAliasFile, wikiAliasFile_stem)

# Stem pubmed titles
#pubmedTitleFile = "/volumes/ext/knowledgebase/pubmed/pubmedTitles.txt"
#pubmedTitleFile_stem = "/volumes/ext/knowledgebase/pubmed/pubmedTitles_stem.txt"
#stemFile(pubmedTitleFile, pubmedTitleFile_stem)



