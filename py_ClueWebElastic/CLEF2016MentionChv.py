# this file is to extract list of entity mention for every CLEF2016 that matched terms in CHV
# We use terms in file Concept_Terms_Flatfile from column: Term, CHV, UMLS
# The Ngrams Flat File

import re
from lxml import etree
import collections

queryFile = "/volumes/Data/Phd/Data/clef2016_eval/queries2016.xml"
conceptTermsFile = "/volumes/Data/Phd/Data/CHV/CHV_flatfiles_all/CHV_concepts_terms_flatfile_20110204.tsv"
ngramFile = "/volumes/Data/Phd/Data/CHV/CHV_flatfiles_all/ngrams_flat_file_2005-Dec-19.tsv"
resultFile = "/volumes/Data/Phd/Data/Clef2016_eval/CLEF2016MentionChv.csv"

# load queries
tree = etree.parse(queryFile)
topics = tree.getroot()

pattern = re.compile('[\W_]+')

# load mentions from concept term file into dictionaries of list with key is first two characters
keylength = 2
mentions = collections.defaultdict(list)
with open(conceptTermsFile, "r") as f:
    for line in f:
        parts = line.split("\t")

        grams = pattern.sub(' ', parts[1]).strip()
        if grams not in mentions[grams[:keylength]]:
            mentions[grams[:keylength]].append(grams)
            #print grams[:keylength] + " : " + grams

        grams = pattern.sub(' ', parts[2]).strip()
        if grams not in mentions[grams[:keylength]]:
            mentions[grams[:keylength]].append(grams)
            #print grams[:keylength] + " : " + grams

        grams = pattern.sub(' ', parts[3]).strip()
        if grams not in mentions[grams[:keylength]]:
            mentions[grams[:keylength]].append(grams)
            #print grams[:keylength] + " : " + grams

print "Finished loading concept term file"

# append mentions from ngram file into dictionaries of list with key is first two characters
with open(ngramFile, "r") as f:
    next(f)
    for line in f:
        parts = line.split("\t")
        grams = pattern.sub(' ', parts[0]).strip()
        if grams not in mentions[grams[:keylength]]:
            mentions[grams[:keylength]].append(grams)
            #print grams[:keylength] + " : " + grams

print "Finished loading n-gram file"

# load queries
queries = []
for topic in topics.iter("query"):
    for detail in topic:
        if detail.tag == "id":
            queryNumber = detail.text
        elif detail.tag == "title":
            queryTerms = detail.text
            #print queryTerms
            queryTerms = re.sub(r'([^\s\w]|_)+', ' ', queryTerms)
            #print queryTerms

    if queryNumber == "129005":
        queryTerms = "craving salt     full body spasm    need 12 hrs sleep    can t maintain body temperature"
    queryData = {"queryNumber": queryNumber, "queryTerms": queryTerms}
    queries.append(queryData)
    #print queryNumber + " >> " + queryTerms


# Match unigram, bigram and trigram from queries to mentions
maxWindowSize = 3
fw = open(resultFile, 'w')
for query in queries:
    print "Processing : " + query["queryNumber"] + " : " + query["queryTerms"]
    qWords = query["queryTerms"].split()
    for windowSize in xrange(maxWindowSize, 0, -1):
        nGrams = []
        if len(qWords) >= windowSize:
            temp = zip(*[qWords[i:] for i in range(windowSize)])
            for t in temp:
                nGrams.append(" ".join(t))

            for grams in nGrams:
                if grams in mentions[grams[:keylength]]:
                    fw.write(query["queryNumber"] + "," + grams + "\n")
                    #print grams
fw.close()