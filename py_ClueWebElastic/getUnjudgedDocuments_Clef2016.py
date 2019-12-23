import os
import argparse
import collections

parser = argparse.ArgumentParser()
parser.add_argument("-k",
                    "--knowledgebase",
                    help="wiki or pubmed",
                    choices=["wiki", "pubmed"])
args = parser.parse_args()

expansionSchemes = ["3", "4", "5", "6", "7", "8", "9", "11", "12"]

rankTreshold = 20
scoreTreshold = 8
maxLength = 3

if args.knowledgebase:
    kb = args.knowledgebase
else:
    raise NameError("missing argument -k ")


titleWeight = 1
metaWeight = 0
headersWeight = 0
bodyWeight = 3
bt = 75
bb = 75
k = 12

rank_cutoff = 10

# load qrels into a dictionary
qrels_path = "/volumes/ext/jimmy/data/clef2016_eval/task1.qrels.30Aug"

if not os.path.exists(qrels_path):
    raise NameError("Path to qrels file not found!")

judged_docs = collections.defaultdict(list)
with open(qrels_path, 'r') as infile:
    for line in infile:
        parts = line.split()
        judged_docs[parts[0]].append(parts[2])


weightScheme = str(titleWeight).zfill(2) + str(metaWeight).zfill(2) + str(headersWeight).zfill(2) + \
               str(bodyWeight).zfill(2)

topFileName = ""
topPath = ""
docs = dict()
docs_count = dict()
docs_score = dict()
docs_judged = dict()
for expScheme in expansionSchemes:
    if kb == "wiki":
        topPath = '/volumes/ext/jimmy/data/expWiki_Clef2016_topFiles/'
        topFileName = "topExpWikiHealth_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + \
                      str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + expScheme
    elif kb == "pubmed":
        topPath = '/volumes/ext/jimmy/data/expPubMed_Clef2016_topFiles/'
        topFileName = "topExpPubMed_" + weightScheme + '_len' + str(maxLength) + "_Clef2016_" + expScheme

    # load results with no PRF
    if not os.path.exists(topPath + topFileName):
        raise NameError("Path to run file not found: " + topPath + topFileName)

    with open(topPath + topFileName, 'r') as infile:
        for line in infile:
            parts = line.split()
            rank = int(parts[3])
            if rank <= rank_cutoff :
                key = parts[0] + "*" + parts[2]
                try:
                    docs[key] += rank
                    docs_count[key] += 1
                    docs_score[key] += (1.0/(len(expansionSchemes)*2)) * (1.0/rank)
                    if parts[2] in judged_docs[parts[0]]:
                        docs_judged[key] = 1
                    else:
                        docs_judged[key] = 0
                except KeyError:
                    docs[key] = rank
                    docs_count[key] = 1
                    docs_score[key] = (1.0 /(len(expansionSchemes)*2)) * (1.0 / rank)
                    if parts[2] in judged_docs[parts[0]]:
                        docs_judged[key] = 1
                    else:
                        docs_judged[key] = 0

    # load results with PRF
    if not os.path.exists(topPath + topFileName + "_prf"):
        raise NameError("Path to run file not found: " + topPath + topFileName + "_prf")

    with open(topPath + topFileName, 'r') as infile:
        for line in infile:
            parts = line.split()
            rank = int(parts[3])
            if rank <= rank_cutoff:
                key = parts[0] + "*" + parts[2]
                try:
                    docs[key] += rank
                    docs_count[key] += 1
                    docs_score[key] += (1.0 /(len(expansionSchemes)*2)) * (1.0 / rank)
                    if parts[2] in judged_docs[parts[0]]:
                        docs_judged[key] = 1
                    else:
                        docs_judged[key] = 0
                except KeyError:
                    docs[key] = rank
                    docs_count[key] = 1
                    docs_score[key] = (1.0 /(len(expansionSchemes)*2)) * (1.0 / rank)
                    if parts[2] in judged_docs[parts[0]]:
                        docs_judged[key] = 1
                    else:
                        docs_judged[key] = 0

    print "Finish processing expansion scheme: " + expScheme

resultFile = "unjudged_ExpWikiHealth_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + \
             str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016.csv"

fw = open(topPath + resultFile, 'w')
fw.write("query_id" + ","  "doc_id" + "," + "sum_rank" + "," + "count" + "," + "score" + "," + "is_judged" + "\n")
for key, value in docs.iteritems():
    parts = key.split("*")

    fw.write(parts[0] + "," + parts[1] + "," + str(value) + "," + str(docs_count[key]) + "," + str(docs_score[key]) +
             "," + str(docs_judged[key]) +"\n")

fw.close()
print "completed, output to: " + topPath + resultFile
