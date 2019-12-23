# This script is to calculate the average fields length of top documents in the best run of each collection
import elasticsearch
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-d",
                    "--dataset",
                    help="CLEF2015,  CLEF2016, HARD2005, or WEB2013-2014",
                    choices=["CLEF2015", "CLEF2016", "HARD2005", "WEB2013-2014"])
args = parser.parse_args()

qrelPath = indexName = docType = outputFile = ""

dataSet = args.dataset
if dataSet == "CLEF2015":
    runFile = "topTuneBvaried_Clef2015_alpha1_bt0.7_bb0.2_k1.2"
    indexName = "kresmoi_all"
    docType = "kresmoi"
    outputFile = "avgTopDocLength_clef2015.txt"
elif dataSet == "CLEF2016":
    runFile = "topTuneBvaried_Clef2016_alpha4_bt0.9_bb0.45_k1.2"
    docType = "clueweb"
    indexName = "clueweb12b_all"
    outputFile = "avgTopDocLength_clef2016.txt"
elif dataSet == "HARD2005":
    runFile = "topTuneBvaried_Hard2005_alpha4_bt1.0_bb0.1_k1.2"
    docType = "aquaint"
    indexName = "aquaint_all"
    outputFile = "avgTopDocLength_hard2005.txt"
elif dataSet == "WEB2013-2014":
    runFile = "topTuneBvaried_Web_alpha4_bt0.5_bb0.2_k1.2"
    docType = "clueweb"
    indexName = "clueweb12b_all"
    outputFile = "avgTopDocLength_web2013-2014.txt"

dataPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/'

# Open Index
es = elasticsearch.Elasticsearch(urls='http://localhost', port=9200, timeout=1000)

fw = open(dataPath + outputFile, "w")
fw.write("QueryNum DocId Rank TitleLen BodyLen \n")
with open(dataPath + runFile, "r") as runs:
    for line in runs:
        queryId, temp, docId, rank, score, temp2 = line.strip().split()
        print("Query: {}, rank: {}".format(queryId, rank))
        try:
            res = es.get(index=indexName, doc_type=docType, id=docId, stored_fields=["title.length", "body.length"])
            if res["found"]:
                fw.write(queryId + " " + docId + " " + rank + " " +
                         str(res["fields"]["title.length"][0]) + " " + str(res["fields"]["body.length"][0]) + "\n")
        except elasticsearch.ElasticsearchException:
            print("Query: {} DocId: {} Not Found".format(queryId, docId))

fw.close()
