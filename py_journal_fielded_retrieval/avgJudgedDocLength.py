# This script is to calculate the average fields length of judged documents in qrel
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
    qrelPath = "/volumes/ext/data/clef2015_eval/qrels.eng.clef2015.test.graded.txt"
    indexName = "kresmoi_all"
    docType = "kresmoi"
    outputFile = "avgRelevantDocLength_clef2015.txt"
elif dataSet == "CLEF2016":
    qrelPath = "/volumes/ext/data/clef2016_eval/task1.qrels.30Aug"
    docType = "clueweb"
    indexName = "clueweb12b_all"
    outputFile = "avgRelevantDocLength_clef2016.txt"
elif dataSet == "HARD2005":
    qrelPath = "/volumes/ext/data/aquaint_eval/TREC2005.qrels.txt"
    docType = "aquaint"
    indexName = "aquaint_all"
    outputFile = "avgRelevantDocLength_hard2005.txt"
elif dataSet == "WEB2013-2014":
    qrelPath = "/volumes/ext/data/webTrack2013-2014_eval/qrels.adhoc2013-2014.txt"
    docType = "clueweb"
    indexName = "clueweb12b_all"
    outputFile = "avgRelevantDocLength_web2013-2014.txt"

dataPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/'

# Open Index
es = elasticsearch.Elasticsearch(urls='http://localhost', port=9200, timeout=1000)

fw = open(dataPath + outputFile, "w")
fw.write("QueryNum DocId Rel TitleLen BodyLen \n")
with open(qrelPath, "r") as qrels:
    for line in qrels:
        queryId, temp, docId, rel = line.strip().split()
        
        try:
            res = es.get(index=indexName, doc_type=docType, id=docId, stored_fields=["title.length", "body.length"])
            if res["found"]:
                fw.write(queryId + " " + docId + " " + rel + " " +
                         str(res["fields"]["title.length"][0]) + " " + str(res["fields"]["body.length"][0]) + "\n")
        except elasticsearch.ElasticsearchException:
            pass

fw.close()
