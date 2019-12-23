import commands, os
import glob
import multiprocessing


import argparse

parser = argparse.ArgumentParser()


parser.add_argument("-d",
                    "--dataset",
                    help="CLEF2015,  CLEF2016, HARD2005, or WEB2013-2014",
                    choices=["CLEF2015", "CLEF2016", "HARD2003", "HARD2005", "WEB2013-2014",
                             "BM25F_CLEF2015", "BM25F_CLEF2016", "BM25F_HARD2003",
                             "BM25F_HARD2005", "BM25F_WEB2013-2014"])
args = parser.parse_args()

dataSet = args.dataset


if dataSet == "CLEF2015":
    qrelPath = "/volumes/ext/data/clef2015_eval/qrels.eng.clef2015.test.graded.txt"
    topPrefix = "topTuneBoost_Clef2015_alpha"
    resultFile = "evalTuneBoost_Clef2015.txt"
elif dataSet == "CLEF2016":
    qrelPath = "/volumes/ext/data/clef2016_eval/task1.qrels.30Aug"
    topPrefix = "topTuneBoost_Clef2016_alpha"
    resultFile = "evalTuneBoost_Clef2016.txt"
elif dataSet == "HARD2003":
    qrelPath = "/volumes/ext/data/Hard2003_eval/qrels.actual.03.txt"
    topPrefix = "topTuneBoost_Hard2003_alpha"
    resultFile = "evalTuneBoost_Hard2003.txt"
elif dataSet == "HARD2005":
    qrelPath = "/volumes/ext/data/aquaint_eval/TREC2005.qrels.txt"
    topPrefix = "topTuneBoost_Hard2005_alpha"
    resultFile = "evalTuneBoost_Hard2005.txt"
elif dataSet == "WEB2013-2014":
    qrelPath = "/volumes/ext/data/webTrack2013-2014_eval/qrels.adhoc2013-2014.txt"
    topPrefix = "topTuneBoost_Web_alpha"
    resultFile = "evalTuneBoost_Web.txt"

elif dataSet == "BM25F_CLEF2015":
    qrelPath = "/volumes/ext/data/clef2015_eval/qrels.eng.clef2015.test.graded.txt"
    topPrefix = "topBm25fTuneBoost_Clef2015_alpha"
    resultFile = "evalBm25fTuneBoost_Clef2015.txt"
elif dataSet == "BM25F_CLEF2016":
    qrelPath = "/volumes/ext/data/clef2016_eval/task1.qrels.30Aug"
    topPrefix = "topBm25fTuneBoost_Clef2016_alpha"
    resultFile = "evalBm25fTuneBoost_Clef2016.txt"
elif dataSet == "BM25F_HARD2003":
    qrelPath = "/volumes/ext/data/Hard2003_eval/qrels.actual.03.txt"
    topPrefix = "topBm25fTuneBoost_Hard2003_alpha"
    resultFile = "evalBm25fTuneBoost_Hard2003.txt"
elif dataSet == "BM25F_HARD2005":
    qrelPath = "/volumes/ext/data/aquaint_eval/TREC2005.qrels.txt"
    topPrefix = "topBm25fTuneBoost_Hard2005_alpha"
    resultFile = "evalBm25fTuneBoost_Hard2005.txt"
elif dataSet == "BM25F_WEB2013-2014":
    qrelPath = "/volumes/ext/data/webTrack2013-2014_eval/qrels.adhoc2013-2014.txt"
    topPrefix = "topBm25fTuneBoost_Web_alpha"
    resultFile = "evalBm25fTuneBoost_Web.txt"

trecPath = "/volumes/ext/tools/trec_eval.9.0/"
dataPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/'


def eval(fname):
    trecResults = commands.getoutput(trecPath +
                                     'trec_eval -q -m map -m P.10 -m ndcg_cut.10,1000 -m bpref -m relstring.10 '
                                     '-m recip_rank ' + qrelPath + " " + fname)
    #print trecResults
    filename = os.path.basename(fname)
    alpha = filename.replace(topPrefix,"")

    map = p10 = ndcg10 = ndcg1000 = bpref = numUnjudged = rr = "*"
    relString = qNum = "*"
    resultString = ""
    for res in trecResults.splitlines():
        measure = res.split()[0]
        qNum = res.split()[1]
        score = res.split()[2]
        if qNum != "all":
            if measure == "map":
                map = score
            elif measure == "bpref":
                bpref = score
            elif measure == "P_10":
                p10 = score
            elif measure == "relstring_10":
                relString = score
                numUnjudged = relString.count('-')
            elif measure == "recip_rank":
                rr = score
            elif measure == "ndcg_cut_10":
                ndcg10 = score
            elif measure == "ndcg_cut_1000":
                ndcg1000 = score

                resultString += filename + " " + alpha + " " + qNum + " " + map + " " + p10 + " " + ndcg10 + " " + \
                                ndcg1000 + " " + bpref + " " + str(numUnjudged) + " " + relString + " " + rr + "\n"

    print ("File name: {0} Completed".format(filename))

    return resultString

fileNames = glob.glob(dataPath + topPrefix + "*")

fw = open(dataPath + resultFile, 'w')
fw.write("schema" + " " + "alpha" + " " + " QueryNum"+ " " + "map" + " " + "p10" + " " + "ndcg10" + " " + "ndcg1000" +
         " " + "bpref" + " " + "unjudged" + " " + "relString" + " " + "rr""\n")

p = multiprocessing.Pool()
resultString = p.map(eval, fileNames)
for res in resultString:
    fw.write(res)
p.close()
p.join()

fw.close()
