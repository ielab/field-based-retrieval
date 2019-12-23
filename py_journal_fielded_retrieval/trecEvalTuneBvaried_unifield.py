import commands, os
import glob
import multiprocessing

import argparse

parser = argparse.ArgumentParser()

parser.add_argument("-d",
                    "--dataset",
                    help="CLEF2015,  CLEF2016, HARD2003, HARD2005, or WEB2013-2014",
                    choices=["CLEF2015", "CLEF2016", "HARD2003" , "HARD2005", "WEB2013-2014"])
args = parser.parse_args()

dataSet = args.dataset


if dataSet == "CLEF2015":
    qrelPath = "/volumes/ext/data/clef2015_eval/qrels.eng.clef2015.test.graded.txt"
    topPrefix = "topTuneBvaried_Clef2015_unifield_b"
    resultFile = "evalTuneBvaried_Clef2015_unifield.txt"
elif dataSet == "CLEF2016":
    qrelPath = "/volumes/ext/data/clef2016_eval/task1.qrels.30Aug"
    topPrefix = "topTuneBvaried_Clef2016_unifield_b"
    resultFile = "evalTuneBvaried_Clef2016_unifield.txt"
elif dataSet == "HARD2003":
    qrelPath = "/volumes/ext/data/Hard2003_eval/qrels.actual.03.txt"
    topPrefix = "topTuneBvaried_Hard2003_unifield_b"
    resultFile = "evalTuneBvaried_Hard2003_unifield.txt"
elif dataSet == "HARD2005":
    qrelPath = "/volumes/ext/data/aquaint_eval/TREC2005.qrels.txt"
    topPrefix = "topTuneBvaried_Hard2005_unifield_b"
    resultFile = "evalTuneBvaried_Hard2005_unifield.txt"
elif dataSet == "WEB2013-2014":
    qrelPath = "/volumes/ext/data/webTrack2013-2014_eval/qrels.adhoc2013-2014.txt"
    topPrefix = "topTuneBvaried_Web_unifield_b"
    resultFile = "evalTuneBvaried_Web_unifield.txt"

trecPath = "/volumes/ext/tools/trec_eval.9.0/"
dataPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/'


def eval(fname):
    trecResults = commands.getoutput(trecPath +
                                     'trec_eval -q -m map -m P.10 -m ndcg_cut.10,1000 -m bpref -m relstring.10 '
                                     '-m recip_rank ' + qrelPath + " " + fname)
    #print trecResults
    filename = os.path.basename(fname)

    # get variables from the file name
    prefix, collection, coltype, b, k1 = filename.split('_')
    b = b.replace('b', '')

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

                resultString += filename + " " + b + " " + qNum + " " + map + " " + p10 + " " + ndcg10 + " " + \
                                ndcg1000 + " " + bpref + " " + str(numUnjudged) + " " + relString + " " + rr + "\n"

    print ("File name: {} Completed, b: {}".format(filename, b))

    return resultString

fileNames = glob.glob(dataPath + topPrefix + "*")

fw = open(dataPath + resultFile, 'w')
fw.write("schema b QueryNum map p10 ndcg10 ndcg1000 bpref unjudged relString rr \n")

p = multiprocessing.Pool()
resultString = p.map(eval, fileNames)
for res in resultString:
    fw.write(res)
p.close()
p.join()

fw.close()
