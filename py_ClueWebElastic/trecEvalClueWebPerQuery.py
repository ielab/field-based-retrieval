import commands
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-d",
                    "--dataset",
                    help="web2013-2014, hard2005, clef2015 or clef2016",
                    choices=["web2013-2014", "hard2005", "clef2015", "clef2016"])
args = parser.parse_args()


dataSet = args.dataset

qrelPath = ""
resultFile = ""
prefixFile = ""
if dataSet == 'web2013-2014':
    # WEB TREC 2013-2014 AdHoc setting
    qrelPath = "/volumes/ext/jimmy/data/AdHoc2013-2014_eval/qrels.adhoc2013-2014.txt"
    topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/web2013-2014_termWeight_topFiles/'
    resultFile = "trecDetailResult_Web2013-2014.csv"
    prefixFile = "web2013-2014_top"
elif dataSet == 'hard2005':
    # HARD2005 setting
    qrelPath = "/volumes/ext/jimmy/data/hard2005_eval/TREC2005.qrels.txt"
    topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/hard2005_termWeight_topFiles/'
    resultFile = "trecDetailResult_hard2005.csv"
    prefixFile = "hard2005_top"
elif dataSet == 'clef2015':
    # CLEF2015 setting
    qrelPath = "/volumes/ext/jimmy/data/clef2015_eval/qrels.eng.clef2015.test.graded.txt"
    topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/clef2015_termWeight_topFiles/'
    resultFile = "trecDetailResult_Clef2015.csv"
    prefixFile = "clef2015_top"
elif dataSet == 'clef2016':
    # CLEF2016 setting
    qrelPath = "/volumes/ext/jimmy/data/clef2016_eval/task1.qrels.30Aug"
    topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/clef2016_termWeight_topFiles/'
    resultFile = "trecDetailResult_Clef2016.csv"
    prefixFile = "clef2016_top"

resultPath = "/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/treceval_results/"
trecPath = "/volumes/ext/tools/trec_eval.9.0/"


if not os.path.exists(qrelPath):
    raise NameError('Qrel Not Found')


titleWeights = [0, 1, 3, 5]
metaWeights = [0, 1, 3, 5]
headersWeights = [0, 1, 3, 5]
bodyWeights = [0, 1, 3, 5]
tieBreakers = ["25.0"]

if dataSet == 'hard2005':
    metaWeights = [0]
    headersWeights = [0]

if not os.path.exists(resultPath):
    os.makedirs(resultPath)

fw = open(resultPath + resultFile, 'w')
fw.write("title" + "," + "meta" + "," + "headers" + "," + "body" + "," + "QueryNumber" + "," +
         "map" + "," + "p10" + "," + "ndcg10" + "," + "ndcg1000" + "," + "bpref" + "," + "rr" + "\n")


for tw in titleWeights:
    for mw in metaWeights:
        for hw in headersWeights:
            for bw in bodyWeights:
                if tw + mw + hw + bw >= 1:
                    for tie in tieBreakers:
                        fileName = prefixFile + "_" + format(tw, '02') + format(mw, '02') + format(hw, '02') + \
                                   format(bw, '02') + "_" + tie
                        if not os.path.exists(topPath + fileName):
                            raise NameError('Top File Not Found')

                        trecResults = commands.getoutput(trecPath +
                                                         'trec_eval -q '
                                                         '-m map -m P.10 -m bpref -m ndcg_cut.10,1000 -m recip_rank ' +
                                                         qrelPath + " " + topPath + fileName)

                        # -J to evaluate only judged results
                        # trecResults = commands.getoutput(
                        #    trecPath + 'trec_eval -q -J -m map -m P.10 -m ndcg_cut.10,1000 ' +
                        #    qrelPath + " " + topPath + fileName)

                        MAP = p10 = ndcg10 = ndcg1000 = bpref = rr = 0
                        for res in trecResults.splitlines():
                            measure = res.split()[0]
                            qNum = res.split()[1]
                            score = res.split()[2]
                            if qNum != "all":
                                if measure == "map":
                                    MAP = score
                                elif measure == "bpref":
                                    bpref = score
                                elif measure == "P_10":
                                    p10 = score
                                elif measure == "recip_rank":
                                    rr = score
                                elif measure == "ndcg_cut_10":
                                    ndcg10 = score
                                elif measure == "ndcg_cut_1000":
                                    ndcg1000 = score

                                    fw.write(str(tw) + "," + str(mw) + "," + str(hw) + "," + str(bw) + "," +
                                             qNum + "," +
                                             MAP + "," + p10 + "," + ndcg10 + "," + ndcg1000 + "," + bpref + "," + rr +
                                             "\n")

                        print ("File name: {0} Completed".format(fileName))
fw.close()
