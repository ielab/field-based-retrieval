import commands
import os

dataSet = 'hard2005'

if dataSet == 'web2013-2014':
    # WEB TREC 2013-2014 AdHoc setting
    qrelPath = "/volumes/ext/jimmy/data/AdHoc2013-2014_eval/qrels.adhoc2013-2014.txt"
    topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/web2013-2014_termWeight_topFiles/'
    resultFile = "trecSummaryResult_Web2013-2014.csv"
    prefixFile = "web2013-2014_top"
elif dataSet == 'hard2005':
    # HARD2005 setting
    qrelPath = "/volumes/ext/jimmy/data/hard2005_eval/TREC2005.qrels.txt"
    topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/hard2005_termWeight_topFiles/'
    resultFile = "trecSummaryResult_hard2005.csv"
    prefixFile = "hard2005_top"
elif dataSet == 'clef2015':
    # CLEF2015 setting
    qrelPath = "/volumes/ext/jimmy/data/clef2015_eval/qrels.eng.clef2015.test.graded.txt"
    topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/clef2015_termWeight_topFiles/'
    resultFile = "trecSummaryResult_Clef2015.csv"
    prefixFile = "clef2015_top"
elif dataSet == 'clef2016':
    # CLEF2016 setting
    qrelPath = "/volumes/ext/jimmy/data/clef2016_eval/task1.qrels.30Aug"
    topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/clef2016_termWeight_topFiles/'
    resultFile = "trecSummaryResult_Clef2016.csv"
    prefixFile = "clef2016_top"

trecPath = "/volumes/ext/tools/trec_eval.9.0/"


titleWeights = [0, 1, 3, 5]
metaWeights = [0, 1, 3, 5]
headersWeights = [0, 1, 3, 5]
bodyWeights = [0, 1, 3, 5]
tieBreakers = ["25.0"]

if dataSet == 'hard2005':
    metaWeights = [0]
    headersWeights = [0]

jsonData = []
fw = open(topPath + resultFile, 'w')
fw.write("title" + "," + "meta" + "," + "headers" + "," + "body" + "," +
         "map" + "," + "p10" + "," + "ndcg10" + "," + "ndcg1000" + "\n")

for tw in titleWeights:
    for mw in metaWeights:
        for hw in headersWeights:
            for bw in bodyWeights:
                if tw + mw + hw + bw >= 1:
                    for tie in tieBreakers:
                        # Index Setting
                        fileName = prefixFile + "_" + format(tw, '02') + format(mw, '02') + format(hw, '02') + \
                                   format(bw, '02')

                        if not os.path.exists(topPath + fileName):
                            raise NameError('Top File Not Found' + topPath + fileName)

                        map = p10 = ndcg10 = ndcg1000 = 0

                        map = commands.getoutput(trecPath + 'trec_eval -m map ' + qrelPath + " " + topPath + fileName).split()[2]
                        p10 = commands.getoutput(trecPath + 'trec_eval -m P.10 ' + qrelPath + " " + topPath + fileName).split()[2]
                        ndcg10 = commands.getoutput(trecPath + 'trec_eval -m ndcg_cut.10 ' + qrelPath + " " + topPath + fileName).split()[2]
                        ndcg1000 = commands.getoutput(trecPath + 'trec_eval -m ndcg_cut.1000 ' + qrelPath + " " + topPath + fileName).split()[2]

                        fw.write(str(tw) + "," + str(mw) + "," + str(hw) + "," + str(bw) + "," +
                                 map + "," + p10 + "," + ndcg10 + "," + ndcg1000 + "\n")
                        print ("File name: {0} Completed".format(fileName))
fw.close()
