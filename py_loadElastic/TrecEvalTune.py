import commands, json, os

dataSet = "WEB2013-2014"

if dataSet == "CLEF2015":
    qrelPath = "/volumes/ext/data/clef2015_eval/qrels.eng.clef2015.test.graded.txt"
    topPath = "/volumes/ext/jimmy/data/clef2015Tuned_topfiles/"
    resultFile = "trecDetailResultTuned_Clef2015.txt"
    prefixFile = "clef2015_"

elif dataSet == "CLEF2016":
    qrelPath = "/volumes/ext/data/clef2016_eval/task1.qrels.30Aug"
    topPath = '/volumes/ext/jimmy/data/clef2016Tuned_topFiles/'
    resultFile = "trecDetailResultTuned_Clef2016.txt"
    prefixFile = "clef2016_"
elif dataSet == "WEB2013-2014":
    qrelPath = "/volumes/ext/data/webTrack2013-2014_eval/qrels.adhoc2013-2014.txt"
    topPath = '/volumes/ext/jimmy/data/clueweb12/WebNav2013-2014Tuned_topFiles/'
    resultFile = "trecDetailResultTuned_webNav2013-2014.txt"
    prefixFile = "webNav2013-2014_"

resultPath = "/volumes/ext/jimmy/experiments/fielded_retrieval/treceval_results/"
trecPath = "/volumes/ext/tools/trec_eval.9.0/"
titleWeights = [0, 1, 3, 5]
metaWeights = [0]
headersWeights = [0]
bodyWeights = [0, 1, 3, 5]
tieBreakers = [0.25]

if not os.path.exists(resultPath):
    os.makedirs(resultPath)

fw = open(resultPath + resultFile, 'w')
fw.write("schema" + " " + " QueryNum"+ " " + "map" + " " + "p10" + " " + "ndcg10" + " " + "ndcg1000" + " " + "bpref" + " " + "unjudged" + " " + "relString"+ "\n")

for tw in titleWeights:
    for mw in metaWeights:
        for hw in headersWeights:
            for bw in bodyWeights:
                if tw + mw + hw + bw >= 1:
                    for tie in tieBreakers:
                        for b in xrange(0, 101, 5):
                            for k in xrange(2, 31, 2):

                                # Index Setting
                                weights = format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02')
                                fileName = prefixFile + weights + "_b" + format(float(b) / 100) + "_k" + format(float(k) / 10)

                                if not os.path.exists(topPath + fileName):
                                    raise NameError('Top File Not Found: ' + fileName)

                                trecResults = commands.getoutput(
                                    trecPath + 'trec_eval -q -m map -m P.10 -m ndcg_cut.10,1000 -m bpref -m relstring.10 ' + qrelPath + " " + topPath + fileName)

                                map = p10 = ndcg10 = ndcg1000 = bpref = unjudged = "*"
                                relString = qNum = "*"
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
                                        elif measure == "ndcg_cut_10":
                                            ndcg10 = score
                                        elif measure == "ndcg_cut_1000":
                                            ndcg1000 = score

                                            fw.write(
                                                fileName + " " + qNum + " " + map + " " + p10 + " " + ndcg10 + " " + ndcg1000 + " " + bpref + " " + str(
                                                    numUnjudged) + " " + relString + "\n")

                                print ("File name: {0} Completed".format(fileName))
fw.close()
