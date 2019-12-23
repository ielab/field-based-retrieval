import commands
import os

qrelPath = "/volumes/ext/jimmy/data/clef2016_eval/task1.qrels.30Aug"
topPath = '/volumes/ext/jimmy/data/clef2016_tuneKL_topFiles/'
topPrefix = "clef2016"
resultFile = "DetailResult_CLEF2016_tuneKL.csv"
trecPath = "/volumes/ext/tools/trec_eval.9.0/"

if not os.path.exists(qrelPath):
    raise NameError('Qrel Not Found')


titleWeights = [0, 1]
metaWeights = [0]
headersWeights = [0]
bodyWeights = [0, 1]
tieBreakers = ["25.0"]

mus = []
mus.append(100)
mus.append(500)
for i in range(1000, 3001, 100):
    mus.append(i)

adt = 8 # based on average length of the CLUEWEB 12B title field
adb = 700 # based on average length of the CLUEWEB 12B body field

fw = open(topPath + resultFile, 'w')
fw.write("mu" + "," + "tw" + "," + "bw" + "," + "QueryNum" + "," + "map" + "," + "p10" + "," +
         "ndcg10" + "," + "ndcg1000" + "," + "bpref" + "," + "unjudged" + "," + "relString" + "\n")

counter = 0
for mu in mus:
    for tw in titleWeights:
        for bw in bodyWeights:
            if tw + bw == 1:
                for tie in tieBreakers:
                    weights = format(tw, '02') + "00" + "00" + format(bw, '02') + \
                              "_mu" + format(mu, '04') + "_adt" + format(adt, '04') + "_adb" + format(adb, '04')

                    topFileName = topPath + topPrefix + "_" + weights

                    if not os.path.exists(topFileName):
                        raise NameError('Top File Not Found : ' + topFileName)

                    trecResults = commands.getoutput(
                        trecPath + 'trec_eval -q -m map -m P.10 -m ndcg_cut.10,1000 -m bpref -m relstring.10 ' +
                        qrelPath + " " + topFileName)

                    # print trecResults
                    map = p10 = ndcg10 = ndcg1000 = bpref = unjudged = "*"
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
                            elif measure == "ndcg_cut_10":
                                ndcg10 = score
                            elif measure == "ndcg_cut_1000":
                                ndcg1000 = score

                                resultString = str(mu) + "," + str(tw) + "," + str(bw) + "," + qNum + "," + \
                                               map + "," + p10 + "," + ndcg10 + "," + \
                                               ndcg1000 + "," + bpref + "," + str(numUnjudged) + "," + relString + "\n"
                                fw.write(resultString)
                                counter += 1
                                if counter % 100 == 0:
                                    print "Completed: " + str(counter) + " Last completed query: " + resultString
                                    # print resultString


fw.close()