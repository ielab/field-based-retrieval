import commands, os
import glob

trecPath = "/volumes/Data/tools/trec_eval.9.0/"
qrelPath = "/volumes/Data/Phd/data/clef2016_eval/task1.qrels.30Aug"
baselinePath = "/Volumes/Data/Phd/Data/clef2016_eval/CLEF2016_Results/*.txt"
resultPath = "/Volumes/Data/Phd/Data/clef2016_eval/CLEF2016_Results/trecEvalResults/"

files = glob.glob(baselinePath)

for topFile in files:
    filename = os.path.basename(topFile).split(".txt")[0]
    print filename

    fw = open(resultPath + "trecEval_" + filename + ".csv", 'w')

    fw.write("schema" + " " + " QueryNum" + " " + "map" + " " + "p10" + " " + "ndcg10" + " " + "ndcg1000" + " " + "bpref" + " " + "unjudged" + " " + "relString" + "\n")

    trecResults = commands.getoutput(trecPath + 'trec_eval -q -m map -m P.10 -m ndcg_cut.10,1000 -m bpref -m relstring.10 ' + qrelPath + " " + topFile)

    #print trecResults

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

                resultString = filename + " " + qNum + " " + map + " " + p10 + " " + ndcg10 + " " + ndcg1000 + " " + bpref + " " + str(numUnjudged) + " " + relString + "\n"
                fw.write(resultString)

    fw.close()
    print "finish trec eval: " + topFile
