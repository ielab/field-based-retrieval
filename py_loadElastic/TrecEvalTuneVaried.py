import commands, os
import glob
import multiprocessing

dataSet = "WEB2013-2014"

if dataSet == "CLEF2015":
    qrelPath = "/volumes/ext/data/clef2015_eval/qrels.eng.clef2015.test.graded.txt"
    topPath = "/volumes/ext/jimmy/data/clef2015TunedVaried_topfiles/"
    resultFile = "trecDetailResultTunedVaried_Clef2015.txt"
    #prefixFile = "clef2015_"

elif dataSet == "CLEF2016":
    qrelPath = "/volumes/ext/data/clef2016_eval/task1.qrels.30Aug"
    topPath = '/volumes/ext/jimmy/data/clef2016TunedVaried_topFiles/'
    resultFile = "trecDetailResultTunedVaried_Clef2016.txt"
    #prefixFile = "clef2016_"
elif dataSet == "WEB2013-2014":
    qrelPath = "/volumes/ext/data/webTrack2013-2014_eval/qrels.adhoc2013-2014.txt"
    topPath = '/volumes/ext/jimmy/data/clueweb12/WEBNav2013-2014TunedVaried_topFiles/'
    resultFile = "trecDetailResultTunedVaried_webNav2013-2014.txt"
    #prefixFile = "webNav2013-2014_"

resultPath = "/volumes/ext/jimmy/experiments/fielded_retrieval/treceval_results/"
trecPath = "/volumes/ext/tools/trec_eval.9.0/"
''' Not required as we will read all files in a folder
titleWeights = [0, 1, 3, 5]
metaWeights = [0]
headersWeights = [0]
bodyWeights = [0, 1, 3, 5]
tieBreakers = [0.25]
'''


def eval(fname):
    trecResults = commands.getoutput(trecPath + 'trec_eval -q -m map -m P.10 -m ndcg_cut.10,1000 -m bpref -m relstring.10 ' + qrelPath + " " + fname)
    filename = os.path.basename(fname)

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

                resultString+= filename + " " + qNum + " " + map + " " + p10 + " " + ndcg10 + " " + ndcg1000 + " " + bpref + " " + str(numUnjudged) + " " + relString + "\n"

    print ("File name: {0} Completed".format(filename))

    return resultString

fileNames = glob.glob(topPath + "*")

if not os.path.exists(resultPath):
    os.makedirs(resultPath)

fw = open(resultPath + resultFile, 'w')
fw.write("schema" + " " + " QueryNum"+ " " + "map" + " " + "p10" + " " + "ndcg10" + " " + "ndcg1000" + " " + "bpref" + " " + "unjudged" + " " + "relString"+ "\n")

p = multiprocessing.Pool()
resultString = p.map(eval, fileNames)
for res in resultString:
    fw.write(res)
p.close()
p.join()

fw.close()
