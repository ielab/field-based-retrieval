import commands, os
import multiprocessing
import glob

# Local setting
qrelPath = "/volumes/data/phd/data/aquaint_eval/TREC2005.qrels.txt"
topPath = "/volumes/data/phd/data/aquaint_topfiles/"
resultPath = "/volumes/data/phd/subresearches/fieldweight/r/"
trecPath = "/users/n9546031/tools/trec_eval.9.0/"


# Server setting
qrelPath = "/volumes/ext/data/aquaint_eval/TREC2005.qrels.txt"
topPath = '/volumes/ext/jimmy/data/aquaintTunedVary_topfiles/'
resultPath = "/volumes/ext/jimmy/experiments/fielded_retrieval/treceval_results/"
trecPath = "/volumes/ext/tools/trec_eval.9.0/"

titleWeights = [0, 1, 3, 5]
bodyWeights = [0, 1, 3, 5]
tieBreakers = [0.25]

if not os.path.exists(resultPath):
    os.makedirs(resultPath)


def eval(fname):
    trecResults = commands.getoutput(trecPath + 'trec_eval -q -m map -m P.10 -m ndcg_cut.10,1000 -m bpref -m relstring.10 ' + qrelPath + " "  + fname)
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

fw = open(resultPath + "trecResultSummaryTunedVaried_aquaint_hard2005.txt", 'w')
fw.write("schema" + " " + " QueryNum"+ " " + "map" + " " + "p10" + " " + "ndcg10" + " " + "ndcg1000" + " " + "bpref" + " " + "unjudged" + " " + "relString"+ "\n")

p = multiprocessing.Pool()
resultString = p.map(eval, fileNames)
for res in resultString:
    fw.write(res)
p.close()
p.join()

fw.close()
