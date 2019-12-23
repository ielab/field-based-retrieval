import subprocess
import os
import glob
import multiprocessing


import argparse

parser = argparse.ArgumentParser()


parser.add_argument("-d",
                    "--dataset",
                    help="CLEF2015,  CLEF2016, HARD2003, HARD2005, or web2013-2014",
                    choices=["CLEF2015", "CLEF2016", "HARD2003", "HARD2005", "web2013-2014"])
args = parser.parse_args()

dataSet = args.dataset

if dataSet == "CLEF2016":
    qrelPath = "/volumes/ext/data/clef2016_eval/task1.qrels.30Aug"
    topPrefix = "ter_tuneB_clef2016_a"
    resultFile = "ter_Clef2016_tuneB.eval"
elif dataSet == "HARD2003":
    qrelPath = "/volumes/ext/data/Hard2003_eval/qrels.actual.03.txt"
    topPrefix = "ter_tuneB_hard2003_a"
    resultFile = "ter_hard2003_tuneB.eval"
elif dataSet == "HARD2005":
    qrelPath = "/volumes/ext/data/aquaint_eval/TREC2005.qrels.txt"
    topPrefix = "ter_tuneB_hard2005_a"
    resultFile = "ter_Hard2005_tuneB.eval"
elif dataSet == "web2013-2014":
    qrelPath = "/volumes/ext/data/webTrack2013-2014_eval/qrels.adhoc2013-2014.txt"
    topPrefix = "ter_tuneB_web_a"
    resultFile = "ter_Web_tuneB.eval"


trecPath = "/volumes/ext/tools/trec_eval.9.0/"
dataPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/terrier/{}/'.format(dataSet)


def eval(fname):
    trecResults = subprocess.getoutput(trecPath +
                                     'trec_eval -q -m map -m P.10 -m ndcg_cut.10,1000 -m bpref -m relstring.10 '
                                     '-m recip_rank ' + qrelPath + " " + fname)
    #print trecResults
    filename = os.path.basename(fname)

    # get variables from the file name
    prefix_ter, prefix_tuneB, collection, alpha, bt, bb = filename.split('_')
    alpha = alpha.replace('a', '')
    bt = bt.replace('bt', '')
    bb = bb.replace('bb','')
    bb = bb.replace('.run', '')

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

                resultString += filename + " " + alpha + " " + bt + " " + bb + " " + qNum + " " + map + " " + p10 + \
                                " " + ndcg10 + " " + \
                                ndcg1000 + " " + bpref + " " + str(numUnjudged) + " " + relString + " " + rr + "\n"

    print ("File name: {} Completed, alpha:{} bt:{} bb:{} ".format(filename, alpha, bt, bb))

    return resultString

#fileNames = glob.glob(dataPath + topPrefix + "*.run")
#for web2013-2014, we only have partial results (due to time limitation). thus only selected runs can be used
fileNames = []
for b_title in range(0, 101, 20):
    for b_body in range(0, 101, 20):
        for alpha in range(0, 11, 2):
            fileNames.append("{prefix_run}{alpha}_bt{b_title}_bb{b_body}.run".
                             format(prefix_run=dataPath + topPrefix, alpha=alpha,
                                    b_title=float(b_title) / 100, b_body=float(b_body) / 100))


print(dataPath + topPrefix + "*.run")
print("Loaded {} files".format(len(fileNames)))

fw = open(dataPath + resultFile, 'w')
fw.write("schema alpha bt bb QueryNum map p10 ndcg10 ndcg1000 bpref unjudged relString rr \n")

p = multiprocessing.Pool()
resultString = p.map(eval, fileNames)
for res in resultString:
    fw.write(res)
p.close()
p.join()

fw.close()
