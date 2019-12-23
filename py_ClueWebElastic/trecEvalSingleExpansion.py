import commands, os, glob



rankTreshold = 20
scoreTreshold = 8
maxLength = 3


titleWeight = 1
metaWeight = 0
headersWeight = 0
bodyWeight = 3
bt = 75
bb = 75
k = 12
weightScheme = str(titleWeight).zfill(2) + str(metaWeight).zfill(2) + str(headersWeight).zfill(2) + str(bodyWeight).zfill(2)

topPath = "/volumes/ext/jimmy/data/singleExpWiki_Clef2016_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength)  + "topFiles/"
trecPath = "/volumes/ext/tools/trec_eval.9.0/"
qrelPath = "/volumes/ext/jimmy/data/clef2016_eval/task1.qrels.30Aug"

resultFile = "trecEval_SingleExpWiki_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength)  + "_Clef2016.csv"

# evaluate the top files
fw = open(topPath + resultFile, 'w')
fw.write("QueryNum" + "," + "expTerm" + "," + "map" + "," + "p10" + "," + "ndcg10" + "," + "ndcg1000" + "," + "bpref" + "," + "unjudged" + "," + "relString"+ "\n")

if not os.path.exists(topPath):
    raise NameError('Top Path Not Found')

counter = 0
for f in glob.glob(topPath + "*"):
    topFileName = f
    currentName = os.path.basename(topFileName)
    newName = currentName.replace(" ","-")
    os.rename(topPath + currentName, topPath + newName)
    temp = topFileName.split("_")
    expTerm = temp[len(temp)-1] #Get the expansion term which is at the end of the top file name
    #print topFileName
    trecResults = commands.getoutput(trecPath + 'trec_eval -q -m map -m P.10 -m ndcg_cut.10,1000 -m bpref -m relstring.10 ' + qrelPath + " " + topPath + newName)

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

                resultString = qNum + "," + expTerm + "," + map + "," + p10 + "," + ndcg10 + "," + ndcg1000 + "," + bpref + "," + str(numUnjudged) + "," + relString + "\n"
                fw.write(resultString)
                counter += 1
                if counter % 100 == 0:
                    print "Completed: " + str(counter) + " Last completed query: " + resultString
                #print resultString


fw.close()
print ("File name: {0} Completed".format(topFileName))
