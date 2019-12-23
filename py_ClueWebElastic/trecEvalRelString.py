import commands, json, os


dataSet = 'clef2016'

if dataSet == 'clef2015':
    # CLEF2015 setting
    qrelPath = "/volumes/data/phd/data/clef2015_eval/qrels.eng.clef2015.test.graded.txt"
    topPath = "/volumes/data/phd/data/clef2015_topFiles/"
    topPrefix = "clef2015_top_"
    resultFile = "relStringDetailResult_CLEF2015.csv"


elif dataSet == 'clef2016':
    qrelPath = "/volumes/data/phd/data/clef2016_eval/task1.qrels.30Aug"
    topPath = "/volumes/data/phd/data/clef2016_topFiles/"
    topPrefix = "clef2016_top_"
    resultFile = "relStringDetailResult_CLEF2016.csv"


elif dataSet == 'hard2005':
    qrelPath = "/volumes/data/phd/data/aquaint_eval/TREC2005.qrels.txt"
    topPath = "/volumes/data/phd/data/aquaint_topFiles/"
    topPrefix = "aquaint_"
    resultFile = "relStringDetailResult_HARD2005.csv"

elif dataSet == 'adhoc2013-2014':
    qrelPath = "/volumes/data/phd/data/AdHoc2013-2014_eval/qrels.adhoc2013-2014.txt"
    topPath = "/volumes/data/phd/data/AdhocNav_2013-2014_topFiles/"
    topPrefix = "adhocNav_2013-2014_top_"
    resultFile = "relStringResult_AdHocNav2013-2014.csv"


resultPath = "/Volumes/Data/Github/adcs2016_chs_fields/adcs2016_chs_fieldSelection/"
trecPath = "/users/n9546031/tools/trec_eval.9.0/"

if not os.path.exists(qrelPath):
    raise NameError('Qrel Not Found')


titleWeights = [0, 1, 3]
metaWeights = [0]
headersWeights = [0]
bodyWeights = [0, 1, 3]
tieBreakers = [ "25.0"]

if not os.path.exists(resultPath):
    os.makedirs(resultPath)

fw = open(resultPath + resultFile, 'w')
fw.write("IndexName" + "," + "QueryNumber" + "," + 'relString' + ',' + 'NumUnjudged' + ',' + "bpref"  + "\n")

if dataSet == 'hard2005':
    for tw in titleWeights:
        for bw in bodyWeights:
            if tw + bw >= 1:
                for tie in tieBreakers:
                    # Index Setting
                    fileName = topPrefix + format(tw, '02') + format(bw, '02')  +  "_" + tie
                    if not os.path.exists(topPath + fileName):
                        raise NameError('Top File Not Found')


                    trecResults = commands.getoutput(
                           trecPath + 'trec_eval -q -m relstring.10 -m bpref ' + qrelPath + " " + topPath + fileName)


                    relString = bpref = ""
                    numUnjudged = 0
                    for res in trecResults.splitlines():
                        measure = res.split()[0]

                        qNum = res.split()[1]
                        score = res.split()[2]
                        if qNum != "all":
                            if measure == "bpref":
                                bpref = score
                            elif measure == "relstring_10":
                                relString = score
                                numUnjudged = relString.count('-')
                                fw.write(
                                    fileName + "," + qNum + "," + relString + "," + str(
                                        numUnjudged) + "," + bpref + "\n")
                    print ("File name: {0} Completed".format(fileName))
else:
    for tw in titleWeights:
        for mw in metaWeights:
            for hw in headersWeights:
                for bw in bodyWeights:
                    if tw + mw + hw + bw >= 1:
                        for tie in tieBreakers:
                            # Index Setting
                            fileName = topPrefix + format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02') + \
                                   "_" + tie
                            if not os.path.exists(topPath + fileName):
                                raise NameError('Top File Not Found')

                            trecResults = commands.getoutput(trecPath + 'trec_eval -q -m bpref -m relstring.10 ' + qrelPath + " " + topPath + fileName)

                            relString = bpref = ""
                            numUnjudged = 0
                            for res in trecResults.splitlines():
                                measure = res.split()[0]

                                qNum = res.split()[1]
                                score = res.split()[2]
                                if qNum != "all":
                                    if measure == "bpref":
                                        bpref = score
                                    elif measure == "relstring_10":
                                        relString = score
                                        numUnjudged = relString.count('-')
                                        fw.write(
                                            fileName + "," + qNum + "," + relString  + "," + str(numUnjudged) + "," + bpref + "\n")
                            print ("File name: {0} Completed".format(fileName))

fw.close()
