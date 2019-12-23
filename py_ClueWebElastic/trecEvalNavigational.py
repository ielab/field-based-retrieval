import commands, json, os

dataSet = 'adhocNav2013-2014'

if dataSet == 'adhocNav2013-2014':
    qrelPath = "/volumes/data/phd/data/AdHoc2013-2014_eval/qrels.adhoc2013-2014.txt"
    topPath = "/volumes/data/phd/data/AdhocNav_2013-2014_topFiles/"
    topPrefix = "adhocNav_2013-2014_top_"
    resultFile = "recipResult_L1_AdHocNav2013-2014.csv"

resultPath = "/volumes/data/phd/subresearches/fieldweight/"
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
fw.write("IndexName" + "," + "QueryNumber" + "," + "recip" + "\n")


for tw in titleWeights:
    for mw in metaWeights:
        for hw in headersWeights:
            for bw in bodyWeights:
                if tw + mw + hw + bw >= 1:
                    for tie in tieBreakers:
                        # Index Setting
                        # indexName format: clef2015_aabbccdd. aa=title weight, bb=meta weight, cc=headers weight, dd=body weight
                        fileName = topPrefix + format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02') + \
                               "_" + tie
                        if not os.path.exists(topPath + fileName):
                            raise NameError('Top File Not Found')

                        trecResults = commands.getoutput(trecPath + 'trec_eval -l1 -q -m recip_rank ' + qrelPath + " " + topPath + fileName)
                        print trecResults
                        recip = 0
                        for res in trecResults.splitlines():
                            measure = res.split()[0]
                            qNum = res.split()[1]
                            score = res.split()[2]
                            if qNum != "all":
                                recip = score

                                fw.write(fileName + "," + qNum + "," + recip + "\n")

                        print ("File name: {0} Completed".format(fileName))
fw.close()
