import commands, json, os

# local setting
qrelPath = "/volumes/data/phd/data/clueweb12_eval/qrels.ndeval.txt"
topPath = "/volumes/data/phd/data/clueweb12_topFiles/"
resultPath = "/volumes/data/phd/subresearches/fieldweight/r/"
ndevalPath = "/volumes/data/phd/data/trec2014_ndeval/"
titleWeights = [0, 1, 3, 5]
metaWeights = [0, 1, 3, 5]
headersWeights = [0, 1, 3, 5]
bodyWeights = [0, 1, 3, 5]
tieBreakers = ["0.0", "25.0", "50.0", "100.0"]

if not os.path.exists(resultPath):
    os.makedirs(resultPath)

fw = open(resultPath + "DetailResult_clueweb12.txt", 'w')
fw.write("IndexName" + "," + "QueryNumber" + "," + "p10" + "\n")

fsw = open(resultPath + "SummaryResult_clueweb12.txt", 'w')
fsw.write("IndexName" + "," + "p10" + "\n")

for tw in titleWeights:
    for mw in metaWeights:
        for hw in headersWeights:
            for bw in bodyWeights:
                if tw + mw + hw + bw >= 1:
                    for tie in tieBreakers:
                        # Index Setting
                        # indexName format: clef2015_aabbccdd. aa=title weight, bb=meta weight, cc=headers weight, dd=body weight
                        fileName = "clueweb12_top_" + format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02') + \
                                   "_" + tie

                        evalResults = commands.getoutput(ndevalPath + 'ndeval -c -traditional ' + qrelPath + " " + topPath + fileName)
                        for res in evalResults.splitlines():
                            row = res.split(",")
                            qNum = row[1]
                            p10 = row[18]
                            if qNum != "amean":
                                #print "Query Number: " + qNum + " p10: " + p10
                                fw.write(fileName + "," + qNum + "," + p10 + "\n")
                            else:
                                fsw.write(fileName + "," + p10 + "\n")
                        print ("File name: {0} Completed".format(fileName))
fw.close()
fsw.close()
