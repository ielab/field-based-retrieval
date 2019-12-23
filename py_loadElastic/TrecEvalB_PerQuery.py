import commands, json, os

dataSet = "2015"

if dataSet == "2015":
    qrelPath = "/volumes/data/Phd/Data/clef2015_eval/qrels.eng.clef2015.test.graded.txt"
    topPath = "/volumes/data/Phd/Data/clef2015_topFiles/"
    resultFile = "trecDetailResult_Clef2015.csv"
    prefixFile = "clef2015_top_"

elif dataSet == "2014":
    qrelPath = "/volumes/data/Phd/Data/clef2014_eval/qrels.clef2014.test.graded.txt"
    topPath = "/volumes/data/Phd/Data/clef2014_topFiles/"
    resultFile = "trecDetailResult_Clef2014.csv"
    prefixFile = "clef2014_top_"

resultPath = "/volumes/data/phd/subresearches/fieldweight/"
trecPath = "/users/n9546031/tools/trec_eval.9.0/"
titleWeights = [0, 1, 3, 5]
metaWeights = [0, 1, 3, 5]
headersWeights = [0, 1, 3, 5]
bodyWeights = [0, 1, 3, 5]
tieBreakers = [0.25]

if not os.path.exists(resultPath):
    os.makedirs(resultPath)

fw = open(resultPath + resultFile, 'w')
fw.write("IndexName" + "," + "QueryNumber" + "," + "map" + "," + "p10" + "," + "ndcg10" + "," + "ndcg1000"+ "\n")

for tw in titleWeights:
    for mw in metaWeights:
        for hw in headersWeights:
            for bw in bodyWeights:
                if tw + mw + hw + bw >= 1:
                    for tie in tieBreakers:
                        # Index Setting
                        # indexName format: clef2015_aabbccdd. aa=title weight, bb=meta weight, cc=headers weight, dd=body weight
                        fileName = prefixFile + format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02') + \
                                   "_" + format(tie * 100, '03')

                        if not os.path.exists(topPath + fileName):
                            raise NameError('Top File Not Found')

                        trecResults = commands.getoutput(
                            trecPath + 'trec_eval -q -m map -m P.10 -m ndcg_cut.10,1000 ' + qrelPath + " " + topPath + fileName)

                        map = p10 = ndcg10 = ndcg1000 = 0
                        for res in trecResults.splitlines():
                            measure = res.split()[0]
                            qNum = res.split()[1]
                            score = res.split()[2]
                            if qNum != "all":
                                if measure == "map":
                                    map = score
                                elif measure == "P_10":
                                    p10 = score
                                elif measure == "ndcg_cut_10":
                                    ndcg10 = score
                                elif measure == "ndcg_cut_1000":
                                    ndcg1000 = score

                                    fw.write(
                                        fileName + "," + qNum + "," + map + "," + p10 + "," + ndcg10 + "," + ndcg1000 + "\n")

                        print ("File name: {0} Completed".format(fileName))
fw.close()
