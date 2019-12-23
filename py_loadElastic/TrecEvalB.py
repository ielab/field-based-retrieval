import commands, json, os

dataSet = "2015"

if dataSet == "2015":
    # CLEF 2015 setting
    qrelPath = "/volumes/data/Phd/Data/clef2015_eval/qrels.eng.clef2015.test.graded.txt"
    topPath = "/volumes/data/Phd/Data/clef2015_topFiles/"
    resultFile = "trecSummaryResult_Clef2015.txt"
    prefixFile = "clef2015_top_"
elif dataSet == "2014":
    # CLEF 2014 setting
    qrelPath = "/volumes/data/Phd/Data/clef2014_eval/qrels.clef2014.test.graded.txt"
    topPath = "/volumes/data/Phd/Data/clef2014_topFiles/"
    resultFile = "trecSummaryResult_Clef2014.txt"
    prefixFile = "clef2014_top_"

resultPath = "/volumes/data/phd/subresearches/fieldweight/r/"
trecPath = "/users/n9546031/tools/trec_eval.9.0/"


titleWeights = [0, 1, 3, 5]
metaWeights = [0, 1, 3, 5]
headersWeights = [0, 1, 3, 5]
bodyWeights = [0, 1, 3, 5]
tieBreakers = [0.25]

if not os.path.exists(resultPath):
    os.makedirs(resultPath)

jsonData = []
fw = open(resultPath + resultFile, 'w')
fw.write("IndexName" + " " + "map" + " " + "P10"+ " " + "ndcg10" + " " + "ndcg1000" + "\n")

for tw in titleWeights:
    for mw in metaWeights:
        for hw in headersWeights:
            for bw in bodyWeights:
                for tie in tieBreakers:
                    # Index Setting
                    # indexName format: clef2015_aabbccdd. aa=title weight, bb=meta weight, cc=headers weight, dd=body weight
                    fileName = prefixFile + format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02') + \
                               "_" + format(tie * 100, '03')

                    map = p10 = ndcg10 = ndcg1000 = 0

                    map = commands.getoutput(trecPath + 'trec_eval -m map ' + qrelPath + " " + topPath + fileName).split()[2]
                    p10 = commands.getoutput(trecPath + 'trec_eval -m P.10 ' + qrelPath + " " + topPath + fileName).split()[2]
                    ndcg10 = commands.getoutput(trecPath + 'trec_eval -m ndcg_cut.10 ' + qrelPath + " " + topPath + fileName).split()[2]
                    ndcg1000 = commands.getoutput(trecPath + 'trec_eval -m ndcg_cut.1000 ' + qrelPath + " " + topPath + fileName).split()[2]

                    fw.write(fileName + " " + map + " " + p10 + " " + ndcg10 + " " + ndcg1000 + "\n")
                    print ("File name: {0} Completed".format(fileName))
fw.close()
