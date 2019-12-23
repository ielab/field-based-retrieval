import commands, json, os

# Aquaint setting
qrelPath = "/volumes/data/phd/data/aquaint_eval/TREC2005.qrels.txt"
topPath = "/volumes/data/phd/data/aquaint_topfiles/"
resultPath = "/volumes/data/phd/subresearches/fieldweight/r/"
trecPath = "/users/n9546031/tools/trec_eval.9.0/"

headlineWeights = [0, 1, 3, 5]
textWeights = [0, 1, 3, 5]
tieBreakers = [0.25]

if not os.path.exists(resultPath):
    os.makedirs(resultPath)

jsonData = []
fw = open(resultPath + "trecResultSummary_aquaint_hard2005.txt", 'w')
fw.write("IndexName" + " " + "ndcg" + " " + "p10" + "\n")

for hw in headlineWeights:
    for tw in textWeights:
        for tie in tieBreakers:
            # Index Setting
            # indexName format: aquaint_aabb. aa=title weight, bb=meta weight
            fileName = "aquaint_" + format(hw, '02') + format(tw, '02') + "_" + format(tie * 100, '03')

            ndcg = commands.getoutput(trecPath + 'trec_eval -m ndcg.0=0,1=1,2=3 ' + qrelPath + " " + topPath + fileName).split()[2]
            p10 = commands.getoutput(trecPath + 'trec_eval -m P.10 ' + qrelPath + " " + topPath + fileName).split()[2]
            print commands.getoutput(trecPath + 'trec_eval -m P.10 ' + qrelPath + " " + topPath + fileName)
            fw.write(fileName + " " + ndcg + " " + p10 + "\n")
            print ("File name: {0} Completed".format(fileName))
fw.close()
