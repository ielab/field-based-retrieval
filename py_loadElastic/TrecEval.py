import commands, json, os

# local setting
qrelPath = "/volumes/data/phd/subresearches/fieldweight/clef2015/qrels.eng.clef2015.test.bin.txt"
topPath = "/volumes/data/phd/subresearches/fieldweight/clef2015/a_topFiles_clef2015/"
resultPath = "/volumes/data/phd/subresearches/fieldweight/clef2015/"
trecPath = "/users/n9546031/tools/trec_eval.9.0/"
titleWeights = [0, 1, 3, 5]
metaWeights = [0, 1, 3, 5]
headersWeights = [0, 1, 3, 5] # headers could not be separated from the body. thus headers min weight is 1 unless the body weight = 0
bodyWeights = [0, 1, 3, 5]

if not os.path.exists(resultPath):
    os.makedirs(resultPath)

jsonData = []
fw = open(resultPath + "a_evalResult_clef2015.txt", 'w')
fw.write("IndexName" + " " + "ndcg" + " " + "p10" + "\n")

for tw in titleWeights:
    for mw in metaWeights:
        for hw in headersWeights:
            for bw in bodyWeights:
                # Index Setting
                # indexName format: clef2015_aabbccdd. aa=title weight, bb=meta weight, cc=headers weight, dd=body weight
                indexName = "clef2015_" + format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02')

                if indexName in ('clef2015_00000001', 'clef2015_00000003', 'clef2015_00000005', 'clef2015_00000100',
                                 'clef2015_00000101', 'clef2015_00000103', 'clef2015_00000105', 'clef2015_00000300',
                                 'clef2015_00000301', 'clef2015_00000303', 'clef2015_00000305', 'clef2015_01000000',
                                 'clef2015_01000001', 'clef2015_01000003', 'clef2015_01000005', 'clef2015_01000100',
                                 'clef2015_01000101', 'clef2015_01000103', 'clef2015_01000105', 'clef2015_01000300',
                                 'clef2015_01000301', 'clef2015_01000303', 'clef2015_01000305', 'clef2015_03000000',
                                 'clef2015_03000001', 'clef2015_03000003', 'clef2015_03000005', 'clef2015_03000100',
                                 'clef2015_03000101', 'clef2015_03000103', 'clef2015_03000105', 'clef2015_03000300',
                                 'clef2015_05000000', 'clef2015_05000001', 'clef2015_05000003', 'clef2015_05000005',
                                 'clef2015_05000100', 'clef2015_05000101', 'clef2015_05000103', 'clef2015_05000105'
                                 ):
                    ndcg = commands.getoutput(trecPath + 'trec_eval -m ndcg.0=0,1=1,2=3 ' + qrelPath + " " + topPath + indexName).split()[2]
                    p10 = commands.getoutput(trecPath + 'trec_eval -m P.10 ' + qrelPath + " " + topPath + indexName).split()[2]

                    fw.write(indexName + " " + ndcg + " " + p10 + "\n")

fw.close()
