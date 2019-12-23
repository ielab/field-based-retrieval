from collections import defaultdict

qrelFile = "/volumes/Data/Phd/Data/Hard2003_eval/qrels.actual.03.txt"

countJudge = defaultdict(lambda: 0)
countRel = defaultdict(lambda:0)
with open(qrelFile, "r") as infile:
    for line in infile:
        topicId, temp, docId, relScore = line.split()
        countJudge[docId[:3]] += 1
        if int(relScore) > 0:
            countRel[docId[:3]] += 1

for key in countJudge:
    print('collection: {}, Judged: {}, Relevant: {}'.format(key, countJudge[key], countRel[key]))