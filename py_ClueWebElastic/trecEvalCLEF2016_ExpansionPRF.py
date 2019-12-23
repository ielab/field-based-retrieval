import commands, os
import argparse

trecPath = "/volumes/ext/tools/trec_eval.9.0/"
qrelPath = "/volumes/ext/jimmy/data/clef2016_eval/task1.qrels.30Aug"

parser = argparse.ArgumentParser()
parser.add_argument("-s",
                    "--scheme",
                    help="1=all fields; "
                         "2=terms in title, alias From Matching Title, and categories From Matching Title; "
                         "3=original only; "
                         "4=original and alias from matching title; "
                         "5=original and terms in title]; "
                         "6=original and categories From Matching Title; "
                         "7=original and Title from martching alias; "
                         "8=original and Title from matching link; "
                         "9=original and Title contain query term; "
                         "10=original, terms in title, categories from matching title, title from matching alias and body; "
                         "11= original and title from matching body; "
                         "12= original and alias from matching body",
                    type=int,
                    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])

parser.add_argument("-k",
                    "--knowledgebase",
                    help="wiki or pubmed",
                    choices=["wiki", "pubmed"])
args = parser.parse_args()

expScheme = args.scheme
rankTreshold = 20
scoreTreshold = 8
maxLength = 3

kb = args.knowledgebase

titleWeight = 1
metaWeight = 0
headersWeight = 0
bodyWeight = 3
bt = 75
bb = 75
k = 12
weightScheme = str(titleWeight).zfill(2) + str(metaWeight).zfill(2) + str(headersWeight).zfill(2) + str(bodyWeight).zfill(2)

topFileName=""
resultFile=""
if kb == "wiki":
    #topPath = '/volumes/ext/jimmy/data/expWikiUmls_Clef2016_topFiles/'
    #topFileName = "topExpWikiUmlsHealth_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)
    #resultFile = "trecEval_ExpWikiUmlsHealth_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)
    topPath = '/volumes/ext/jimmy/data/expWiki_Clef2016_topFiles/'
    #topFileName = "topExpWikiHealth_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)
    #resultFile = "trecEval_ExpWikiHealth_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)

    ''' # setting for expansion using Wikipedia based on Infobox health types and health link'''
    topFileName = "topExpWikiInfoboxHealthLinks_" + weightScheme + "_rank" + str(rankTreshold).zfill(
        2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)
    resultFile = "trecEval_ExpWikiInfoboxHealthLinks_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)

    '''# setting for manual link
    topFileName = "topExpWikiHealthManualLink_" + weightScheme + "_rank" + str(rankTreshold).zfill(2) + '_score' + str(
        scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)
    resultFile = "trecEval_ExpWikiHealthManualLink_" + weightScheme + "_rank" + str(rankTreshold).zfill(
        2) + '_score' + str(
        scoreTreshold).zfill(2) + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)
    '''
elif kb == "pubmed":
    topPath = '/volumes/ext/jimmy/data/expPubMed_Clef2016_topFiles/'
    topFileName = "topExpPubMed_" + weightScheme + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme)
    resultFile = "trecEval_ExpPubmed_" + weightScheme + '_len' + str(maxLength) + "_Clef2016_" + str(expScheme) + ".csv"

# evaluate the top files WITHOUT Pseudo Relevance Feedback (PRF)
fw = open(topPath + resultFile + ".csv", 'w')
fw.write("schema" + " " + " QueryNum"+ " " + "map" + " " + "p10" + " " + "ndcg10" + " " + "ndcg1000" + " " + "bpref" +
         " " + "unjudged" + " " + "relString" + " " + "rbp10" + " " + "err10" + "\n")

if not os.path.exists(topPath + topFileName):
    raise NameError('Top File Not Found' + topPath + topFileName)

# execute rbp eval
rbpResults = commands.getoutput('rbp_eval -q -d 10 -p 0.5 -T -H ' + qrelPath + " " + topPath + topFileName)

rbps = dict()
for res in rbpResults.splitlines():
    parts = res.split()
    qNum = parts[3]
    rbp = parts[7]
    err = parts[8]
    rbps[qNum] = rbp + err

# execute trec eval
trecResults = commands.getoutput(trecPath +
                                 'trec_eval -q -m map -m P.10 -m ndcg_cut.10,1000 -m bpref -m relstring.10 ' +
                                 qrelPath + " " + topPath + topFileName)
filename = os.path.basename(topPath + topFileName)

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

            trbp = rbps[qNum].split("+")

            resultString = filename + " " + qNum + " " + map + " " + p10 + " " + ndcg10 + " " + ndcg1000 + " " + \
                           bpref + " " + str(numUnjudged) + " " + relString + " " + trbp[0] + " " + trbp[1] + "\n"
            fw.write(resultString)


fw.close()
print ("File name: {0} Completed".format(topFileName))


# evaluate the top files WITH Pseudo Relevance Feedback (PRF)
fw = open(topPath + resultFile + "_prf.csv", 'w')
fw.write("schema" + " " + " QueryNum" + " " + "map" + " " + "p10" + " " + "ndcg10" + " " + "ndcg1000" + " " + "bpref" +
         " " + "unjudged" + " " + "relString" + " " + "rbp10" + " " + "err10" + "\n")


if not os.path.exists(topPath + topFileName + "_prf"):
    raise NameError('Top File Not Found' + topPath + topFileName + "_prf")

# execute rbp eval
rbpResults = commands.getoutput('rbp_eval -q -d 10 -p 0.5 -T -H ' + qrelPath + " " + topPath + topFileName + "_prf")

rbps = dict()
for res in rbpResults.splitlines():
    parts = res.split()
    qNum = parts[3]
    rbp = parts[7]
    err = parts[8]
    rbps[qNum] = rbp + err

# execute trec eval
trecResults = commands.getoutput(trecPath +
                                 'trec_eval -q -m map -m P.10 -m ndcg_cut.10,1000 -m bpref -m relstring.10 ' +
                                 qrelPath + " " + topPath + topFileName + "_prf")
filename = os.path.basename(topPath + topFileName + "_prf")

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

            trbp = rbps[qNum].split("+")

            resultString = filename + " " + qNum + " " + map + " " + p10 + " " + ndcg10 + " " + ndcg1000 + " " + \
                           bpref + " " + str(numUnjudged) + " " + relString + " " + trbp[0] + " " + trbp[1] + "\n"
            fw.write(resultString)


fw.close()
print ("File name: {0} Completed".format(topFileName + "_prf"))

