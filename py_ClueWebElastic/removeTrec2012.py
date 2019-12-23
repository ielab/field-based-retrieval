import time, os


# local setting
path2012_2014 = '/volumes/data/phd/data/AdHoc2012-2014_TopFiles/'
path2013_2014 = '/volumes/data/phd/data/AdHoc2013-2014_TopFiles/'
titleWeights = [0, 1, 3, 5]
metaWeights = [0, 1, 3, 5]
headersWeights = [0, 1, 3, 5]
bodyWeights = [0, 1, 3, 5]
tieBreakers = [0.25]

if not os.path.exists(path2013_2014):
    os.makedirs(path2013_2014)

for tw in titleWeights:
    for mw in metaWeights:
        for hw in headersWeights:
            for bw in bodyWeights:
                if tw + mw + hw + bw >= 1:
                    for tie in tieBreakers:
                        weights = format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02')

                        fr = open(path2012_2014 + "adhoc_2012-2014_top" + "_" + weights + "_" + format(tie*100, '03'), 'r')
                        fw = open(path2013_2014 + "adhoc_2013-2014_top" + "_" + weights + "_" + format(tie * 100, '03'), 'w')

                        content = fr.readline()
                        while content:
                            contentList = content.split(' ')
                            if int(contentList[0])>200:
                                fw.write(content)
                            content = fr.readline()
                        fr.close()
                        fw.close()

                        print ("Weights: {0} Completed".format(weights))
