import gzip
import time
import glob
import re
import sys
from lxml import etree


# Server setting
docPath = "/volumes/ext/data/aquaint/"
outputFile = topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/fieldLength_Aquaint.txt'



startTime = time.time()
bulk_data = []
lapTime = time.time()
indexTime = time.time()

i = 0
totalSize = 0
docNo = ""
headline = ""
text = ""

fw = open(outputFile,'w')
sourceFolders = glob.glob(docPath + "/*")
for sourceFold in sourceFolders:
    print "****************   Processing Source: " + sourceFold
    yearFolders = glob.glob(sourceFold + "/*")
    for yearFold in yearFolders:
        print "Processing Year folder: " + yearFold
        print "Processing folder: " + yearFold
        for f in glob.glob(yearFold + "/*"):
            print "Processing file: " + f
            with gzip.open(f, mode='rb') as gzf:
                temp = gzf.read()
                temp = re.sub(r'&\w{2,6};', '', temp)
                temp = temp.replace("<P>", " ").replace("</P>", " ")
                content = "<ROOT>" + temp + "</ROOT>"
                try:
                    root = etree.fromstring(content)

                    for doc in root.findall('DOC'):
                        docNo = ""
                        headline = ""
                        text = ""

                        docNo = doc.find('DOCNO').text.strip()
                        body = doc.find('BODY')

                        try:
                            headline = body.find('HEADLINE').text.strip()
                        except:
                            headline = ""

                        try:
                            text = body.find('TEXT').text.strip()
                        except:
                            text = ""

                        fw.write('{},{},{}\n'.format(docNo, len(headline.split()), len(text.split())))

                except:
                    print "~~~" + docNo
                    print content
                    raise
fw.close()