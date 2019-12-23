import gzip
import zipfile
import time, glob, re, sys
from lxml import etree
from unlzw import unlzw


# Server setting
docPath = "/volumes/data/Phd/Data/Hard2003_docs/cr"
docCount = 0
yearFolders = glob.glob(docPath + "/*")
for yearFold in yearFolders:
    print("Processing folder: {}".format(yearFold))
    for f in glob.glob(yearFold + "/*"):
        if f.endswith('.gz'):
            gzf = gzip.open(f, mode='rb')
            temp = gzf.read()
        elif f.endswith('.Z'):
            gzf = open(f, 'r').read()
            temp = unlzw(gzf)
            temp = temp.decode('utf-8',errors='replace').encode('utf-8')
        else:
            temp = open(f, 'r').read()
            temp = temp.decode('utf-8', errors='replace').encode('utf-8')


        temp = re.sub(r'&\w{2,6};', '', temp)
        temp = temp.replace("<P>", " ").replace("</P>", " ")
        #print(temp)
        content = "<ROOT>" + temp + "</ROOT>"

        root = etree.fromstring(content)

        for doc in root.findall('DOC'):
            #docNo = doc.find('DOCNO').text.strip()
            #print(docNo)
            docCount += 1

        print("Finished file: {} Current Doc Count: {}".format(f,docCount))

print("Total Doc Count: {}".format(docCount))