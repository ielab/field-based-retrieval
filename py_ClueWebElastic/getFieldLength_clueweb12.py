import gzip
import warc
import time, glob, lxml.html, re, io
import multiprocessing

# server setting
warcPath = "/volumes/ext/data/clueweb12/diskB/"
warcFolder = ['clueweb12_00', 'clueweb12_01', 'clueweb12_02', 'clueweb12_03', 'clueweb12_04',
              'clueweb12_05', 'clueweb12_06', 'clueweb12_07', 'clueweb12_08', 'clueweb12_09',
              'clueweb12_10', 'clueweb12_11', 'clueweb12_12', 'clueweb12_13', 'clueweb12_14',
              'clueweb12_15', 'clueweb12_16', 'clueweb12_17', 'clueweb12_18', 'clueweb12_19']
outputFile = topPath = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/fieldLength_Clueweb12B.txt'

startTime = time.time()

def count_fieldLength(fname):
    i = 0
    lapTime = time.time()
    resultLines = ''
    print "Processing file: " + fname
    with gzip.open(fname, mode='rb') as gzf:
        WarcTotalDocuments = 0
        EmptyDocuments = 0
        for record in warc.WARCFile(fileobj=gzf):
            if record.header.get('WARC-Type').lower() == 'warcinfo':
                WarcTotalDocuments = record.header.get('WARC-Number-Of-Documents')

            if record.header.get('WARC-Type').lower() == 'response':
                docId = record.header.get('WARC-Trec-ID')
                docString = record.payload.read()

                htmlStart = 0
                htmlStart = docString.find('<html')
                if htmlStart < 1:
                    htmlStart = docString.find('<HTML')
                if htmlStart < 1:
                    htmlStart = docString.find('<Html')

                if htmlStart < 1:
                    EmptyDocuments += 1
                else:
                    htmlString = docString[htmlStart:]

                    htmlString = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\xff]', '', htmlString)
                    htmlString = re.sub(r'&\w{4,6};', '', htmlString)
                    htmlString = htmlString.replace(",", " ").replace("-", " ").replace(".", " ")

                    fContent = io.BytesIO(str(htmlString.decode("utf-8", "ignore")))

                    try:
                        htmlDoc = lxml.html.parse(fContent)

                        # the html.xpath return an array so need to convert it to string with join method
                        title = " ".join(htmlDoc.xpath('//title/text()'))
                        title = title.strip()

                        rootClean = htmlDoc.getroot()
                        body = ""
                        try:
                            body = rootClean.body.text_content()
                            body = body.strip()
                            body = ' '.join(word for word in body.split() if word.isalnum())
                        except:
                            body = ""

                        resultLines += '{},{},{}\n'.format(docId, len(title.split()), len(body.split()))
                        #fw.write('{},{},{}\n'.format(docId, len(title.split()), len(body.split())))
                        i += 1
                    except:
                        print "~~~" + docId
                        print htmlString
                        raise
    print ("File {0} Completed, Duration: {1} sec, Total: {2}, Processed: {3}, Empty: {4}, Variance: {5}".
           format(fname, time.time() - lapTime, WarcTotalDocuments, str(i), str(EmptyDocuments),
                  str(int(WarcTotalDocuments) - i - EmptyDocuments)))

    return resultLines


totalSize = 0
lapTime = time.time()
fw = open(outputFile,'w')
for warcFold in warcFolder:
    folders = glob.glob(warcPath + warcFold + "/*")
    print "****************   Processing Path: " + warcFold
    for fold in folders:
        print "Checking folder: " + fold
        if fold[-2:] == "wb":
            folderLap = time.time()
            print "Processing folder: " + fold
            # for fname in glob.glob(fold + "/*"):

            p = multiprocessing.Pool()
            resultString = p.map(count_fieldLength, glob.glob(fold + "/*"))
            p.close()
            p.join()

            for res in resultString:
                fw.write(res)

            print ("Folder {0} Completed, Duration: {1} sec".format(fold, time.time() - folderLap))


fw.close()
