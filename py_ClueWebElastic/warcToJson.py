import gzip
import warc


warcPath = "/Volumes/Data/Phd/Data/clueweb12/0001wb-00.warc.gz"

i = 0
with gzip.open(warcPath, mode='rb') as gzf:
    for record in warc.WARCFile(fileobj=gzf):
        if i > 40:
            print str(i) + " >>> Type: >>>" + str(record.header.get('WARC-Type'))
            print str(i) + " >>> Target URL: >>>" + str(record.header.get('WARC-Target-URI'))
            print str(i) + " >>> Record-ID : >>>" + str(record.header)
            #docString = record.payload.read()
            #htmlStart = docString.find('<html')
            #htmlString = docString[htmlStart:]
            # print htmlString
            print str(i) + " >>> " + record.payload.read()

        # print str(i) + " >>> " + record.payload.read()


        if i == 45:
            break;
        i += 1