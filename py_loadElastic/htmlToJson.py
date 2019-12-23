import time, glob, os, json, lxml, re, io
from lxml.html.clean import Cleaner
from shutil import copyfile
# from htmlmin import minify

# local setting
path = '/applications/mamp/htdocs/clef2015_0k/*'
jsonPath = '/applications/mamp/htdocs/json_0k/'
errorPath= '/applications/mamp/htdocs/error_0k/'

# server setting
#path = '/users/jimmy/data/clef2015/*'
#jsonPath = '/users/jimmy/data/json2015/'
#errorPath= '/users/jimmy/data/error_json_clef2015/'

if not os.path.exists(jsonPath):
    os.makedirs(jsonPath)

if not os.path.exists(errorPath):
    os.makedirs(errorPath)

cleaner = Cleaner()
cleaner.javascript = True
cleaner.style = True
cleaner.scripts = True
cleaner.comments = True

files = glob.glob(path)
totalFiles = len(files)

i = 0.0
errorCount = 0
startTime = time.time()
lapTime = time.time()

for f in files:
    #try:
        fr = open(f, 'r')
        temp = fr.read()

        temp = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\xff]', '', temp)
        temp = re.sub(r'&\w{4,6};', '', temp)
        temp = temp.replace(",", " ").replace("-", " ").replace(".", " ")
        fContent = io.BytesIO(temp)

        htmlDoc = lxml.html.parse(fContent)

        docId = os.path.basename(f)

        # the html.xpath return an array so need to convert it to string with join method
        title = " ".join(htmlDoc.xpath('//title/text()'))

        # populate meta by searching for keywords and description
        meta = " ".join(htmlDoc.xpath('//meta[@name="keywords" or @name="description"]/@content'))

        # populate headers by searching for h1, h2, h3, h4, h5 and h6
        raw = htmlDoc.xpath('//body//*[self::h1 or self::h2 or self::h3 or self::h4 or self::h5 or self::h6]')

        headers = ""
        for h in raw:
            headers = headers + " " + h.text_content()

        # Replace comma with space so words surrounding text can be detected as alphanumeric words
        #headers = headers.replace(",", " ").replace("-", " ").replace(".", " ")
        # Remove non alphanumeric words
        headers = " ".join(word for word in headers.split() if word.isalnum())

        rootClean = htmlDoc.getroot()

        body = ""
        try:
            body = rootClean.body.text_content()

            # body = ' '.join(word for word in body.split() if word.isalnum() and word.lower() not in stopWordList)
            body = ' '.join(word for word in body.split() if word.isalnum())
        except:
            body = ""

        jsonData = {
            'title': title,
            'meta': meta,
            'headers': headers,
            'body': body
        }
        with open(jsonPath + docId, 'w') as fw:
            json.dump(jsonData, fw)
            fw.close()

        i += 1

        if i % 1000 == 0:
            print ("{0} Completed in: {1} seconds, total Error: {2}". format(str(i), time.time()-lapTime, errorCount))
            lapTime = time.time()
    #except Exception as e:
    #    print("unexpected error:", f, e)
    #    copyfile(f, errorPath + docId)
    #    errorCount += 1

print "Total Processed Files:", i
print (" Duration ", time.time()-startTime)
