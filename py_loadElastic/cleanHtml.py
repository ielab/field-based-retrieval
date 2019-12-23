import lxml.html, time, glob, os, json, lxml.etree
from lxml.html.clean import Cleaner
from htmlmin import minify

path = '/applications/mamp/htdocs/clef2015_30k/*'
cleanPath = '/applications/mamp/htdocs/clean_30k/'
stopWordFile = '/users/n9546031/tools/terrier-stop.txt'

# Load stop words
stopWordList = [line.rstrip('\r\n') for line in open(stopWordFile)]

if not os.path.exists(cleanPath):
    os.makedirs(cleanPath)

cleaner = Cleaner()
cleaner.javascript = True
cleaner.style = True
cleaner.scripts = True
cleaner.comments = True

files = glob.glob(path)
totalFiles = len(files)

i = 0.0
startTime = time.time()

for f in files:
    fr = open(f, 'r')
    docId = os.path.basename(f)

    fw = open(cleanPath + docId, 'w')

    htmlDoc = lxml.html.parse(f)
    docId = os.path.basename(f)

    # the html.xpath return an array so need to convert it to string with join method
    title = " ".join(htmlDoc.xpath('//title/text()'))

    # populate meta by searching for keywords and description
    meta = " ".join(htmlDoc.xpath('//meta[@name="keywords" or @name="description"]/@content'))

    # populate headers by searching for h1, h2, h3, h4, h5 and h6
    #headers = " ".join(
    #    htmlDoc.xpath('//body/*[self::h1 or self::h2 or self::h3 or self::h4 or self::h5 or self::h6]/text()'))
    raw = htmlDoc.xpath('//h1')
    h1 = ""
    for h in raw:
        h1 = h1 + " "+ h.text_content()

    raw = htmlDoc.xpath('//h2')
    h2 = ""
    for h in raw:
        h2 = h2 + " "+ h.text_content()

    raw = htmlDoc.xpath('//h3')
    h3 = ""
    for h in raw:
        h3 = h3 + " " + h.text_content()

    raw = htmlDoc.xpath('//h4')
    h4 = ""
    for h in raw:
        h4 = h4 + " " + h.text_content()

    raw = htmlDoc.xpath('//h5')
    h5 = ""
    for h in raw:
        h5 = h5 + " " + h.text_content()

    raw = htmlDoc.xpath('//h6')
    h6 = ""
    for h in raw:
        h6 = h6 + " " + h.text_content()

    headers = h1 + " " + h2 + " " + h3 + " " + h4 + " " + h5 + " " + h6

    # Replace comma with space so words surrounding text can be detected as alphanumeric words
    headers = headers.replace(",", " ")
    # Remove non alphanumeric words
    headers = " ".join(word for word in headers.split() if word.isalnum() and word.lower() not in stopWordList)

    htmlClean = cleaner.clean_html(htmlDoc)
    rootClean = htmlClean.getroot()
    body = rootClean.body.text_content()
    body = minify(body, remove_comments=True, remove_empty_space=True)
    body = body.replace(",", " ")
    body = ' '.join(word for word in body.split() if word.isalnum() and word.lower() not in stopWordList)

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
        print (i, " Completed")


print (" Duration ", time.time()-startTime)