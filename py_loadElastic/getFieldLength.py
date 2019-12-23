import time, glob, os, json, sys

#local setting
path = '/applications/mamp/htdocs/json_30k/*'
resultPath = '/applications/mamp/htdocs/'
bulk_size = 5000 #in num of files

#server setting
path = '/users/jimmy/data/json2015/*'
resultPath = '/volumes/ext/jimmy/data/'
bulk_size = 5000 #in num of files

if not os.path.exists(resultPath):
    os.makedirs(resultPath)

startTime = time.time()
files = glob.glob(path)
i = 0.0

fw = open(resultPath + "fieldLength_clef2015.txt", 'w')
fw.write("FileName" + " " + "TitleWords" + " " + "TitleChars" + " " + "MetaWords" + " " + "MetaChars" + " " +
         "HeadersWords" + " " + "HeadersChars" + " " + "BodyWords" + " " + "BodyChars" + "\n")

try:
    for f in files:
        with open(f, 'r') as fr:
            data = json.load(fr)
            fr.close()
        docId = os.path.basename(f)

        title = ""
        if data["title"]:
            title = data["title"]

        meta = ""
        if data["meta"]:
            meta = data["meta"]

        headers = ""
        if data["headers"]:
            headers = data["headers"]

        body = ""
        if data["body"]:
            body = data["body"]

        fw.write(docId + " " + str(len(title.split())) + " " + str(len(title)) + " " + str(len(meta.split())) + " " + str(len(meta)) + " " +
                 str(len(headers.split())) + " " + str(len(headers)) + " " + str(len(body.split())) + " " + str(len(body)) + "\n")

        i += 1
        if i % bulk_size == 0:
            print ("{0} Completed".format(str(i)))


except Exception as e:
    print("unexpected error:", docId, e)


