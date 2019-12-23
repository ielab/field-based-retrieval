from elasticsearch import Elasticsearch

#local setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=300)

#server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=600)

ops = "close"

titleWeights = [0, 1, 3, 5]
metaWeights = [0, 1, 3, 5]
headersWeights = [0, 1, 3, 5] # note: by default: the body will contain header once
bodyWeights = [0, 1, 3, 5]

docType = "clef"

i = 0.0
errorCount=0

for mw in titleWeights:
    for hw in metaWeights:
        for tw in headersWeights:
            for bw in bodyWeights:
                if tw + mw + hw + bw >= 1:
                    # Index Setting
                    # indexName format: clef2015_aabbccdd. aa=title weight, bb=meta weight, cc=headers weight, dd=body weight
                    indexName = "clef2015_" + format(tw, '02') + format(mw, '02') + format(hw, '02') + format(bw, '02')

                    # Open/close if exist
                    if es.indices.exists(indexName):
                        if ops == "close":
                            print("closing '%s' index..." % indexName)
                            res = es.indices.close(index=indexName)

                            print(" response: '%s'" % res)
                        elif ops == "open":
                            print("opening '%s' index..." % indexName)
                            res = es.indices.open(index=indexName)
                            print(" response: '%s'" % res)





