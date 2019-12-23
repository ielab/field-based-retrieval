from elasticsearch import Elasticsearch

#local setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=300)

#server setting
#es = Elasticsearch(urls='http://localhost', port=9200, timeout=600)

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
                        res = es.indices.open(index=indexName)
                        es.cluster.health(index=indexName, wait_for_status='green') # wait until index is ready
                        print indexName + " " + str(es.indices.stats(index=indexName, metric='docs')["indices"][indexName]["total"]["docs"]["count"])

                        res = es.indices.close(index=indexName)
                    else:
                        print indexName + " not found"
