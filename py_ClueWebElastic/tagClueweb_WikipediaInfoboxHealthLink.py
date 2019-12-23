# This project is to tag clueweb12 documents that the title contain title of aliases from Wikipedia
# Wikipedia collection that is built based on infobox health type and health links

from elasticsearch import Elasticsearch
import multiprocessing
import re

# server setting
es = Elasticsearch(urls='http://localhost', port=9200, timeout=5000)
healthIdFile = "/volumes/ext/jimmy/experiments/knowledgeGraph/clueweb12Contain_wikiInfoboxHealthLinks_Rank1000"

indexName = "clueweb12b_all"
docType = "clueweb"


#load titles and aliases the load as one list: wikiTerms
fr = open(healthIdFile, 'r')
docs = fr.readlines()
docs = [line.strip() for line in docs]
fr.close()


def tagClueweb(docId):
    updateBody = {"doc": {"is_health": 1}}
    resU = es.update(index=indexName, doc_type=docType, id=docId, body=updateBody, refresh=True)

for doc in docs:
