from elasticsearch import Elasticsearch
import operator

es = Elasticsearch(urls='http://localhost', port=9200, timeout=100000)
docType = "wiki"
indexName = "wikipedia"

res = es.count(index=indexName,doc_type=docType)
docCount = int(res['count'])
print docCount

numTermsUsed = 10

request_body = {
   "fields" : ["body"],
  "offsets" : False,
  "positions" : False,
  "term_statistics" : True,
  "field_statistics" : False
}


res = es.termvectors(index=indexName, doc_type=docType, id="25", body=request_body)

terms = {}
for term in res['term_vectors']['body']['terms']:
    term_frequency = res['term_vectors']['body']['terms'][term]['term_freq']
    doc_frequency = res['term_vectors']['body']['terms'][term]['doc_freq']
    terms[term] = term_frequency * (docCount/doc_frequency)
    #print term + " term_freq : " + str(term_frequency) + " doc_freq : " + str(doc_frequency) + " tfidf : " + str(terms[term])


terms = sorted(terms.items(), key=operator.itemgetter(1), reverse=True)

for term in terms:
    print term


