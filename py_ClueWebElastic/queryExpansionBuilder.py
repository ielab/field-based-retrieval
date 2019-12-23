from collections import Counter

def build_query_expansion(expanded_query_terms, title_weight, meta_weight, headers_weight, body_weight, tie_breaker, page, resultPerPage):

    #configure the term boosting
    queryTerms = ""
    for term, count in Counter(expanded_query_terms).most_common():
        queryTerms += "\"" + term + "\"^" + str(count) + " "
        #queryTerms += "\"" + term + "\"^1 "
    # configure the field boosting
    fields = []
    if title_weight > 0:
        fields.append("title^" + str(title_weight))

    if meta_weight > 0:
        fields.append("meta^" + str(meta_weight))

    if headers_weight > 0:
        fields.append("headers^" + str(headers_weight))

    if body_weight > 0:
        fields.append("body^" + str(body_weight))

    query_string = {
        "from": (page-1)*resultPerPage, "size": resultPerPage,
        "_source": False,
        "query": {
            "query_string": {
                "query": str(queryTerms),
                "fields": fields,
                "use_dis_max": True,
                "tie_breaker": tie_breaker
            }
        }
    }

    return query_string
