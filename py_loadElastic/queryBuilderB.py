def b_query_builder(query_terms, title_weight, meta_weight, headers_weight, body_weight, tie_breaker):
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
        "from": 0, "size": 1000,
        "query": {
            "query_string": {
                "query": query_terms,
                "fields": fields,
                "use_dis_max": True,
                "tie_breaker": tie_breaker
            }
        }
    }
    return query_string

'''
query_string = {
 "from": 0, "size": 1000,
 "query": {
     "multi_match": {
         "query": query_terms,
         "type": "best_fields",
         "fields": ["title^" + str(title_weight), "meta^" + str(meta_weight), "headers^" + str(headers_weight),
                    "body^" + str(body_weight)],
         "tie_breaker": 0
     }
 }
}'''
