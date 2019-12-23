def query_builder_aquaint(query_terms, title_weight, body_weight, tie_breaker):
    fields = []

    if title_weight > 0:
        fields.append("title^" + str(title_weight))

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