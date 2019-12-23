def query_builder_normal(query_terms, alpha, tie_breaker):
    fields = list()
    fields.append("title^" + str(alpha))
    fields.append("body^" + str(1 - alpha))

    query_string = {
        "from": 0, "size": 1000,
        "_source": False,
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
