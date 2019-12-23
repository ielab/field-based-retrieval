def query_builder(query_terms):
    query_string = {
        "from": 0, "size": 1000,
        "query": {
            "query_string": {
                "query": query_terms,
                "fields": ["content"]
            }
        }
    }
    return query_string