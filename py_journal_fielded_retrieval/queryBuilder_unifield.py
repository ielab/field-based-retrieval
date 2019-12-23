def query_builder_unifield(query_terms):
    query_string = {
        "from": 0, "size": 1000,
        "_source": False,
        "query": {
            "query_string": {
                "query": query_terms,
                "fields": ["titlebody"]
            }
        }
    }
    return query_string
