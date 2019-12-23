# Filtered with should (OR) to consider only documents containing at least one of the query term in either title or body
def bm25f_query_builder(query_terms, size, k_1, alpha, b_title, b_body, avg_len_title, avg_len_body, df):
    w_title = alpha
    w_body = 1-alpha

    script = "score=0; N=_index.numDocs(); "
    filter_list = list()
    for term in query_terms.split():
        script = "{s} n_i={df_term}; " \
                  "IDF = Math.log(1 + ((N - n_i + 0.5) / (n_i + 0.5))); " \
                  "tf_title = _index['title']['{oriTerm}'].tf(); " \
                  "tf_body = _index['body']['{oriTerm}'].tf(); " \
                  "Bs_title = ((1 - b_title) + b_title * (doc['title.length'].value / avglen_title)); " \
                  "Bs_body = ((1 - b_body) + b_body * (doc['body.length'].value / avglen_body)); " \
                  "tfi = w_title * (tf_title / Bs_title) + w_body * (tf_body / Bs_body); " \
                  "score = score + ((tfi / (k_1 + tfi)) * IDF); "\
            .format(s=script, oriTerm=term, df_term=df[term])
        filter_list.append({"term": {"title":term}})
        filter_list.append({"term": {"body": term}})
    script = "{} return score".format(script)

    query_string = {
        "from": 0, "size": size,
        "_source": False,
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "should": filter_list
                    }
                },
                "functions": [
                    {
                        "script_score": {
                            "script": {
                                "lang": "groovy",
                                "params": {
                                    "k_1": k_1,
                                    "b_title": b_title,
                                    "b_body": b_body,
                                    "w_title": w_title,
                                    "w_body": w_body,
                                    "avglen_title": avg_len_title,
                                    "avglen_body": avg_len_body
                                },
                                "inline": script
                            }
                        }
                    }
                ]
            }
        }
    }

    return query_string
