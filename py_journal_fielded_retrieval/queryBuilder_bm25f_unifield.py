# Filtered with should (OR) to consider only documents containing at least one of the query term in either title or body
def bm25f_query_builder_unifield(query_terms, size, k_1, b_titlebody, avg_len_titlebody, df):
    script = "score=0; N=_index.numDocs(); "
    filter_list = list()
    for term in query_terms.split():
        script = "{s} n_i={df_term}; " \
                  "IDF = Math.log(1 + ((N - n_i + 0.5) / (n_i + 0.5))); " \
                  "tf_titlebody = _index['titlebody']['{oriTerm}'].tf(); " \
                  "Bs_titlebody = ((1 - b_titlebody) + b_titlebody * (doc['titlebody.length'].value / avglen_titlebody)); " \
                  "tfi = w_titlebody * (tf_titlebody / Bs_titlebody); " \
                  "score = score + ((tfi / (k_1 + tfi)) * IDF); "\
            .format(s=script, oriTerm=term, df_term=df[term])
        filter_list.append({"term": {"titlebody": term}})
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
                                    "b_titlebody": b_titlebody,
                                    "w_titlebody": 1,
                                    "avglen_titlebody": avg_len_titlebody
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
