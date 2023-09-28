from elasticsearch import Elasticsearch 
from utils import *

es = Elasticsearch("http://localhost:9200")

es.info().body 

if (__name__ == "__main__"):

    # mappings = {
    #     "properties" : {
    #         "name" : { "type" : "text", "analyzer" : "standard" },
    #         "age" : { "type" : "integer" }
    #     }
    # }

    # create_index(es, index_name = "students", mappings = mappings)

    insert_document(es, index_name = "students", document = {
        "name" : "Kevin",
        "age" : 22
    })

    insert_document(es, index_name = "students", document = {
        "name" : "Devin", 
        "age" : 16
    })

    print(es.search(index = "students", query = { "match_all" : {} }))