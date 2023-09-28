from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from typing import List, Dict, Any

def create_index(es : Elasticsearch, index_name : str, mappings : dict, **kwargs) -> None:

    assert isinstance(es, Elasticsearch)

    assert isinstance(index_name, str) 

    assert isinstance(mappings, dict)

    es.indices.create(index = index_name, mappings = mappings, **kwargs)

def insert_document(es : Elasticsearch, index_name : str, document : dict, **kwargs) -> None:

    assert isinstance(es, Elasticsearch)

    assert isinstance(index_name, str) 

    assert isinstance(document, dict)

    es.index(index = index_name, document = document, **kwargs)

def insert_documents(es : Elasticsearch, index_name : str, documents : list, **kwargs) -> None:

    assert isinstance(es, Elasticsearch)

    assert isinstance(index_name, str) 

    assert isinstance(documents, list)

    bulk(es, documents, **kwargs)

def search_documents(es : Elasticsearch, index_name : str, query_dict : dict, **kwargs) -> Any:

    assert isinstance(es, Elasticsearch)

    assert isinstance(index_name, str) 

    assert isinstance(query_dict, dict)

    return es.search(index = index_name, query = query_dict, **kwargs)

def count_documents(es : Elasticsearch, index_name : str, query_dict : dict, **kwargs) -> int:

    assert isinstance(es, Elasticsearch)

    assert isinstance(index_name, str) 

    assert isinstance(query_dict, dict)

    return int(es.count(index = index_name, body = { "query" : query_dict }, **kwargs)["count"])

def delete_index(es : Elasticsearch, index_name : str, **kwargs) -> None:

    assert isinstance(es, Elasticsearch)

    assert isinstance(index_name, str) 

    es.indices.delete(index = index_name, **kwargs)
