import json
from time import sleep

from elasticsearch import Elasticsearch, TransportError

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

INDEX_NAME = "wikipedia_pages"
DOC_TYPE = "Wikipage"

if es.indices.exists(INDEX_NAME):
    es.indices.delete(index=INDEX_NAME)

if not es.indices.exists(INDEX_NAME):
    es.indices.create(index=INDEX_NAME)

    with open("data/mapping.json", "r") as file:
        es.indices.put_mapping(doc_type=DOC_TYPE, body=json.load(file), index=INDEX_NAME)

    with open("data/web_crawler.json", "r") as file:
        json_object: dict = json.load(file)[0]
        try:
            es.index(index=INDEX_NAME, body=json_object, doc_type=DOC_TYPE)
        except TransportError as e:
            print(e.info)

    sleep(1)

with open("data/simple_query.json", "r") as query_file:
    query = json.load(query_file)
    results = es.search(INDEX_NAME, DOC_TYPE, query)

print('Title: %s' % (results["hits"]["hits"][0]['_source']['title']))
