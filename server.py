import json

from elasticsearch import Elasticsearch, TransportError

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

INDEX_NAME = "wikipedia_pages"
DOC_TYPE = "Wikipage"

# ignore 404 and 400
if es.indices.exists(INDEX_NAME):
    es.indices.delete(index=INDEX_NAME, ignore=[400, 404])

# ignore 400 cause by IndexAlreadyExistsException when creating an index
if not es.indices.exists(INDEX_NAME):
    es.indices.create(index=INDEX_NAME, ignore=400)

    with open("data/mapping.json", "r") as file:
        es.indices.put_mapping(DOC_TYPE, json.load(file), index=INDEX_NAME)

    with open("data/sample_document.json", "r") as file:
        try:
            es.index(INDEX_NAME, DOC_TYPE, json.load(file))
        except TransportError as e:
            print(e.info)


with open("data/query.json", "r") as query_file:
    query = json.load(query_file)
    results = es.search(INDEX_NAME, DOC_TYPE, query)

print(results)
print(results["hits"]["hits"][0]["highlight"])
