import json

from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

INDEX_NAME = "wikipedia_pages"

# ignore 400 cause by IndexAlreadyExistsException when creating an index
if not es.indices.exists(INDEX_NAME):
    es.indices.create(index=INDEX_NAME, ignore=400)

    with open("data/test.json", "r") as file:
        es.index(INDEX_NAME, "paragraph", json.load(file))

with open("data/query.json", "r") as query_file:
    query = json.load(query_file)
    results = es.search(INDEX_NAME, "paragraph", query)

print(results)

# ignore 404 and 400
# es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
