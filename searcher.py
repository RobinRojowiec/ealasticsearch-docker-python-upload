from elasticsearch import Elasticsearch


class Highlight:
    def __init__(self, field: str):
        self.field: str = field
        self.snippets = []

    def add_snippet(self, text: str):
        self.snippets.append(text)


class SearchResult:
    def __init__(self, title):
        self.score: float = 0.0
        self.title: str = title
        self.text: str = ""
        self.highlights: [] = []

    def add_highlight(self, highlight: Highlight):
        self.highlights.append(highlight)


class Searcher:
    def __init__(self):
        self.client: Elasticsearch = None
        self.index_name: str = "wikipedia_pages"
        self.doc_type: str = "Wikipage"
        self.query_template = {
            "query": {
                "simple_query_string": {
                    "fields": [
                        "title"
                    ],
                    "query": "Web crawler",
                    "default_operator": "or"
                }
            },
            "highlight": {
                "order": "score",
                "fields": {
                    "title": {
                        "type": "unified"
                    }
                }
            }
        }

    def connect(self):
        self.client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        self.client.ping()

    def search(self, query_string: str):
        query: dict = self.query_template.copy()
        query['query']['simple_query_string']['query'] = query_string

        raw_results = self.client.search(self.index_name, self.doc_type, query, size=10)
        search_results = []

        for hit in raw_results["hits"]["hits"]:
            result = SearchResult(hit['_source']['title'])
            result.score = hit["_score"]

            for field in hit['highlight']:
                highligth = Highlight(field)
                highligth.snippets = hit['highlight'][field]
                result.add_highlight(highligth)

            search_results.append(result)

        return search_results
