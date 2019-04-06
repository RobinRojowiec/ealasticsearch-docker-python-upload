import os

import jsonpickle
from flask import Flask, request
from werkzeug.exceptions import BadRequest

from searcher import Searcher

app = Flask(__name__)

QUERY_KEY = 'query'

searcher: Searcher = Searcher()
searcher.connect()


@app.route('/search', methods=['GET'])
def search():
    query_string: str = request.args.get(QUERY_KEY)
    if query_string:
        results = searcher.search(query_string)
        response = app.response_class(
            response=jsonpickle.encode(results, unpicklable=False),
            status=200,
            mimetype='application/json'
        )
        return response
    raise BadRequest("Query missing or incomplete!")


if __name__ == '__main__':
    port: int = os.getenv("PORT", 1200)
    app.run(port=port, host="0.0.0.0")
