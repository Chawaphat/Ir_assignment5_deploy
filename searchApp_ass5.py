from flask import Flask, request, jsonify , render_template
from elasticsearch import Elasticsearch
import time
import os
from flask_cors import CORS
import re

app = Flask(__name__)
CORS(app)
es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "NN6j6HonKzcnAiybILi0"),
    ca_certs=os.path.expanduser("~/http_ca.crt")
)

@app.route("/")
def home():
    return render_template("index.html")


@app.route("/search_es_pr")
def search_es_pr():
    query = request.args.get("query")
    start = time.time()

    results = es.search(
        index="simple",
        size=100,
        query={
            "script_score": {
                "query": {"match": {"text": query}},
                "script": {
                    "source": "_score * doc['pagerank'].value"
                }
            }
        }
    )

    end = time.time()
    
    return jsonify({
    "total_hit": results["hits"]["total"]["value"],
    "elapse": round(end - start, 3),
    "results": [
        {
            "title": hit["_source"]["title"],
            "url": hit["_source"]["url"],
            "text": make_snippet(hit["_source"]["text"], query),
            "score": hit["_score"]
        }
        for hit in results["hits"]["hits"]
    ]
    })


@app.route("/search_custom")
def search_custom():
    query = request.args.get("query")
    start = time.time()

    results = es.search(
        index="custom",
        size=100,
        query={
            "script_score": {
                "query": {"match": {"text": query}},
                "script": {
                    "source": "_score * doc['pagerank'].value"
                }
            }
        }
    )

    end = time.time()

    return jsonify({
    "total_hit": results["hits"]["total"]["value"],
    "elapse": round(end - start, 3),
    "results": [
        {
            "title": hit["_source"]["title"],
            "url": hit["_source"]["url"],
            "text": make_snippet(hit["_source"]["text"], query),
            "score": hit["_score"]
        }
        for hit in results["hits"]["hits"]
    ]
    })


def make_snippet(text, query, window=120):
    text_lower = text.lower()
    query_lower = query.lower()

    idx = text_lower.find(query_lower)

    if idx == -1:
        return text[:200]  

    start = max(idx - window, 0)
    end = min(idx + len(query) + window, len(text))

    snippet = text[start:end]

    
    snippet = re.sub(
        f"({re.escape(query)})",
        r"<b>\1</b>",
        snippet,
        flags=re.IGNORECASE
    )

    return "..." + snippet + "..."



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)