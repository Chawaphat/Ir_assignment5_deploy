import os
import json
import pickle
from pathlib import Path
from elasticsearch import Elasticsearch
from pageRank import Pr


es_client = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "NN6j6HonKzcnAiybILi0"),
    ca_certs=os.path.expanduser("~/http_ca.crt")
)


class SimpleIndexer:

    def __init__(self):
        self.crawled_folder = Path(os.path.abspath("")).parent / "crawled"

        with open(self.crawled_folder / "url_list.pickle", "rb") as f:
            self.file_mapper = pickle.load(f)

        self.es_client = es_client

        self.pr = Pr(alpha=0.85)

    def run_indexer(self):

        self.pr.pr_calc()
        self.es_client.options(ignore_status=[400,404]).indices.delete(index="simple")

        body = {
            "mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "url": {"type": "keyword"},
                    "text": {"type": "text"},
                    "pagerank": {"type": "float"}
                }
            }
        }

        self.es_client.options(ignore_status=400).indices.create(
            index="simple",
            body=body
        )

        
        for file in os.listdir(self.crawled_folder):
            if file.endswith(".txt"):

                j = json.load(open(os.path.join(self.crawled_folder, file)))
                j["id"] = j["url"]

                if j["url"] in self.pr.pr_result.index:
                    j["pagerank"] = float(self.pr.pr_result.loc[j["url"]].score)
                else:
                    j["pagerank"] = 0.0

                self.es_client.index(
                    index="simple",
                    body=j
                )
                
                
class CustomIndexer:

    def __init__(self):
        self.crawled_folder = Path(os.path.abspath("")).parent / "crawled"

        self.es_client = es_client
          
        self.pr = Pr(alpha=0.85)

    def run_indexer(self):

        self.pr.pr_calc()

        self.es_client.options(ignore_status=[400,404]).indices.delete(index="custom")

        body = {
            "settings": {
                "similarity": {
                    "tfidf_similarity": {
                        "type": "scripted",
                        "script": {
                            "source": """
                            double tf = Math.sqrt(doc.freq);
                            double idf = Math.log((field.docCount+1.0)/(term.docFreq+1.0)) + 1.0;
                            return tf * idf;
                            """
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "url": {"type": "keyword"},
                    "text": {
                        "type": "text",
                        "similarity": "tfidf_similarity"
                    },
                    "pagerank": {"type": "float"}
                }
            }
        }

        self.es_client.options(ignore_status=400).indices.create(
            index="custom",
            body=body
        )

        for file in os.listdir(self.crawled_folder):
            if file.endswith(".txt"):

                j = json.load(open(os.path.join(self.crawled_folder, file)))
                j["id"] = j["url"]

                if j["url"] in self.pr.pr_result.index:
                    j["pagerank"] = float(self.pr.pr_result.loc[j["url"]].score)
                else:
                    j["pagerank"] = 0.0

                self.es_client.index(
                    index="custom",
                    body=j
                )
                
                
if __name__ == "__main__":
    s = SimpleIndexer()
    s.run_indexer()

    c = CustomIndexer()
    c.run_indexer()