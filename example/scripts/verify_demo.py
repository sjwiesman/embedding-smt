import json
import time
import urllib.error
import urllib.request


ES_DOC_URL = "http://elasticsearch:9200/articles-cdc/_doc/1"
CONNECT_STATUS_URL = "http://connect:8083/connectors/articles-elasticsearch-sink/status"


def get_json(url: str):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def wait_for(predicate, description: str, timeout: int = 180, interval: float = 2.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            value = predicate()
            if value:
                print(f"verified: {description}")
                return value
        except Exception:
            pass
        time.sleep(interval)
    raise RuntimeError(f"timed out waiting for {description}")


def connector_running():
    status = get_json(CONNECT_STATUS_URL)
    return (
        status["connector"]["state"] == "RUNNING"
        and all(task["state"] == "RUNNING" for task in status.get("tasks", []))
    )


def fetch_doc():
    try:
        body = get_json(ES_DOC_URL)
    except urllib.error.HTTPError as err:
        if err.code == 404:
            return None
        raise
    return body.get("_source")


insert_doc = wait_for(connector_running, "connector running")

insert_doc = wait_for(
    lambda: (
        (doc := fetch_doc())
        and doc.get("title") == "Hello world"
        and doc.get("body") == "First body text"
        and doc.get("views") == 10
        and doc.get("title_embedding") == [11.0, 87.0, 3.0]
        and doc.get("body_embedding") == [15.0, 470.0, 3.0]
        and doc
    ),
    "initial article indexed with embeddings",
)

title_embedding = insert_doc["title_embedding"]
body_embedding = insert_doc["body_embedding"]

wait_for(
    lambda: (
        (doc := fetch_doc())
        and doc.get("views") == 42
        and doc.get("title_embedding") == title_embedding
        and doc.get("body_embedding") == body_embedding
    ),
    "views-only update preserves embeddings",
)

wait_for(
    lambda: (
        (doc := fetch_doc())
        and doc.get("body") == "First body text updated"
        and doc.get("body_embedding") == [23.0, 248.0, 6.0]
        and doc.get("title_embedding") == title_embedding
    ),
    "body update refreshes only the body embedding",
)

print("End-to-end example verified successfully.")
