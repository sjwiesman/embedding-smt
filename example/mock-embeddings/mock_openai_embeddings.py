from http.server import BaseHTTPRequestHandler, HTTPServer
import json


def embedding_for(text: str) -> list[float]:
    length = float(len(text))
    checksum = float(sum(ord(ch) for ch in text) % 997)
    vowels = float(sum(1 for ch in text.lower() if ch in "aeiou"))
    return [length, checksum, vowels]


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        if self.path != "/v1/embeddings":
            self.send_response(404)
            self.end_headers()
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(content_length).decode("utf-8"))
        text = payload["input"]
        response = {
            "data": [
                {
                    "embedding": embedding_for(text),
                    "index": 0,
                    "object": "embedding"
                }
            ],
            "model": payload.get("model", "mock-embedding-model"),
            "object": "list"
        }

        body = json.dumps(response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        return


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
