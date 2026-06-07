package com.materialize.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

/**
 * In-JVM OpenAI-compatible embeddings endpoint. Replaces the Python mock used by the old Docker
 * Compose example. The embedding vector is deterministic and depends only on the input text so the
 * integration test can assert exact values.
 */
final class MockEmbeddingsServer implements AutoCloseable {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final MockWebServer server = new MockWebServer();

  MockEmbeddingsServer() {
    server.setDispatcher(
        new Dispatcher() {
          @Override
          public MockResponse dispatch(RecordedRequest request) {
            if (!"/v1/embeddings".equals(request.getPath())) {
              return new MockResponse().setResponseCode(404);
            }
            try {
              JsonNode payload = MAPPER.readTree(request.getBody().readUtf8());
              double[] embedding = embeddingFor(payload.get("input").asText());

              ObjectNode root = MAPPER.createObjectNode();
              ArrayNode data = root.putArray("data");
              ObjectNode item = data.addObject();
              ArrayNode vector = item.putArray("embedding");
              for (double value : embedding) {
                vector.add(value);
              }
              item.put("index", 0);
              item.put("object", "embedding");
              root.put(
                  "model",
                  payload.hasNonNull("model")
                      ? payload.get("model").asText()
                      : "mock-embedding-model");
              root.put("object", "list");

              return new MockResponse()
                  .setHeader("Content-Type", "application/json")
                  .setBody(MAPPER.writeValueAsString(root));
            } catch (IOException e) {
              return new MockResponse().setResponseCode(500).setBody(e.getMessage());
            }
          }
        });
  }

  /** Mirrors the previous Python mock: [length, sum(ord(c)) mod 997, vowel count]. */
  static double[] embeddingFor(String text) {
    int length = text.length();
    int ordSum = 0;
    int vowels = 0;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      ordSum += c;
      char lower = Character.toLowerCase(c);
      if (lower == 'a' || lower == 'e' || lower == 'i' || lower == 'o' || lower == 'u') {
        vowels++;
      }
    }
    return new double[] {length, ordSum % 997, vowels};
  }

  void start() throws IOException {
    server.start();
  }

  int port() {
    return server.getPort();
  }

  @Override
  public void close() throws IOException {
    server.shutdown();
  }
}
