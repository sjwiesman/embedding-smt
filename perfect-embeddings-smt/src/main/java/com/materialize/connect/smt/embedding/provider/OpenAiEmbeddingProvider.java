package com.materialize.connect.smt.embedding.provider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.materialize.embedding.spi.EmbeddingProvider;
import com.materialize.embedding.spi.FatalEmbeddingException;
import com.materialize.embedding.spi.RetriableEmbeddingException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;

/**
 * The built-in {@link EmbeddingProvider} backed by OpenAI's embeddings REST API (or any
 * OpenAI-compatible endpoint, such as a proxy, gateway, or Azure deployment). This is the default
 * provider shipped with the plugin and the one selected when {@code provider=openai}.
 */
public final class OpenAiEmbeddingProvider implements EmbeddingProvider {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String apiKey;
  private String model;
  private String endpoint;
  private Integer dimensions;
  private HttpClient httpClient;
  private Duration requestTimeout;

  @Override
  public String name() {
    return "openai";
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.apiKey = str(configs, "openai.api.key", null);
    if (apiKey == null || apiKey.isBlank()) {
      throw new ConfigException("openai.api.key", apiKey, "OpenAI API key must be configured");
    }
    this.model = str(configs, "openai.model", "text-embedding-3-small");
    this.endpoint = str(configs, "openai.endpoint", "https://api.openai.com/v1/embeddings");
    String dims = str(configs, "openai.dimensions", null);
    this.dimensions = (dims == null || dims.isBlank()) ? null : Integer.valueOf(dims.trim());
    long timeoutMs = Long.parseLong(str(configs, "request.timeout.ms", "30000"));
    this.requestTimeout = Duration.ofMillis(timeoutMs);
    this.httpClient = HttpClient.newBuilder().connectTimeout(requestTimeout).build();
  }

  @Override
  public List<Float> embed(String text) {
    String requestBody = buildRequestBody(text);
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(endpoint))
            .timeout(requestTimeout)
            .header("Content-Type", "application/json")
            .header("Authorization", "Bearer " + apiKey)
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();

    HttpResponse<String> response;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException e) {
      throw new RetriableEmbeddingException("I/O error calling embedding service", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RetriableEmbeddingException("Interrupted calling embedding service", e);
    }

    int status = response.statusCode();
    if (status == 429 || status >= 500) {
      throw new RetriableEmbeddingException("Embedding service returned HTTP " + status);
    }
    if (status >= 400) {
      throw new FatalEmbeddingException(
          "Embedding service returned HTTP " + status + ": " + response.body());
    }
    return parseEmbedding(response.body());
  }

  private String buildRequestBody(String text) {
    ObjectNode root = MAPPER.createObjectNode();
    root.put("model", model);
    root.put("input", text);
    if (dimensions != null) {
      root.put("dimensions", dimensions);
    }
    try {
      return MAPPER.writeValueAsString(root);
    } catch (IOException e) {
      throw new FatalEmbeddingException("Failed to serialize embedding request", e);
    }
  }

  private List<Float> parseEmbedding(String body) {
    try {
      JsonNode embedding = MAPPER.readTree(body).path("data").path(0).path("embedding");
      if (!embedding.isArray()) {
        throw new FatalEmbeddingException("Embedding response missing data[0].embedding: " + body);
      }
      List<Float> vector = new ArrayList<>(embedding.size());
      for (JsonNode element : embedding) {
        vector.add(element.floatValue());
      }
      return vector;
    } catch (IOException e) {
      throw new FatalEmbeddingException("Failed to parse embedding response", e);
    }
  }

  private static String str(Map<String, ?> configs, String key, String defaultValue) {
    Object value = configs.get(key);
    return value == null ? defaultValue : value.toString();
  }
}
