package com.materialize.connect.smt.embedding;

import java.util.List;
import java.util.Map;

/**
 * Pluggable embedding backend, discovered via {@link java.util.ServiceLoader}. The implementation
 * whose {@link #name()} matches the {@code provider} config is selected.
 */
public interface EmbeddingProvider extends AutoCloseable {

  /** Identifier matched against the {@code provider} config value (e.g. "openai"). */
  String name();

  /** Receives the connector's raw config map; reads its own provider-specific keys. */
  void configure(Map<String, ?> configs);

  /**
   * Returns the embedding vector for the given text. Throws {@link RetriableEmbeddingException} for
   * transient failures and {@link FatalEmbeddingException} for permanent ones.
   */
  List<Float> embed(String text);

  @Override
  default void close() {}
}
