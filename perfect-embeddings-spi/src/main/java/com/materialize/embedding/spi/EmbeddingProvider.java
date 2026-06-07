package com.materialize.embedding.spi;

import java.util.List;
import java.util.Map;

/**
 * The extension point for embedding backends: a service that turns a piece of text into an
 * embedding vector. Implementations are discovered at runtime via {@link java.util.ServiceLoader}
 * and selected by matching {@link #name()} against the consumer's {@code provider} configuration.
 * Additional backends are added by registering new implementations on the classpath.
 */
public interface EmbeddingProvider extends AutoCloseable {

  /** Identifier matched against the {@code provider} config value (e.g. "openai"). */
  String name();

  /** Receives the consumer's raw config map; reads its own provider-specific keys. */
  void configure(Map<String, ?> configs);

  /**
   * Returns the embedding vector for the given text. Throws {@link RetriableEmbeddingException} for
   * transient failures and {@link FatalEmbeddingException} for permanent ones.
   */
  List<Float> embed(String text);

  @Override
  default void close() {}
}
