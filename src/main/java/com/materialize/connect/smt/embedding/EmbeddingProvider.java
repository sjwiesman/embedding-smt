package com.materialize.connect.smt.embedding;

import java.util.List;
import java.util.Map;

/**
 * The plugin's extension point for embedding backends: a service that turns a piece of text into an
 * embedding vector. Implementations are discovered at runtime via {@link java.util.ServiceLoader}
 * and selected by matching {@link #name()} against the {@code provider} configuration.
 * <b>OpenAI</b> ships in the box ({@link
 * com.materialize.connect.smt.embedding.provider.OpenAiEmbeddingProvider}); additional backends can
 * be added by registering new implementations.
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
