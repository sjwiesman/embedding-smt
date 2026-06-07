package com.materialize.embedding.spi;

/**
 * Signals a transient embedding failure that may succeed if tried again, such as a timeout or an
 * HTTP 429 / 5xx response. Raised by {@link EmbeddingProvider} implementations to mark a call as
 * eligible for retry by the consumer.
 */
public class RetriableEmbeddingException extends RuntimeException {
  public RetriableEmbeddingException(String message) {
    super(message);
  }

  public RetriableEmbeddingException(String message, Throwable cause) {
    super(message, cause);
  }
}
