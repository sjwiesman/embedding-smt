package com.materialize.connect.smt.embedding;

/**
 * Signals a permanent embedding failure — one that retrying cannot fix, such as a malformed request
 * or an authentication error. Raised by {@link EmbeddingProvider} implementations to tell the
 * {@link RetryingEmbeddingClient} to give up immediately rather than retry.
 */
public class FatalEmbeddingException extends RuntimeException {
  public FatalEmbeddingException(String message) {
    super(message);
  }

  public FatalEmbeddingException(String message, Throwable cause) {
    super(message, cause);
  }
}
