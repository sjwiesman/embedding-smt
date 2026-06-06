package com.materialize.connect.smt.embedding;

/** Transient embedding failure (timeout, 429, 5xx) — worth retrying. */
public class RetriableEmbeddingException extends RuntimeException {
  public RetriableEmbeddingException(String message) {
    super(message);
  }

  public RetriableEmbeddingException(String message, Throwable cause) {
    super(message, cause);
  }
}
