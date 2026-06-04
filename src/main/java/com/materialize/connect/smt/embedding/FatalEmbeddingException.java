package com.materialize.connect.smt.embedding;

/** Permanent embedding failure (malformed request, auth) — not worth retrying. */
public class FatalEmbeddingException extends RuntimeException {
    public FatalEmbeddingException(String message) { super(message); }
    public FatalEmbeddingException(String message, Throwable cause) { super(message, cause); }
}
