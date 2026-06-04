package com.materialize.connect.smt.embedding;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

import java.util.List;

/** Retries transient embedding failures with exponential backoff, then fails fast. */
public final class RetryingEmbeddingClient {

    /** Backoff seam so tests can avoid real sleeping. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final EmbeddingProvider provider;
    private final int maxRetries;
    private final long backoffMs;
    private final Sleeper sleeper;

    public RetryingEmbeddingClient(EmbeddingProvider provider, int maxRetries, long backoffMs, Sleeper sleeper) {
        this.provider = provider;
        this.maxRetries = maxRetries;
        this.backoffMs = backoffMs;
        this.sleeper = sleeper;
    }

    public List<Float> embed(String text) {
        int attempt = 0;
        while (true) {
            try {
                return provider.embed(text);
            } catch (FatalEmbeddingException e) {
                throw new ConnectException("Embedding request failed permanently", e);
            } catch (RetriableEmbeddingException e) {
                if (attempt >= maxRetries) {
                    throw new RetriableException(
                            "Embedding request failed after " + attempt + " retries", e);
                }
                backoff(attempt);
                attempt++;
            }
        }
    }

    private void backoff(int attempt) {
        long shift = Math.min(attempt, 30); // cap to avoid 1L << attempt overflowing
        long delay = backoffMs * (1L << shift); // exponential: base * 2^shift
        try {
            sleeper.sleep(delay);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted during embedding backoff", ie);
        }
    }
}
