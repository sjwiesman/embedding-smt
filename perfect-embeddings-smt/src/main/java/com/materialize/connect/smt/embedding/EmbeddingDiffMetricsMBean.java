package com.materialize.connect.smt.embedding;

/**
 * JMX view of the embedding-diff savings. Registered on the platform MBeanServer so the value of
 * skipping unchanged embeddings is visible to any JMX scraper (Prometheus jmx_exporter, JConsole).
 *
 * <p>The baseline is a naive pipeline that re-embeds every configured embedded column on every
 * record that has an {@code after} (insert/update). {@code EmbeddingsPossible} counts that
 * baseline; {@code EmbeddingsComputed} counts the calls actually made; {@code EmbeddingsSkipped} is
 * the difference — the calls this SMT avoided.
 */
public interface EmbeddingDiffMetricsMBean {

  /** Embedding API calls actually made. */
  long getEmbeddingsComputed();

  /** Embedding API calls avoided (dropped records, unchanged columns, changed-to-null columns). */
  long getEmbeddingsSkipped();

  /** Calls a naive re-embed-everything pipeline would have made ({@code computed + skipped}). */
  long getEmbeddingsPossible();

  /** Fraction of possible calls that were skipped ({@code skipped / possible}); 0.0 when idle. */
  double getSkipRatio();
}
