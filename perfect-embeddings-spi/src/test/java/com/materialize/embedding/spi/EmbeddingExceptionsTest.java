package com.materialize.embedding.spi;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EmbeddingExceptionsTest {

  @Test
  void retriableCarriesMessageAndCause() {
    Throwable cause = new RuntimeException("boom");
    RetriableEmbeddingException e = new RetriableEmbeddingException("429", cause);
    assertThat(e).hasMessage("429").hasCause(cause);
    assertThat(new RetriableEmbeddingException("429")).hasMessage("429").hasNoCause();
  }

  @Test
  void fatalCarriesMessageAndCause() {
    Throwable cause = new RuntimeException("nope");
    FatalEmbeddingException e = new FatalEmbeddingException("401", cause);
    assertThat(e).hasMessage("401").hasCause(cause);
    assertThat(new FatalEmbeddingException("401")).hasMessage("401").hasNoCause();
  }
}
