package com.materialize.example;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class MockEmbeddingsServerTest {

  @Test
  void embeddingMatchesPythonMockForKnownInputs() {
    assertThat(MockEmbeddingsServer.embeddingFor("Hello world")).containsExactly(11.0, 87.0, 3.0);
    assertThat(MockEmbeddingsServer.embeddingFor("First body text"))
        .containsExactly(15.0, 470.0, 3.0);
    assertThat(MockEmbeddingsServer.embeddingFor("First body text updated"))
        .containsExactly(23.0, 248.0, 6.0);
  }
}
