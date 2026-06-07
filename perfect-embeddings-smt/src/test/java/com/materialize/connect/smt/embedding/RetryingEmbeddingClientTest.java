package com.materialize.connect.smt.embedding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.materialize.embedding.spi.EmbeddingProvider;
import com.materialize.embedding.spi.FatalEmbeddingException;
import com.materialize.embedding.spi.RetriableEmbeddingException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.Test;

class RetryingEmbeddingClientTest {

  /** Provider whose embed() behavior is driven by a supplied function of attempt-count. */
  private static final class ScriptedProvider implements EmbeddingProvider {
    final AtomicInteger calls = new AtomicInteger();
    final java.util.function.IntFunction<List<Float>> script;

    ScriptedProvider(java.util.function.IntFunction<List<Float>> script) {
      this.script = script;
    }

    public String name() {
      return "scripted";
    }

    public void configure(Map<String, ?> configs) {}

    public List<Float> embed(String text) {
      return script.apply(calls.incrementAndGet());
    }
  }

  private static RetryingEmbeddingClient client(EmbeddingProvider p, int maxRetries) {
    // no-op sleeper: backoff does not actually pause the test
    return new RetryingEmbeddingClient(p, maxRetries, 1L, ms -> {});
  }

  @Test
  void returnsResultOnFirstSuccess() {
    ScriptedProvider p = new ScriptedProvider(attempt -> List.of(0.1f, 0.2f));
    assertThat(client(p, 3).embed("hi")).containsExactly(0.1f, 0.2f);
    assertThat(p.calls).hasValue(1);
  }

  @Test
  void retriesThenSucceeds() {
    ScriptedProvider p =
        new ScriptedProvider(
            attempt -> {
              if (attempt < 3) throw new RetriableEmbeddingException("429");
              return List.of(1.0f);
            });
    assertThat(client(p, 5).embed("hi")).containsExactly(1.0f);
    assertThat(p.calls).hasValue(3);
  }

  @Test
  void exhaustedRetriesThrowConnectRetriable() {
    ScriptedProvider p =
        new ScriptedProvider(
            attempt -> {
              throw new RetriableEmbeddingException("503");
            });
    assertThatThrownBy(() -> client(p, 2).embed("hi")).isInstanceOf(RetriableException.class);
    assertThat(p.calls).hasValue(3); // initial try + 2 retries
  }

  @Test
  void fatalFailsImmediatelyAsConnectException() {
    ScriptedProvider p =
        new ScriptedProvider(
            attempt -> {
              throw new FatalEmbeddingException("401");
            });
    assertThatThrownBy(() -> client(p, 5).embed("hi"))
        .isInstanceOf(ConnectException.class)
        .isNotInstanceOf(RetriableException.class);
    assertThat(p.calls).hasValue(1); // no retries
  }
}
