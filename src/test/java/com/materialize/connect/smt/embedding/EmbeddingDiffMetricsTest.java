package com.materialize.connect.smt.embedding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class EmbeddingDiffMetricsTest {

  private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
  private EmbeddingDiffMetrics metrics;

  @AfterEach
  void tearDown() {
    if (metrics != null) {
      metrics.unregister();
    }
  }

  @Test
  void recordAccumulatesAndMaintainsInvariant() {
    metrics = new EmbeddingDiffMetrics();

    metrics.record(2, 2); // create: 2 computed, 0 skipped
    metrics.record(0, 2); // dropped-unchanged: 2 skipped
    metrics.record(1, 2); // one column changed: 1 computed, 1 skipped

    assertThat(metrics.getEmbeddingsComputed()).isEqualTo(3);
    assertThat(metrics.getEmbeddingsSkipped()).isEqualTo(3);
    assertThat(metrics.getEmbeddingsPossible()).isEqualTo(6);
    assertThat(metrics.getEmbeddingsPossible())
        .isEqualTo(metrics.getEmbeddingsComputed() + metrics.getEmbeddingsSkipped());
    assertThat(metrics.getSkipRatio()).isCloseTo(0.5, within(1e-9));
  }

  @Test
  void skipRatioIsZeroWhenIdle() {
    metrics = new EmbeddingDiffMetrics();
    assertThat(metrics.getEmbeddingsPossible()).isZero();
    assertThat(metrics.getSkipRatio()).isEqualTo(0.0);
  }

  @Test
  void registerExposesLiveValuesOverJmx() throws Exception {
    metrics = new EmbeddingDiffMetrics();
    metrics.register("test-" + System.nanoTime());
    ObjectName name = metrics.registeredName();

    assertThat(server.isRegistered(name)).isTrue();

    metrics.record(1, 4);
    assertThat(server.getAttribute(name, "EmbeddingsComputed")).isEqualTo(1L);
    assertThat(server.getAttribute(name, "EmbeddingsSkipped")).isEqualTo(3L);
    assertThat(server.getAttribute(name, "EmbeddingsPossible")).isEqualTo(4L);
    assertThat((double) server.getAttribute(name, "SkipRatio")).isCloseTo(0.75, within(1e-9));
  }

  @Test
  void unregisterRemovesTheMBean() {
    metrics = new EmbeddingDiffMetrics();
    metrics.register("test-" + System.nanoTime());
    ObjectName name = metrics.registeredName();
    assertThat(server.isRegistered(name)).isTrue();

    metrics.unregister();
    assertThat(server.isRegistered(name)).isFalse();
    assertThat(metrics.registeredName()).isNull();
  }

  @Test
  void blankConfiguredIdFallsBackToAutoSequence() {
    metrics = new EmbeddingDiffMetrics();
    metrics.register("  ");
    ObjectName name = metrics.registeredName();
    assertThat(name).isNotNull();
    assertThat(name.getKeyProperty("id")).isNotBlank();
    assertThat(server.isRegistered(name)).isTrue();
  }

  @Test
  void duplicateIdGetsDisambiguatedInsteadOfFailing() {
    String id = "dup-" + System.nanoTime();
    EmbeddingDiffMetrics first = new EmbeddingDiffMetrics();
    EmbeddingDiffMetrics second = new EmbeddingDiffMetrics();
    try {
      first.register(id);
      second.register(id); // same id -> must not throw, gets a distinct name
      assertThat(second.registeredName()).isNotEqualTo(first.registeredName());
      assertThat(server.isRegistered(first.registeredName())).isTrue();
      assertThat(server.isRegistered(second.registeredName())).isTrue();
    } finally {
      first.unregister();
      second.unregister();
    }
  }
}
