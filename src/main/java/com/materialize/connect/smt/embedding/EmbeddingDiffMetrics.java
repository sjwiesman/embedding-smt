package com.materialize.connect.smt.embedding;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Mutable, thread-safe counters backing {@link EmbeddingDiffMetricsMBean}, plus the JMX
 * registration lifecycle. One instance lives per {@link EmbeddingDiffTransform} (i.e. per Connect
 * task).
 */
public final class EmbeddingDiffMetrics implements EmbeddingDiffMetricsMBean {

  static final String DOMAIN = "com.materialize.connect.smt.embedding";

  /** Disambiguates MBean ObjectNames when no {@code metrics.id} is configured. */
  private static final AtomicInteger SEQ = new AtomicInteger();

  private final AtomicLong computed = new AtomicLong();
  private final AtomicLong skipped = new AtomicLong();

  private volatile ObjectName registeredName;

  @Override
  public long getEmbeddingsComputed() {
    return computed.get();
  }

  @Override
  public long getEmbeddingsSkipped() {
    return skipped.get();
  }

  @Override
  public long getEmbeddingsPossible() {
    return computed.get() + skipped.get();
  }

  @Override
  public double getSkipRatio() {
    long possible = getEmbeddingsPossible();
    return possible == 0 ? 0.0 : (double) skipped.get() / possible;
  }

  /**
   * Records the outcome of one record: {@code possible} embedding calls a naive pipeline would have
   * made for it, of which {@code computed} were actually made. The remainder was skipped.
   */
  public void record(long computedDelta, long possibleDelta) {
    computed.addAndGet(computedDelta);
    skipped.addAndGet(possibleDelta - computedDelta);
  }

  /**
   * Registers this instance as an MBean. Uses {@code configuredId} for the ObjectName's {@code id}
   * key when non-blank, otherwise an auto-assigned per-instance sequence. Never throws on a name
   * collision: it appends a unique suffix and retries so a task is never failed by a duplicate
   * MBean.
   */
  public void register(String configuredId) {
    String id =
        (configuredId == null || configuredId.isBlank())
            ? Integer.toString(SEQ.getAndIncrement())
            : configuredId;
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    try {
      registeredName = registerUnique(server, id);
    } catch (JMException e) {
      registeredName = null;
      throw new ConnectException("Failed to register embedding metrics MBean", e);
    }
  }

  /** Registers under {@code id}, retrying once with a unique suffix if that name is taken. */
  private ObjectName registerUnique(MBeanServer server, String id) throws JMException {
    ObjectName name = objectName(id);
    try {
      server.registerMBean(this, name);
    } catch (InstanceAlreadyExistsException collision) {
      name = objectName(id + "-" + SEQ.getAndIncrement());
      server.registerMBean(this, name);
    }
    return name;
  }

  private static ObjectName objectName(String id) throws MalformedObjectNameException {
    return new ObjectName(DOMAIN + ":type=EmbeddingDiff,id=" + ObjectName.quote(id));
  }

  /** Unregisters the MBean if registered. Idempotent and safe to call when never registered. */
  public void unregister() {
    ObjectName name = registeredName;
    if (name == null) {
      return;
    }
    try {
      ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
    } catch (InstanceNotFoundException e) {
      // already gone — nothing to do
    } catch (MBeanRegistrationException e) {
      throw new ConnectException("Failed to unregister embedding metrics MBean", e);
    } finally {
      registeredName = null;
    }
  }

  /** The ObjectName this instance is registered under, or null if not registered. */
  ObjectName registeredName() {
    return registeredName;
  }
}
