# Extract embedding-spi Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the dependency-free `EmbeddingProvider` SPI into a separately publishable Maven module (`com.materialize:embedding-spi`) within a new Maven reactor, with the existing SMT moved under `embedding-diff-smt/` and consuming the SPI.

**Architecture:** Convert the single-module project into a 3-POM reactor: a `pom`-packaged parent (`com.materialize:embedding-parent`) aggregating `embedding-spi` (the SPI interface + two exceptions, repackaged to `com.materialize.embedding.spi`, zero dependencies) and `embedding-diff-smt` (everything else, depends on the SPI). The SPI is shaded into the plugin jar so the Connect plugin stays self-contained; no SMT runtime behavior changes.

**Tech Stack:** Java 17, Maven (reactor / multi-module), JUnit 5 + AssertJ, maven-shade/-assembly/-source/-javadoc, Spotless (google-java-format), `java.util.ServiceLoader`.

**Build note:** `java` is not on PATH; every Maven command must be prefixed `JAVA_HOME=/opt/homebrew/opt/openjdk`.

---

### Task 1: Commit the in-flight work so later moves register as renames

The working tree has uncommitted work (JMX metrics, fixed `before`/`after` fields, Javadoc pass, `/simplify` cleanups). Commit it first; otherwise the `git mv` in later tasks mixes with content changes and git won't detect renames. Do **not** stage `.idea/`.

**Files:** all currently-modified/untracked source under `src/`, plus `README.md`, `pom.xml`.

- [ ] **Step 1: Confirm formatting + tests are green before committing**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk mvn --batch-mode --no-transfer-progress spotless:check test`
Expected: `BUILD SUCCESS`, `Tests run: 47, Failures: 0, Errors: 0`.

- [ ] **Step 2: Stage everything except `.idea/` and commit**

```bash
git add src README.md pom.xml
git status --short   # verify .idea/ is NOT staged
git commit -m "feat: JMX savings metrics, fixed envelope fields, SPI Javadoc + cleanups"
```

- [ ] **Step 3: Verify the tree is clean except `.idea/`**

Run: `git status --short`
Expected: only `?? .idea/` remains.

---

### Task 2: Move the existing module sources under `embedding-diff-smt/`

A pure relocation: `src/` (including `src/assembly/`) becomes `embedding-diff-smt/src/`. The SPI files are carved out of it in Task 4.

**Files:**
- Move: `src/` → `embedding-diff-smt/src/`

- [ ] **Step 1: git mv the source tree**

```bash
mkdir -p embedding-diff-smt
git mv src embedding-diff-smt/src
```

- [ ] **Step 2: Verify the move is a rename, not a delete+add**

Run: `git status --short`
Expected: lines beginning with `R ` (renames) for files under `src/...` → `embedding-diff-smt/src/...`. No `D`/`??` for those paths.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "refactor: move SMT sources under embedding-diff-smt/ module dir"
```

---

### Task 3: Create the `embedding-diff-smt` child POM

The SMT module inherits shared config from the parent (created in Task 6) and adds the SPI dependency, Jackson, Kafka (provided), and the shade+assembly build. Versions for managed deps/plugins come from the parent, so they're omitted here.

**Files:**
- Create: `embedding-diff-smt/pom.xml`

- [ ] **Step 1: Write `embedding-diff-smt/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>com.materialize</groupId>
    <artifactId>embedding-parent</artifactId>
    <version>0.1.0-SNAPSHOT</version>
  </parent>

  <artifactId>embedding-diff-smt</artifactId>
  <packaging>jar</packaging>

  <name>Embedding Diff SMT</name>
  <description>Kafka Connect Single Message Transform that converts a CDC before/after
    envelope into a minimal, embedding-enriched diff for sinks running in UPSERT mode. It
    emits only the changed columns plus a &lt;col&gt;_embedding vector for any changed
    embedded column, so unchanged columns and their embeddings are left untouched.</description>

  <dependencies>
    <dependency>
      <groupId>com.materialize</groupId>
      <artifactId>embedding-spi</artifactId>
    </dependency>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>connect-api</artifactId>
      <version>${kafka.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>connect-transforms</artifactId>
      <version>${kafka.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-databind</artifactId>
      <version>${jackson.version}</version>
    </dependency>

    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
    </dependency>
    <dependency>
      <groupId>org.assertj</groupId>
      <artifactId>assertj-core</artifactId>
    </dependency>
    <dependency>
      <groupId>com.squareup.okhttp3</groupId>
      <artifactId>mockwebserver</artifactId>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <createDependencyReducedPom>false</createDependencyReducedPom>
            </configuration>
          </execution>
        </executions>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-assembly-plugin</artifactId>
        <configuration>
          <appendAssemblyId>false</appendAssemblyId>
          <descriptors>
            <descriptor>src/assembly/plugin.xml</descriptor>
          </descriptors>
        </configuration>
        <executions>
          <execution>
            <id>plugin-archive</id>
            <phase>package</phase>
            <goals><goal>single</goal></goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 2: Commit**

```bash
git add embedding-diff-smt/pom.xml
git commit -m "build: add embedding-diff-smt child pom"
```

---

### Task 4: Create the `embedding-spi` module (move + repackage the SPI types)

Move the three SPI types into the new module, change their package to `com.materialize.embedding.spi`, and remove Javadoc `{@link}`s to downstream classes (`RetryingEmbeddingClient`, `OpenAiEmbeddingProvider`) that live in the SMT module — those links would break the SPI's javadoc jar.

**Files:**
- Move+edit: `embedding-diff-smt/src/main/java/com/materialize/connect/smt/embedding/EmbeddingProvider.java` → `embedding-spi/src/main/java/com/materialize/embedding/spi/EmbeddingProvider.java`
- Move+edit: `…/FatalEmbeddingException.java` → `embedding-spi/src/main/java/com/materialize/embedding/spi/FatalEmbeddingException.java`
- Move+edit: `…/RetriableEmbeddingException.java` → `embedding-spi/src/main/java/com/materialize/embedding/spi/RetriableEmbeddingException.java`
- Create: `embedding-spi/pom.xml`
- Test: `embedding-spi/src/test/java/com/materialize/embedding/spi/EmbeddingExceptionsTest.java`

- [ ] **Step 1: git mv the three SPI files into the new module**

```bash
mkdir -p embedding-spi/src/main/java/com/materialize/embedding/spi
BASE=embedding-diff-smt/src/main/java/com/materialize/connect/smt/embedding
DEST=embedding-spi/src/main/java/com/materialize/embedding/spi
git mv $BASE/EmbeddingProvider.java          $DEST/EmbeddingProvider.java
git mv $BASE/FatalEmbeddingException.java     $DEST/FatalEmbeddingException.java
git mv $BASE/RetriableEmbeddingException.java $DEST/RetriableEmbeddingException.java
```

- [ ] **Step 2: Rewrite `embedding-spi/src/main/java/com/materialize/embedding/spi/EmbeddingProvider.java`**

Change the package and drop the downstream `OpenAiEmbeddingProvider` link; keep it provider-neutral (it is no longer "the connector's" config).

```java
package com.materialize.embedding.spi;

import java.util.List;
import java.util.Map;

/**
 * The extension point for embedding backends: a service that turns a piece of text into an
 * embedding vector. Implementations are discovered at runtime via {@link java.util.ServiceLoader}
 * and selected by matching {@link #name()} against the consumer's {@code provider} configuration.
 * Additional backends are added by registering new implementations on the classpath.
 */
public interface EmbeddingProvider extends AutoCloseable {

  /** Identifier matched against the {@code provider} config value (e.g. "openai"). */
  String name();

  /** Receives the consumer's raw config map; reads its own provider-specific keys. */
  void configure(Map<String, ?> configs);

  /**
   * Returns the embedding vector for the given text. Throws {@link RetriableEmbeddingException} for
   * transient failures and {@link FatalEmbeddingException} for permanent ones.
   */
  List<Float> embed(String text);

  @Override
  default void close() {}
}
```

- [ ] **Step 3: Rewrite `embedding-spi/src/main/java/com/materialize/embedding/spi/RetriableEmbeddingException.java`**

Change package; replace the `{@link RetryingEmbeddingClient}` reference (downstream) with neutral wording.

```java
package com.materialize.embedding.spi;

/**
 * Signals a transient embedding failure that may succeed if tried again, such as a timeout or an
 * HTTP 429 / 5xx response. Raised by {@link EmbeddingProvider} implementations to mark a call as
 * eligible for retry by the consumer.
 */
public class RetriableEmbeddingException extends RuntimeException {
  public RetriableEmbeddingException(String message) {
    super(message);
  }

  public RetriableEmbeddingException(String message, Throwable cause) {
    super(message, cause);
  }
}
```

- [ ] **Step 4: Rewrite `embedding-spi/src/main/java/com/materialize/embedding/spi/FatalEmbeddingException.java`**

```java
package com.materialize.embedding.spi;

/**
 * Signals a permanent embedding failure — one that retrying cannot fix, such as a malformed request
 * or an authentication error. Raised by {@link EmbeddingProvider} implementations to tell the
 * consumer to give up immediately rather than retry.
 */
public class FatalEmbeddingException extends RuntimeException {
  public FatalEmbeddingException(String message) {
    super(message);
  }

  public FatalEmbeddingException(String message, Throwable cause) {
    super(message, cause);
  }
}
```

- [ ] **Step 5: Write the SPI smoke test `embedding-spi/src/test/java/com/materialize/embedding/spi/EmbeddingExceptionsTest.java`**

```java
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
```

- [ ] **Step 6: Write `embedding-spi/pom.xml`**

Inherits group/version from the parent; declares no compile dependencies; attaches sources + javadoc jars.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>com.materialize</groupId>
    <artifactId>embedding-parent</artifactId>
    <version>0.1.0-SNAPSHOT</version>
  </parent>

  <artifactId>embedding-spi</artifactId>
  <packaging>jar</packaging>

  <name>Embedding SPI</name>
  <description>Service-provider interface for pluggable embedding backends: implement
    EmbeddingProvider and register it via java.util.ServiceLoader. Consumed by the
    embedding-diff Kafka Connect SMT and implementable by third parties.</description>

  <dependencies>
    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
    </dependency>
    <dependency>
      <groupId>org.assertj</groupId>
      <artifactId>assertj-core</artifactId>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-source-plugin</artifactId>
        <executions>
          <execution>
            <id>attach-sources</id>
            <goals><goal>jar-no-fork</goal></goals>
          </execution>
        </executions>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-javadoc-plugin</artifactId>
        <executions>
          <execution>
            <id>attach-javadoc</id>
            <goals><goal>jar</goal></goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 7: Commit**

```bash
git add embedding-spi embedding-diff-smt
git commit -m "feat: extract embedding-spi module (repackage to com.materialize.embedding.spi)"
```

---

### Task 5: Update the SMT module to consume the relocated SPI

The three SPI types moved packages, so the SMT's references need imports, and the `ServiceLoader` resource file (named after the SPI's fully-qualified interface name) must be renamed.

**Files:**
- Modify: `embedding-diff-smt/src/main/java/com/materialize/connect/smt/embedding/EmbeddingDiffTransform.java`
- Modify: `embedding-diff-smt/src/main/java/com/materialize/connect/smt/embedding/RetryingEmbeddingClient.java`
- Modify: `embedding-diff-smt/src/main/java/com/materialize/connect/smt/embedding/provider/OpenAiEmbeddingProvider.java`
- Modify: `embedding-diff-smt/src/test/java/com/materialize/connect/smt/embedding/EmbeddingDiffTransformTest.java`
- Modify: `embedding-diff-smt/src/test/java/com/materialize/connect/smt/embedding/RetryingEmbeddingClientTest.java`
- Modify: `embedding-diff-smt/src/test/java/com/materialize/connect/smt/embedding/provider/OpenAiEmbeddingProviderTest.java`
- Rename: `embedding-diff-smt/src/main/resources/META-INF/services/com.materialize.connect.smt.embedding.EmbeddingProvider` → `…/com.materialize.embedding.spi.EmbeddingProvider`

- [ ] **Step 1: Rename the ServiceLoader registration file (contents unchanged)**

```bash
cd embedding-diff-smt/src/main/resources/META-INF/services
git mv com.materialize.connect.smt.embedding.EmbeddingProvider \
       com.materialize.embedding.spi.EmbeddingProvider
cd -
```
Its single line stays `com.materialize.connect.smt.embedding.provider.OpenAiEmbeddingProvider`.

- [ ] **Step 2: Add the SPI import to `EmbeddingDiffTransform.java`**

Add this import (alongside the existing `org.apache.kafka...` imports; Spotless will order it):

```java
import com.materialize.embedding.spi.EmbeddingProvider;
```

- [ ] **Step 3: Add the three SPI imports to `RetryingEmbeddingClient.java`**

It references `EmbeddingProvider`, `RetriableEmbeddingException`, and `FatalEmbeddingException` (catches the latter two). Add:

```java
import com.materialize.embedding.spi.EmbeddingProvider;
import com.materialize.embedding.spi.FatalEmbeddingException;
import com.materialize.embedding.spi.RetriableEmbeddingException;
```

- [ ] **Step 4: Repoint the existing imports in `provider/OpenAiEmbeddingProvider.java`**

Replace these three lines:

```java
import com.materialize.connect.smt.embedding.EmbeddingProvider;
import com.materialize.connect.smt.embedding.FatalEmbeddingException;
import com.materialize.connect.smt.embedding.RetriableEmbeddingException;
```

with:

```java
import com.materialize.embedding.spi.EmbeddingProvider;
import com.materialize.embedding.spi.FatalEmbeddingException;
import com.materialize.embedding.spi.RetriableEmbeddingException;
```

- [ ] **Step 5: Add the SPI import to `EmbeddingDiffTransformTest.java`**

Its `StubProvider implements EmbeddingProvider`. Add:

```java
import com.materialize.embedding.spi.EmbeddingProvider;
```

- [ ] **Step 6: Add the three SPI imports to `RetryingEmbeddingClientTest.java`**

Its `ScriptedProvider implements EmbeddingProvider` and throws both exceptions. Add:

```java
import com.materialize.embedding.spi.EmbeddingProvider;
import com.materialize.embedding.spi.FatalEmbeddingException;
import com.materialize.embedding.spi.RetriableEmbeddingException;
```

- [ ] **Step 7: Repoint the existing imports in `provider/OpenAiEmbeddingProviderTest.java`**

Replace:

```java
import com.materialize.connect.smt.embedding.FatalEmbeddingException;
import com.materialize.connect.smt.embedding.RetriableEmbeddingException;
```

with:

```java
import com.materialize.embedding.spi.FatalEmbeddingException;
import com.materialize.embedding.spi.RetriableEmbeddingException;
```

- [ ] **Step 8: Commit (build verified after the parent POM exists in Task 6)**

```bash
git add embedding-diff-smt
git commit -m "refactor: point SMT at com.materialize.embedding.spi"
```

---

### Task 6: Replace the root POM with the reactor parent

The root `pom.xml` becomes a `pom`-packaged aggregator: it lists the modules and centralizes versions, dependency management (including the internal `embedding-spi`), and plugin management (surefire, spotless, source, javadoc, shade, assembly). Spotless is declared in `<build><plugins>` so both children run it; javadoc doclint is disabled to avoid failing on intentionally terse method docs.

**Files:**
- Modify (full replace): `pom.xml`

- [ ] **Step 1: Replace `pom.xml` with the parent POM**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>com.materialize</groupId>
  <artifactId>embedding-parent</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <packaging>pom</packaging>

  <name>Embedding Parent</name>
  <description>Reactor for the embedding SPI and the embedding-diff Kafka Connect SMT.</description>
  <url>https://github.com/sjwiesman/embedding-smt</url>

  <licenses>
    <license>
      <name>The Apache License, Version 2.0</name>
      <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
    </license>
  </licenses>

  <developers>
    <developer>
      <name>Seth Wiesman</name>
      <email>seth@materialize.com</email>
      <organization>Materialize, Inc.</organization>
      <organizationUrl>https://materialize.com</organizationUrl>
    </developer>
  </developers>

  <scm>
    <connection>scm:git:git://github.com/sjwiesman/embedding-smt.git</connection>
    <developerConnection>scm:git:ssh://git@github.com/sjwiesman/embedding-smt.git</developerConnection>
    <url>https://github.com/sjwiesman/embedding-smt</url>
  </scm>

  <modules>
    <module>embedding-spi</module>
    <module>embedding-diff-smt</module>
  </modules>

  <properties>
    <maven.compiler.release>17</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <kafka.version>3.8.1</kafka.version>
    <jackson.version>2.17.2</jackson.version>
    <junit.version>5.10.3</junit.version>
    <assertj.version>3.26.3</assertj.version>
    <mockwebserver.version>4.12.0</mockwebserver.version>
    <spotless.version>2.44.5</spotless.version>
  </properties>

  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>com.materialize</groupId>
        <artifactId>embedding-spi</artifactId>
        <version>${project.version}</version>
      </dependency>
      <dependency>
        <groupId>org.junit.jupiter</groupId>
        <artifactId>junit-jupiter</artifactId>
        <version>${junit.version}</version>
        <scope>test</scope>
      </dependency>
      <dependency>
        <groupId>org.assertj</groupId>
        <artifactId>assertj-core</artifactId>
        <version>${assertj.version}</version>
        <scope>test</scope>
      </dependency>
      <dependency>
        <groupId>com.squareup.okhttp3</groupId>
        <artifactId>mockwebserver</artifactId>
        <version>${mockwebserver.version}</version>
        <scope>test</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>

  <build>
    <pluginManagement>
      <plugins>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-surefire-plugin</artifactId>
          <version>3.2.5</version>
        </plugin>
        <plugin>
          <groupId>com.diffplug.spotless</groupId>
          <artifactId>spotless-maven-plugin</artifactId>
          <version>${spotless.version}</version>
          <configuration>
            <java>
              <googleJavaFormat>
                <version>1.35.0</version>
              </googleJavaFormat>
            </java>
          </configuration>
        </plugin>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-source-plugin</artifactId>
          <version>3.3.1</version>
        </plugin>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-javadoc-plugin</artifactId>
          <version>3.8.0</version>
          <configuration>
            <doclint>none</doclint>
          </configuration>
        </plugin>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-shade-plugin</artifactId>
          <version>3.6.0</version>
        </plugin>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-assembly-plugin</artifactId>
          <version>3.7.1</version>
        </plugin>
      </plugins>
    </pluginManagement>

    <plugins>
      <plugin>
        <groupId>com.diffplug.spotless</groupId>
        <artifactId>spotless-maven-plugin</artifactId>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 2: Verify the full reactor builds, formats, and tests green**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk mvn --batch-mode --no-transfer-progress spotless:apply clean package`
Expected: `Reactor Summary` lists `Embedding Parent`, `Embedding SPI`, `Embedding Diff SMT` all `SUCCESS`; `embedding-spi` tests run (2) and SMT tests run (47); `BUILD SUCCESS`.

- [ ] **Step 3: Verify the SPI sources + javadoc jars were produced**

Run: `ls embedding-spi/target/*.jar`
Expected: `embedding-spi-0.1.0-SNAPSHOT.jar`, `embedding-spi-0.1.0-SNAPSHOT-sources.jar`, `embedding-spi-0.1.0-SNAPSHOT-javadoc.jar`.

- [ ] **Step 4: Verify the plugin zip is still self-contained (SPI classes shaded in)**

Run: `unzip -l embedding-diff-smt/target/embedding-diff-smt-0.1.0-SNAPSHOT.jar | grep -E "com/materialize/embedding/spi/EmbeddingProvider|META-INF/services/com.materialize.embedding.spi.EmbeddingProvider"`
Expected: both the `EmbeddingProvider.class` under `com/materialize/embedding/spi/` and the renamed services file are listed (present in the shaded jar).
Also run: `ls embedding-diff-smt/target/*.zip`
Expected: `embedding-diff-smt-0.1.0-SNAPSHOT.zip`.

- [ ] **Step 5: Commit**

```bash
git add pom.xml
git commit -m "build: convert root pom into reactor parent (com.materialize:embedding-parent)"
```

---

### Task 7: Fix the assembly's repo-root file references

The assembly previously read `LICENSE`/`NOTICE`/`README.md` from `${project.basedir}` (then the repo root). Now that the SMT is a submodule, point those at the reactor root via `${maven.multiModuleProjectDirectory}`. The shaded-jar `<file>` source stays module-relative.

**Files:**
- Modify: `embedding-diff-smt/src/assembly/plugin.xml`

- [ ] **Step 1: Update the fileSet directory**

Replace:

```xml
  <fileSets>
    <fileSet>
      <directory>${project.basedir}</directory>
      <outputDirectory>.</outputDirectory>
      <includes>
        <include>LICENSE</include>
        <include>NOTICE</include>
        <include>README.md</include>
      </includes>
    </fileSet>
  </fileSets>
```

with:

```xml
  <fileSets>
    <fileSet>
      <directory>${maven.multiModuleProjectDirectory}</directory>
      <outputDirectory>.</outputDirectory>
      <includes>
        <include>LICENSE</include>
        <include>NOTICE</include>
        <include>README.md</include>
      </includes>
    </fileSet>
  </fileSets>
```

- [ ] **Step 2: Rebuild and confirm the zip contains the root docs**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk mvn --batch-mode --no-transfer-progress -pl embedding-diff-smt -am package`
Then: `unzip -l embedding-diff-smt/target/embedding-diff-smt-0.1.0-SNAPSHOT.zip | grep -E "README.md|LICENSE|NOTICE"`
Expected: `embedding-diff-smt/README.md`, `embedding-diff-smt/LICENSE`, `embedding-diff-smt/NOTICE` listed (non-empty).

- [ ] **Step 3: Commit**

```bash
git add embedding-diff-smt/src/assembly/plugin.xml
git commit -m "build: resolve assembly docs from reactor root"
```

---

### Task 8: Verify a third party can compile against `embedding-spi` alone

Install the reactor locally, then compile a throwaway consumer that depends only on `com.materialize:embedding-spi` and implements `EmbeddingProvider` — proving the artifact needs no Kafka/Jackson on the consumer's classpath.

**Files:**
- Create (throwaway, outside the repo): `/tmp/spi-consumer/pom.xml`, `/tmp/spi-consumer/src/main/java/example/MyProvider.java`

- [ ] **Step 1: Install the reactor to the local Maven repo**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk mvn --batch-mode --no-transfer-progress install -DskipTests`
Expected: `BUILD SUCCESS`; `embedding-spi-0.1.0-SNAPSHOT` installed to `~/.m2`.

- [ ] **Step 2: Create the consumer POM**

```bash
mkdir -p /tmp/spi-consumer/src/main/java/example
cat > /tmp/spi-consumer/pom.xml <<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>example</groupId>
  <artifactId>spi-consumer</artifactId>
  <version>1.0</version>
  <properties><maven.compiler.release>17</maven.compiler.release></properties>
  <dependencies>
    <dependency>
      <groupId>com.materialize</groupId>
      <artifactId>embedding-spi</artifactId>
      <version>0.1.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
</project>
XML
```

- [ ] **Step 3: Create a trivial provider implementation**

```bash
cat > /tmp/spi-consumer/src/main/java/example/MyProvider.java <<'JAVA'
package example;

import com.materialize.embedding.spi.EmbeddingProvider;
import java.util.List;
import java.util.Map;

public class MyProvider implements EmbeddingProvider {
  @Override public String name() { return "mine"; }
  @Override public void configure(Map<String, ?> configs) {}
  @Override public List<Float> embed(String text) { return List.of(0.0f); }
}
JAVA
```

- [ ] **Step 4: Compile the consumer**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk mvn -f /tmp/spi-consumer/pom.xml --batch-mode --no-transfer-progress compile`
Expected: `BUILD SUCCESS` — confirms `embedding-spi` is consumable standalone.

- [ ] **Step 5: Clean up the throwaway (no commit; outside the repo)**

```bash
rm -rf /tmp/spi-consumer
```

---

### Task 9: Document the SPI in the README

Add a short section telling third parties how to depend on and implement the SPI, and note the reactor layout. Keep the existing content.

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add an "Embedding SPI" section**

Insert the following after the existing intro section (before "## Source: Materialize Kafka sink"):

```markdown
## Embedding SPI

The embedding backend is a small, dependency-free service-provider interface published as
its own artifact, so you can implement your own provider without depending on the SMT:

```xml
<dependency>
  <groupId>com.materialize</groupId>
  <artifactId>embedding-spi</artifactId>
  <version>0.1.0</version>
</dependency>
```

Implement `com.materialize.embedding.spi.EmbeddingProvider` and register it for
`java.util.ServiceLoader` by adding its fully-qualified class name to
`META-INF/services/com.materialize.embedding.spi.EmbeddingProvider`. Select it at runtime
with `transforms.embed.provider=<your name()>`. Throw `RetriableEmbeddingException` for
transient failures (timeouts, 429, 5xx) and `FatalEmbeddingException` for permanent ones;
the SMT retries the former and fails fast on the latter.

This repository is a Maven reactor: `embedding-spi/` (the published SPI) and
`embedding-diff-smt/` (the Connect plugin, which bundles the SPI). `mvn clean package` at
the root builds both.
```
```

- [ ] **Step 2: Update the "From source" build note if it references a single module**

Confirm the build commands still read `mvn clean package` (run at the repo root — unchanged for a reactor). If any path now needs a module qualifier, leave the root-level command as-is; no change required.

- [ ] **Step 3: Run a final whole-reactor verification**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk mvn --batch-mode --no-transfer-progress spotless:check clean package`
Expected: `BUILD SUCCESS`, all three reactor modules SUCCESS, SPI (2) + SMT (47) tests green.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: document the embedding-spi artifact and reactor layout"
```

---

## Notes on CI

`.github/workflows/tests.yml` runs `mvn spotless:check` and `mvn clean package` at the repo
root with Temurin JDK 17 and Maven caching. A reactor build is driven from the root, so no
workflow change is required; Task 9 Step 3 mirrors what CI runs. (If you later want CI to
also publish `embedding-spi`, that's the deferred publishing work, out of scope here.)
```
