# Design: Extract the EmbeddingProvider SPI into a publishable module

## Context

The embedding-diff SMT lets the embedding backend be swapped via a small service-provider
interface — `EmbeddingProvider` plus its two failure exceptions — discovered through
`java.util.ServiceLoader`. Today those types live inside the SMT jar
(`com.materialize.connect.smt.embedding`), so a third party who wants to write their own
provider would have to depend on the entire SMT (Kafka Connect, Jackson, the transform
itself) and import a package whose name (`.connect.smt.`) wrongly implies the SPI is
SMT-specific.

This change extracts the SPI into its own dependency-free, independently publishable Maven
module so external developers can depend on a small, stable artifact and implement their
own providers. The work converts the single-module project into a Maven reactor; no runtime
behavior of the SMT changes.

## Decisions (confirmed with the user)

- **Layout:** multi-module monorepo (Maven reactor), not a separate repository.
- **SPI package:** `com.materialize.embedding.spi` (drops the misleading `.connect.smt.`).
- **Coordinates:** groupId `com.materialize` for every module.
  - Parent: `com.materialize:embedding-parent:0.1.0-SNAPSHOT` (`pom` packaging)
  - SPI: `com.materialize:embedding-spi`
  - SMT: `com.materialize:embedding-diff-smt` (groupId changes from `com.materialize.connect`)
- **OpenAI provider:** stays in the SMT module as the bundled default (not its own module).
- **Publishing:** structure now (produce `-sources` and `-javadoc` jars, locally
  installable); defer `distributionManagement`/signing until a destination is chosen.

## Target layout

```
embedding-smt/                         parent pom (packaging=pom)
├── pom.xml                            declares modules + shared config
├── README.md  LICENSE  NOTICE         remain at repo root
├── .github/workflows/tests.yml        builds the whole reactor (unchanged path)
├── docs/superpowers/specs/…           this document
├── embedding-spi/                     ← published artifact, zero dependencies
│   ├── pom.xml
│   └── src/main/java/com/materialize/embedding/spi/
│         EmbeddingProvider.java
│         RetriableEmbeddingException.java
│         FatalEmbeddingException.java
│   └── src/test/java/com/materialize/embedding/spi/
│         EmbeddingExceptionsTest.java   (small smoke test)
└── embedding-diff-smt/                depends on embedding-spi
    ├── pom.xml
    ├── src/assembly/plugin.xml
    ├── src/main/java/com/materialize/connect/smt/embedding/…   transform, config,
    │      RecordDiffer, OutputSchemaCache, metrics, RetryingEmbeddingClient,
    │      provider/OpenAiEmbeddingProvider
    ├── src/main/resources/META-INF/services/
    │      com.materialize.embedding.spi.EmbeddingProvider       (renamed)
    └── src/test/java/com/materialize/connect/smt/embedding/…   existing tests
```

The existing repo-root `src/` moves under `embedding-diff-smt/` via `git mv` to preserve
history.

## Module responsibilities

### `embedding-spi`
The published contract a provider implements. Contains exactly:
`EmbeddingProvider` (the `ServiceLoader` SPI), `RetriableEmbeddingException`,
`FatalEmbeddingException` — repackaged to `com.materialize.embedding.spi`, carrying their
existing Javadoc. The interface uses only `java.util`, so the module declares **no compile
dependencies** (JUnit/AssertJ test-scope only). Produces main + sources + javadoc jars.

### `embedding-diff-smt`
Everything else, unchanged in behavior. Adds a `compile`-scope dependency on
`embedding-spi`. All references to the three SPI types update their imports to the new
package. The `META-INF/services` file is renamed to the new SPI FQN; its contents (the
`OpenAiEmbeddingProvider` line) are unchanged.

### `embedding-parent`
`pom`-packaged aggregator. Declares the two `<modules>` and centralizes shared
configuration: `<properties>` (Java 17 `release`, kafka/jackson/junit versions, spotless
version), `<pluginManagement>` (spotless, surefire, maven-source, maven-javadoc), and
`<dependencyManagement>` (JUnit, AssertJ, and the internal `embedding-spi` version). Both
children inherit it.

## Packaging & build details

- **Self-contained plugin preserved.** `embedding-spi` is a compile dependency of the SMT,
  so `maven-shade-plugin` bundles its classes into the shaded SMT jar exactly as it bundles
  Jackson. The plugin zip's `lib/` is unchanged — no separate SPI jar to install.
- **Assembly paths.** `src/assembly/plugin.xml` switches its `LICENSE`/`NOTICE`/`README.md`
  references from `${project.basedir}` to `${maven.multiModuleProjectDirectory}` so they
  still resolve to the repo root, keeping the root README as the single source.
- **Formatting/CI.** Spotless moves to parent `pluginManagement`; `mvn spotless:apply`,
  `mvn spotless:check`, and `mvn package` run from the reactor root and cover both modules.
  The GitHub Actions workflow continues to run at the root with no path changes.
- **Versioning.** Both modules share the parent's `0.1.0-SNAPSHOT`.

## Documentation

README gains a short "Embedding SPI" section: the published coordinates
(`com.materialize:embedding-spi`), a minimal example of implementing `EmbeddingProvider`
and registering it via `META-INF/services`, and a note that the reactor builds both
modules. Existing build instructions are adjusted for the multi-module layout where needed.

## Testing / verification

1. `mvn clean package` at the root builds `embedding-spi` before `embedding-diff-smt`
   (reactor order), with all existing SMT tests (47) plus the new SPI smoke test green.
2. The SMT plugin zip still builds and remains self-contained (SPI classes present in the
   shaded jar).
3. `mvn install` followed by a throwaway consumer `pom` depending on
   `com.materialize:embedding-spi` compiles a trivial `EmbeddingProvider` implementation —
   confirming an external developer can build against the artifact alone (no Kafka/Jackson
   on their classpath).
4. `mvn spotless:check` passes for the whole reactor.

## Out of scope

- Choosing/configuring a publish destination (Maven Central, GitHub Packages) and signing.
- Moving `OpenAiEmbeddingProvider` into its own module.
- Any change to the SMT's runtime behavior, config keys, or the transform's class name.
