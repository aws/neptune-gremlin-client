# AGENTS.md

Guidance for AI agents and developers working in this repository.

## Project overview

Neptune Gremlin Client is a Java Gremlin client for Amazon Neptune that lets you
change the endpoints used by the client while it is running. It includes an
endpoint refresh agent that retrieves cluster topology details and updates the
client periodically, support for custom endpoint selectors (filtering instances
by tag, instance type, instance ID, Availability Zone, etc.), and support for
connecting to Neptune through a proxy such as a network or application load
balancer.

Coordinates: `software.amazon.neptune:neptune-gremlin-client`. The current
version is defined in the root `pom.xml`.

## Release branches

This repository maintains two long-lived release branches:

- **`main`** — the next **major** release line (currently `5.0.0-SNAPSHOT`).
  Breaking changes land here, including the eventual removal of APIs that are
  only retained as deprecated shims on the `4.x` line.
- **`4.x-dev`** — the **4.x** release line (currently `4.0.3-SNAPSHOT`). Changes
  here must remain backward compatible; deprecate rather than remove public API.

`4.x-dev` **merges up into `main`**: fixes and features are developed on
`4.x-dev` (keeping 4.x backward compatible) and then merged into `main`, where
breaking follow-ups (such as removing deprecated methods) may be applied. When
targeting a change, branch from and raise it against `4.x-dev` unless it is
inherently major/breaking, in which case target `main` directly.

## Module layout

This is a multi-module Maven project. The root `pom.xml` (packaging `pom`)
aggregates three modules:

- `gremlin-client` — the core client library (packaging `jar`). Contains the
  `GremlinClient` / `GremlinCluster` builders, endpoint selectors, and the
  `ClusterEndpointsRefreshAgent`. Sources live under
  `gremlin-client/src/main/java` in the `org.apache.tinkerpop.gremlin.driver`
  and `software.amazon.neptune` / `software.amazon.utils` packages. This is the
  only module that declares the Gremlin driver version.
- `gremlin-client-demo` — a runnable demo/CLI application. Built into an
  executable uber-jar via the maven-shade-plugin (main class
  `software.amazon.neptune.ApplicationRunner`).
- `neptune-endpoints-info-lambda` — an AWS Lambda function that retrieves
  details of Amazon Neptune endpoints; used by the refresh agent's Lambda proxy
  mode. Also packaged as a shaded jar.

## Prerequisites

- Java 17 (all modules compile with source/target 17). `JAVA_HOME` must point at
  a JDK 17 install — the build runs the maven-javadoc-plugin, which requires
  `JAVA_HOME` to be set correctly so it can locate `javadoc`.
- Maven 3.x.

## Build

From the repository root:

```bash
mvn clean install
```

This builds all three modules in dependency order and runs the unit tests. The
`release` Maven profile (in `gremlin-client`) additionally signs artifacts with
GPG and publishes to Maven Central; do not activate it for local builds.

## Test

Unit tests run automatically as part of the build. To run tests only:

```bash
mvn test
```

Tests use JUnit 4 and Mockito and live under each module's `src/test/java`.

## Key dependencies

Version properties are defined in `gremlin-client/pom.xml`:

- `gremlin.driver.version` — Apache TinkerPop `gremlin-driver`.
- `aws.sdk.version` — AWS SDK for Java 2.x modules.
- `sigv4.version` — `com.amazonaws:amazon-neptune-sigv4-signer` (SigV4 signing
  for IAM auth).
- `jackson.databind.version` — Jackson.

## Conventions

- `.gitignore` excludes `target/`. Note that shade-plugin
  `dependency-reduced-pom.xml` files are generated during the build; avoid
  committing regenerated copies as part of unrelated changes.
- When bumping the Gremlin/TinkerPop driver, change only the
  `gremlin.driver.version` property in `gremlin-client/pom.xml`; the
  `gremlin-driver` dependency references it via `${gremlin.driver.version}`, and
  no other module hardcodes a driver version.
