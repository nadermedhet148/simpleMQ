### Development Guidelines for simpleMQ

This document provides project-specific information for developers working on the `simpleMQ` message broker.

#### 1. Build and Configuration Instructions

This project is built using **Java 21** and **Quarkus** with the **Gradle** build tool.

- **Prerequisites**:
    - JDK 21 or higher.
    - Docker (optional, for native builds or containerization).

- **Build Commands**:
    - To compile the project: `./gradlew compileJava`
    - To build the application: `./gradlew build`
    - To run the application in development mode (with live reload): `./gradlew quarkusDev`
    - To build a native executable: `./gradlew build -Dquarkus.package.type=native`

- **Configuration**:
    - Main configuration file: `src/main/resources/application.properties`
    - SQLite database path and other runtime properties should be defined here.

#### 2. Testing Information

Testing is performed using **JUnit 5** and **RestAssured**.

- **Running Tests**:
    - Run all tests: `./gradlew test`
    - Run a specific test class: `./gradlew test --tests io.dist.YourTest`
    - Tests are also run during the build process unless skipped with `-x test`.

- **Adding New Tests**:
    - Place tests in `src/test/java/`.
    - Use `@QuarkusTest` annotation on the class to bootstrap the Quarkus environment.
    - Use RestAssured for integration testing of REST endpoints.

- **Simple Test Example**:
```java
package io.dist;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class GuidelinesDemoTest {
    @Test
    void demoTest() {
        assertTrue(true, "This is a demonstration test for guidelines.");
    }
}
```

#### 3. Functional and Non-Functional Requirements

- **Functional Requirements**:
    - **Exchange & Queue Management**: Ability to create and delete exchanges and queues. Queues are assigned to specific groups.
    - **Exchange Types**: Support for `Direct` (routing key based) and `Pub-Sub` (Fanout/Broadcast) exchange logic.
    - **Producer API**: REST endpoint to publish messages to a specific exchange with a routing key.
    - **Consumer API (Pull)**: REST endpoint for consumers to pull messages from assigned queues.
    - **Acknowledgment System**: Mechanism to mark messages as 'done' (Ack) or 'failed' (Nack).
    - **Dead Letter Queue (DLQ)**: Automated routing of messages to a DLQ after a specific number of failed attempts or a Nack.
    - **Management UI**: A simple web-based dashboard to visualize and manage exchanges, queues, and message flow.

- **Non-Functional Requirements**:
    - **Hybrid Storage**: Messages must be handled in-memory for speed but persisted to disk using SQLite for durability.
    - **High Availability**: Support for cluster mode with replication using Quorums.
    - **Consensus & Election**: Implementation of the Raft algorithm for leader election and state consistency across the cluster.
    - **Framework**: Developed using the Java Quarkus framework.
    - **Quality Assurance**: Inclusion of Unit, Integration, and End-to-End (E2E) test cases.

#### 4. Additional Development Information

- **Code Style**:
    - Follow standard Java coding conventions.
    - Use `jakarta.*` packages for REST and DI (Quarkus uses Jakarta EE).
    - Encoding: All source files must be UTF-8.
    - Java Version: Strictly target Java 21 as specified in `build.gradle`.

- **Key Components**:
    - Core Domain: `io.dist` package contains the main logic.
    - Persistence: SQLite is used for disk-based storage.
    - Clustering: Raft implementation is planned for HA.

- **Debugging**:
    - Quarkus Dev Mode (`./gradlew quarkusDev`) provides excellent debugging capabilities, including remote debugging support on port `5005` by default.
