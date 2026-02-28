package io.dist;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@QuarkusTest
public class ManagementSummaryTest {

    @Test
    void testGetSummary() {
        given()
                .when().get("/api/management/summary")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("nodeId", notNullValue())
                .body("leaderId", notNullValue())
                .body("metrics", notNullValue())
                .body("queues", notNullValue())
                .body("exchanges", notNullValue());
    }

    @Test
    void testMetricsIncrease() throws InterruptedException {
        // Wait for leader election
        boolean leaderFound = false;
        for (int i = 0; i < 30; i++) {
            try {
                Boolean isLeader = given()
                        .when().get("/api/management/summary")
                        .then().extract().path("isLeader");
                if (isLeader != null && isLeader) {
                    leaderFound = true;
                    break;
                }
            } catch (Exception e) {
                // Node might not be ready yet
            }
            Thread.sleep(1000);
        }

        // Initially 0 or some value
        Integer initialPublished = given()
                .when().get("/api/management/summary")
                .then().extract().path("metrics.totalMessagesPublished");

        // Create exchange and queue
        given()
                .contentType(ContentType.JSON)
                .body("{\"name\": \"test-exchange\", \"type\": \"DIRECT\", \"durable\": true}")
                .post("/api/management/exchanges")
                .then().statusCode(201);
        
        given()
                .contentType(ContentType.JSON)
                .body("{\"name\": \"test-queue\", \"queueGroup\": \"default\", \"durable\": true}")
                .post("/api/management/queues")
                .then().statusCode(201);
                
        given()
                .contentType(ContentType.JSON)
                .body("{\"exchangeName\": \"test-exchange\", \"queueName\": \"test-queue\", \"routingKey\": \"test-key\"}")
                .post("/api/management/bindings")
                .then().statusCode(201);

        // Publish a message
        given()
                .contentType(ContentType.JSON)
                .body("{\"routingKey\": \"test-key\", \"payload\": \"Hello\"}")
                .post("/api/publish/test-exchange")
                .then().statusCode(202);

        // Check metrics
        given()
                .when().get("/api/management/summary")
                .then()
                .statusCode(200)
                .body("metrics.totalMessagesPublished", greaterThan(initialPublished));
    }
}
