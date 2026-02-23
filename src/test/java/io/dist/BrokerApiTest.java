package io.dist;

import io.dist.model.ExchangeType;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

@QuarkusTest
public class BrokerApiTest {

    @jakarta.inject.Inject
    io.dist.service.QueueService queueService;

    @jakarta.inject.Inject
    io.dist.storage.StorageService storageService;

    @jakarta.transaction.Transactional
    @org.junit.jupiter.api.BeforeEach
    public void setup() {
        storageService.clear();
        io.dist.model.Message.deleteAll();
        io.dist.model.Binding.deleteAll();
        io.dist.model.Queue.deleteAll();
        io.dist.model.Exchange.deleteAll();
    }

    @Test
    public void testBrokerLifecycle() {
        // 1. Create Exchange
        given()
            .contentType(ContentType.JSON)
            .body("{\"name\": \"test-exchange\", \"type\": \"DIRECT\", \"durable\": true}")
            .when().post("/api/management/exchanges")
            .then()
            .statusCode(201);

        // 2. Create Queue
        given()
            .contentType(ContentType.JSON)
            .body("{\"name\": \"test-queue\", \"queueGroup\": \"test-group\", \"durable\": true, \"autoDelete\": false}")
            .when().post("/api/management/queues")
            .then()
            .statusCode(201);

        // 3. Bind Queue to Exchange
        given()
            .contentType(ContentType.JSON)
            .body("{\"exchangeName\": \"test-exchange\", \"queueName\": \"test-queue\", \"routingKey\": \"test-key\"}")
            .when().post("/api/management/bindings")
            .then()
            .statusCode(201);

        // 4. Publish Message
        given()
            .contentType(ContentType.JSON)
            .body("{\"routingKey\": \"test-key\", \"payload\": \"hello simpleMQ\"}")
            .when().post("/api/publish/test-exchange")
            .then()
            .statusCode(202);

        // 5. Poll Message
        String messageId = given()
            .when().get("/api/poll/test-queue")
            .then()
            .statusCode(200)
            .body("payload", is("hello simpleMQ"))
            .body("routingKey", is("test-key"))
            .extract().path("id");

        // 6. Acknowledge Message
        given()
            .pathParam("messageId", messageId)
            .when().post("/api/poll/ack/{messageId}")
            .then()
            .statusCode(200);

        // 7. Verify Queue is empty
        given()
            .when().get("/api/poll/test-queue")
            .then()
            .statusCode(204);
    }

    @Test
    public void testManagementLists() {
        given()
            .when().get("/api/management/exchanges")
            .then()
            .statusCode(200)
            .body("$.size()", greaterThanOrEqualTo(0));

        given()
            .when().get("/api/management/queues")
            .then()
            .statusCode(200)
            .body("$.size()", greaterThanOrEqualTo(0));
    }
}
