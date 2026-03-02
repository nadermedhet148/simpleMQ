package io.dist;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * Simple health-check / smoke-test endpoint.
 *
 * <p>Returns a plain-text greeting to verify that the Quarkus application
 * is running and serving HTTP requests.</p>
 */
@Path("/hello")
public class ExampleResource {

  /**
   * Returns a simple greeting message.
   *
   * @return plain-text "Hello from Quarkus REST"
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String hello() {
    return "Hello from Quarkus REST";
  }
}
