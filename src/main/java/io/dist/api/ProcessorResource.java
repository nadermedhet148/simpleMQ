package io.dist.api;

import io.dist.model.StreamProcessor;
import io.dist.model.StreamProcessorState;
import io.dist.service.ProcessorService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * REST resource for managing server-side stream processing pipelines.
 *
 * <h3>Endpoints</h3>
 * <ul>
 *   <li>{@code POST   /api/processors}              — create a processor</li>
 *   <li>{@code GET    /api/processors}              — list all processors</li>
 *   <li>{@code GET    /api/processors/{id}}         — get a single processor</li>
 *   <li>{@code DELETE /api/processors/{id}}         — delete a processor</li>
 *   <li>{@code POST   /api/processors/{id}/pause}   — pause a processor</li>
 *   <li>{@code POST   /api/processors/{id}/resume}  — resume a processor</li>
 *   <li>{@code GET    /api/processors/{id}/state}   — get aggregation state</li>
 * </ul>
 */
@Path("/api/processors")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ProcessorResource {

    private static final Logger LOG = Logger.getLogger(ProcessorResource.class);

    @Inject
    ProcessorService processorService;

    @POST
    public Uni<Response> createProcessor(StreamProcessor body) {
        if (body.name == null || body.name.isBlank()) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                    .entity("name is required").build());
        }
        if (body.sourceQueue == null || body.sourceQueue.isBlank()) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                    .entity("sourceQueue is required").build());
        }
        if (body.targetExchange == null || body.targetExchange.isBlank()) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                    .entity("targetExchange is required").build());
        }

        body.id = UUID.randomUUID().toString();
        body.createdAt = LocalDateTime.now();

        return processorService.createProcessor(body)
                .replaceWith(Response.status(Response.Status.CREATED).entity(body).build());
    }

    @GET
    public Uni<List<StreamProcessor>> listProcessors() {
        return Uni.createFrom().item(() -> processorService.listAll())
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @GET
    @Path("/{id}")
    public Uni<Response> getProcessor(@PathParam("id") String id) {
        return Uni.createFrom().item(() -> {
            StreamProcessor p = processorService.findById(id);
            if (p == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            return Response.ok(p).build();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @DELETE
    @Path("/{id}")
    public Uni<Response> deleteProcessor(@PathParam("id") String id) {
        return processorService.deleteProcessor(id)
                .replaceWith(Response.noContent().build());
    }

    @POST
    @Path("/{id}/pause")
    public Uni<Response> pauseProcessor(@PathParam("id") String id) {
        return processorService.pauseProcessor(id)
                .replaceWith(Response.noContent().build());
    }

    @POST
    @Path("/{id}/resume")
    public Uni<Response> resumeProcessor(@PathParam("id") String id) {
        return processorService.resumeProcessor(id)
                .replaceWith(Response.noContent().build());
    }

    @GET
    @Path("/{id}/state")
    public Uni<List<StreamProcessorState>> getProcessorState(@PathParam("id") String id) {
        return Uni.createFrom().item(() -> processorService.getProcessorStates(id))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }
}
