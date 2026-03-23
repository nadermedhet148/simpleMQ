package io.dist.api;

import io.dist.cluster.RaftService;
import io.dist.model.Message;
import io.dist.model.Queue;
import io.dist.model.QueueType;
import io.dist.model.StreamConsumerOffset;
import io.dist.storage.StreamBuffer;
import io.dist.storage.StorageService;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

/**
 * REST endpoint for consuming messages from STREAM queues.
 *
 * <h3>Endpoint</h3>
 * <pre>GET /api/stream/{queue}?consumerId={id}</pre>
 *
 * <p>Each call returns the next unread message for the given {@code consumerId}
 * and automatically advances the consumer's offset. No explicit ACK step is
 * required (at-most-once delivery). A {@code 204 No Content} response is
 * returned when the consumer has caught up to the end of the stream.</p>
 *
 * <p>New consumers start from offset {@code 0} (full history). Offsets are
 * replicated through Raft so they survive node failures.</p>
 *
 * <p>The response includes an {@code X-Stream-Offset} header indicating the
 * absolute offset of the returned message.</p>
 *
 * @see io.dist.model.StreamConsumerOffset
 * @see io.dist.storage.StreamBuffer
 */
@Path("/api/stream")
@Produces(MediaType.APPLICATION_JSON)
public class StreamResource {
    private static final Logger LOG = Logger.getLogger(StreamResource.class);

    @Inject
    StorageService storageService;

    @Inject
    RaftService raftService;

    /**
     * Polls the next unread message from a STREAM queue for the given consumer.
     *
     * <ol>
     *   <li>Validates that the queue exists and is of type {@link QueueType#STREAM}.</li>
     *   <li>Loads (or creates) the {@link StreamConsumerOffset} for
     *       {@code (consumerId, queueName)}.</li>
     *   <li>Peeks at the message at the consumer's current offset in the
     *       {@link StreamBuffer}.</li>
     *   <li>If found, advances the offset by 1 (replicated through Raft) and
     *       returns the message with HTTP 200 and an {@code X-Stream-Offset} header.</li>
     *   <li>If the consumer is at the end of the stream, returns HTTP 204.</li>
     * </ol>
     *
     * @param queueName  the STREAM queue to read from
     * @param consumerId the consumer's unique identifier
     * @return the next message, or 204 if the stream is empty at this offset
     */
    @GET
    @Path("/{queue}")
    public Uni<Response> pollStream(
            @PathParam("queue") String queueName,
            @QueryParam("consumerId") String consumerId) {

        if (consumerId == null || consumerId.isBlank()) {
            return Uni.createFrom().item(
                    Response.status(Response.Status.BAD_REQUEST)
                            .entity("{\"error\":\"consumerId query parameter is required\"}")
                            .build());
        }

        return Uni.createFrom().item(() -> {
            // Validate queue exists and is STREAM type
            Queue queue = QuarkusTransaction.requiringNew().call(() -> Queue.findById(queueName));
            if (queue == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("{\"error\":\"Queue not found: " + queueName + "\"}")
                        .build();
            }
            if (queue.queueType != QueueType.STREAM) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("{\"error\":\"Queue '" + queueName + "' is not a STREAM queue\"}")
                        .build();
            }

            // Load consumer offset (creates at 0 if new consumer)
            long currentOffset = QuarkusTransaction.requiringNew().call(() -> {
                StreamConsumerOffset record = StreamConsumerOffset.getOrCreate(consumerId, queueName);
                return record.nextOffset;
            });

            // Peek at the message at the current offset
            StreamBuffer buf = storageService.getStreamBuffer(queueName);
            Message msg = buf.peekAt(currentOffset);

            if (msg == null) {
                // Consumer is at the end of the stream
                return Response.noContent().build();
            }

            // Advance offset and replicate through Raft
            long newOffset = currentOffset + 1;
            try {
                raftService.replicateUpdateStreamOffset(consumerId, queueName, newOffset)
                        .await().indefinitely();
            } catch (Exception e) {
                LOG.errorf("Failed to replicate stream offset update for consumer %s on queue %s: %s",
                        consumerId, queueName, e.getMessage());
                // Continue — message was still read; offset may be re-replicated on next poll
            }

            LOG.infof("Stream consumer '%s' polled offset %d from queue '%s'", consumerId, currentOffset, queueName);

            return Response.ok(msg)
                    .header("X-Stream-Offset", String.valueOf(currentOffset))
                    .build();

        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }
}
