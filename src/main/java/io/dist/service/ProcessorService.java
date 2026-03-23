package io.dist.service;

import io.dist.cluster.RaftService;
import io.dist.model.ProcessorStatus;
import io.dist.model.StreamProcessor;
import io.dist.model.StreamProcessorState;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Service layer for managing stream processors.
 *
 * <p>The public {@code *Processor} methods replicate the operation through
 * Raft so all nodes apply the change. The {@code *Local} variants are called
 * by the {@link io.dist.cluster.SimpleStateMachine} on every node after the
 * log entry is committed.</p>
 */
@ApplicationScoped
public class ProcessorService {

    private static final Logger LOG = Logger.getLogger(ProcessorService.class);

    @Inject
    RaftService raftService;

    // ======================== Replicated operations ========================

    public Uni<Void> createProcessor(StreamProcessor p) {
        return raftService.replicateCreateProcessor(p).replaceWithVoid();
    }

    public Uni<Void> pauseProcessor(String id) {
        return raftService.replicatePauseProcessor(id).replaceWithVoid();
    }

    public Uni<Void> resumeProcessor(String id) {
        return raftService.replicateResumeProcessor(id).replaceWithVoid();
    }

    public Uni<Void> deleteProcessor(String id) {
        return raftService.replicateDeleteProcessor(id).replaceWithVoid();
    }

    // ======================== Local operations (called by state machine) ========================

    @Transactional
    public void createProcessorLocal(StreamProcessor p) {
        StreamProcessor existing = StreamProcessor.findById(p.id);
        if (existing == null) {
            if (p.createdAt == null) p.createdAt = LocalDateTime.now();
            p.persist();
            LOG.infof("Created processor: %s (%s)", p.name, p.id);
        }
    }

    @Transactional
    public void pauseProcessorLocal(String id) {
        StreamProcessor p = StreamProcessor.findById(id);
        if (p != null) {
            p.status = ProcessorStatus.PAUSED;
            LOG.infof("Paused processor: %s", id);
        }
    }

    @Transactional
    public void resumeProcessorLocal(String id) {
        StreamProcessor p = StreamProcessor.findById(id);
        if (p != null) {
            p.status = ProcessorStatus.RUNNING;
            LOG.infof("Resumed processor: %s", id);
        }
    }

    @Transactional
    public void deleteProcessorLocal(String id) {
        StreamProcessor p = StreamProcessor.findById(id);
        if (p != null) {
            StreamProcessorState.delete("processorId", id);
            p.delete();
            LOG.infof("Deleted processor: %s", id);
        }
    }

    @Transactional
    public void updateProcessorStateLocal(String processorId, String stateKey, String stateValue) {
        StreamProcessorState state = StreamProcessorState.getOrCreate(processorId, stateKey);
        state.stateValue = stateValue;
        state.updatedAt = LocalDateTime.now();
        state.persist();
    }

    // ======================== Query operations ========================

    @Transactional
    public List<StreamProcessor> listAll() {
        return StreamProcessor.listAll();
    }

    @Transactional
    public StreamProcessor findById(String id) {
        return StreamProcessor.findById(id);
    }

    @Transactional
    public List<StreamProcessorState> getProcessorStates(String processorId) {
        return StreamProcessorState.list("processorId", processorId);
    }
}
