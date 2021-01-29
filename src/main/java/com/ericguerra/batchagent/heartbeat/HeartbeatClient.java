package com.ericguerra.batchagent.heartbeat;

import com.ericguerra.batchagent.jobs.JobManager;
import com.ericguerra.batchagent.proto.HeartbeatReply;
import com.ericguerra.batchagent.proto.HeartbeatRequest;
import com.ericguerra.batchagent.proto.RegistryGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Reports health to the central registry. The registry controls the cadence (it can slow 2,000 stores
 * down during an incident) and can put the agent into drain mode before a maintenance window.
 * Failures back off with jitter so a registry outage doesn't come back to a thundering herd.
 */
public class HeartbeatClient {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatClient.class);

    private final RegistryGrpc.RegistryBlockingStub registry;
    private final JobManager jobs;
    private final String storeId;
    private final String version;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private int failures;

    public HeartbeatClient(RegistryGrpc.RegistryBlockingStub registry, JobManager jobs, String storeId, String version) {
        this.registry = registry;
        this.jobs = jobs;
        this.storeId = storeId;
        this.version = version;
    }

    public void start() {
        timer.schedule(this::beat, 0, TimeUnit.SECONDS);
    }

    /** Visible for tests: one heartbeat, returns the delay until the next one in seconds. */
    public int beatOnce() {
        try {
            HeartbeatReply reply = registry.withDeadlineAfter(5, TimeUnit.SECONDS).heartbeat(HeartbeatRequest.newBuilder()
                    .setStoreId(storeId)
                    .setAgentVersion(version)
                    .setRunningJobs((int) jobs.runningCount())
                    .setLoadAverage(ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage())
                    .setFreeDiskBytes(new File("/").getUsableSpace())
                    .build());
            failures = 0;
            if (reply.getDrain() != jobs.isDraining()) {
                log.info("registry set drain={}", reply.getDrain());
                jobs.setDraining(reply.getDrain());
            }
            return Math.max(5, reply.getNextIntervalSeconds() == 0 ? 30 : reply.getNextIntervalSeconds());
        } catch (RuntimeException e) {
            failures++;
            int backoff = (int) Math.min(300, 5 * Math.pow(2, Math.min(failures, 6)));
            log.warn("heartbeat failed ({} in a row): {}", failures, e.getMessage());
            return backoff / 2 + ThreadLocalRandom.current().nextInt(backoff / 2 + 1);
        }
    }

    private void beat() {
        int next = beatOnce();
        if (!timer.isShutdown()) timer.schedule(this::beat, next, TimeUnit.SECONDS);
    }

    public void stop() {
        timer.shutdownNow();
    }
}
