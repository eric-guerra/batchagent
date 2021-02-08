package com.ericguerra.batchagent;

import com.ericguerra.batchagent.grpc.AgentService;
import com.ericguerra.batchagent.grpc.ClientCertAuthInterceptor;
import com.ericguerra.batchagent.heartbeat.HeartbeatClient;
import com.ericguerra.batchagent.jobs.JobManager;
import com.ericguerra.batchagent.proto.*;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

class AgentGrpcTest {
    @TempDir Path tmp;
    final List<Server> servers = new ArrayList<>();
    final List<ManagedChannel> channels = new ArrayList<>();
    JobManager jobs;

    @AfterEach
    void tearDown() {
        channels.forEach(ManagedChannel::shutdownNow);
        servers.forEach(Server::shutdownNow);
        if (jobs != null) jobs.shutdown();
    }

    ManagedChannel serve(BindableService... services) throws Exception {
        String name = InProcessServerBuilder.generateName();
        InProcessServerBuilder b = InProcessServerBuilder.forName(name).directExecutor();
        for (BindableService s : services) b.addService(s);
        servers.add(b.build().start());
        ManagedChannel ch = InProcessChannelBuilder.forName(name).directExecutor().build();
        channels.add(ch);
        return ch;
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void runJobStreamsStartedOutputFinished() throws Exception {
        Path s = tmp.resolve("hello.sh");
        Files.writeString(s, "#!/bin/sh\necho hello $1\n");
        Files.setPosixFilePermissions(s, PosixFilePermissions.fromString("rwxr-xr-x"));
        JobManager.Config c = new JobManager.Config();
        c.allowedCommands = Map.of("hello", s);
        c.workRoot = tmp.resolve("work");
        jobs = new JobManager(c, Clock.systemUTC());

        AgentGrpc.AgentBlockingStub stub = AgentGrpc.newBlockingStub(serve(new AgentService(jobs)));
        List<JobEvent> events = new ArrayList<>();
        stub.runJob(JobRequest.newBuilder().setJobId("run-1").setCommand("hello").addArgs("store").setTimeoutSeconds(10).build())
                .forEachRemaining(events::add);

        assertThat(events.get(0).hasStarted()).isTrue();
        assertThat(events).filteredOn(JobEvent::hasOutput).extracting(e -> e.getOutput().getText()).containsExactly("hello store");
        JobEvent last = events.get(events.size() - 1);
        assertThat(last.getFinished().getStatus()).isEqualTo(Finished.Status.SUCCEEDED);

        ListJobsReply list = stub.listJobs(ListJobsRequest.getDefaultInstance());
        assertThat(list.getJobs(0).getState()).isEqualTo("SUCCEEDED");
    }

    @Test
    void rejectedJobFinishesImmediately() throws Exception {
        JobManager.Config c = new JobManager.Config();
        c.workRoot = tmp;
        jobs = new JobManager(c, Clock.systemUTC());
        AgentGrpc.AgentBlockingStub stub = AgentGrpc.newBlockingStub(serve(new AgentService(jobs)));
        Iterator<JobEvent> it = stub.runJob(JobRequest.newBuilder().setJobId("x").setCommand("nope").setTimeoutSeconds(5).build());
        Finished f = it.next().getFinished();
        assertThat(f.getStatus()).isEqualTo(Finished.Status.REJECTED);
        assertThat(it.hasNext()).isFalse();
    }

    @Test
    void interceptorRequiresClientCertificate() throws Exception {
        JobManager.Config c = new JobManager.Config();
        c.workRoot = tmp;
        jobs = new JobManager(c, Clock.systemUTC());
        ServerServiceDefinition guarded = ServerInterceptors.intercept(new AgentService(jobs),
                new ClientCertAuthInterceptor(Map.of("dagrunner-scheduler", Set.of("*"))));
        String name = InProcessServerBuilder.generateName();
        servers.add(InProcessServerBuilder.forName(name).directExecutor().addService(guarded).build().start());
        ManagedChannel ch = InProcessChannelBuilder.forName(name).directExecutor().build();
        channels.add(ch);
        // In-process transport has no SSL session, exactly like a plaintext connection would.
        assertThatThrownBy(() -> AgentGrpc.newBlockingStub(ch).listJobs(ListJobsRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));
    }

    @Test
    void heartbeatAppliesDrainAndBacksOff() throws Exception {
        JobManager.Config c = new JobManager.Config();
        c.workRoot = tmp;
        jobs = new JobManager(c, Clock.systemUTC());
        List<HeartbeatRequest> seen = new ArrayList<>();
        boolean[] fail = {false};
        RegistryGrpc.RegistryImplBase registry = new RegistryGrpc.RegistryImplBase() {
            @Override public void heartbeat(HeartbeatRequest req, StreamObserver<HeartbeatReply> out) {
                if (fail[0]) { out.onError(Status.UNAVAILABLE.asRuntimeException()); return; }
                seen.add(req);
                out.onNext(HeartbeatReply.newBuilder().setNextIntervalSeconds(45).setDrain(true).build());
                out.onCompleted();
            }
        };
        HeartbeatClient hb = new HeartbeatClient(RegistryGrpc.newBlockingStub(serve(registry)), jobs, "S042", "1.0.0");
        assertThat(hb.beatOnce()).isEqualTo(45);
        assertThat(seen.get(0).getStoreId()).isEqualTo("S042");
        assertThat(jobs.isDraining()).isTrue();

        fail[0] = true;
        int first = hb.beatOnce();
        int second = hb.beatOnce();
        assertThat(first).isBetween(5, 10);
        assertThat(second).isBetween(10, 20);
    }
}
