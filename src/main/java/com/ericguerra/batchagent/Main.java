package com.ericguerra.batchagent;

import com.ericguerra.batchagent.grpc.AgentService;
import com.ericguerra.batchagent.grpc.ClientCertAuthInterceptor;
import com.ericguerra.batchagent.heartbeat.HeartbeatClient;
import com.ericguerra.batchagent.jobs.JobManager;
import com.ericguerra.batchagent.proto.RegistryGrpc;
import com.ericguerra.batchagent.tls.Tls;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.services.HealthStatusManager;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Config comes from /etc/batchagent/agent.properties:
 * <pre>
 * store.id=S042
 * listen.port=7443
 * registry.address=registry.internal:7443
 * tls.cert=/etc/batchagent/tls/agent.crt  tls.key=…  tls.ca=…
 * commands.inventory_export=/opt/batch/bin/inventory_export
 * commands.price_sync=/opt/batch/bin/price_sync
 * acl.dagrunner-scheduler=*
 * acl.monitoring=ListJobs
 * </pre>
 */
public final class Main {
    public static final String VERSION = "1.0.0";

    public static void main(String[] args) throws Exception {
        Properties p = new Properties();
        try (FileInputStream in = new FileInputStream(args.length > 0 ? args[0] : "/etc/batchagent/agent.properties")) {
            p.load(in);
        }

        JobManager.Config jc = new JobManager.Config();
        Map<String, Path> commands = new HashMap<>();
        Map<String, Set<String>> acl = new HashMap<>();
        for (String k : p.stringPropertyNames()) {
            if (k.startsWith("commands.")) commands.put(k.substring(9), Path.of(p.getProperty(k)));
            if (k.startsWith("acl.")) acl.put(k.substring(4), new HashSet<>(Arrays.asList(p.getProperty(k).split(","))));
        }
        jc.allowedCommands = commands;
        jc.workRoot = Path.of(p.getProperty("work.root", "/var/lib/batchagent/jobs"));
        jc.maxConcurrent = Integer.parseInt(p.getProperty("jobs.max-concurrent", "2"));
        JobManager jobs = new JobManager(jc, Clock.systemUTC());

        File cert = new File(p.getProperty("tls.cert"));
        File key = new File(p.getProperty("tls.key"));
        File ca = new File(p.getProperty("tls.ca"));

        HealthStatusManager health = new HealthStatusManager();
        Server server = NettyServerBuilder.forPort(Integer.parseInt(p.getProperty("listen.port", "7443")))
                .sslContext(Tls.server(cert, key, ca))
                .addService(ServerInterceptors.intercept(new AgentService(jobs), new ClientCertAuthInterceptor(acl)))
                .addService(health.getHealthService())
                .addService(ProtoReflectionService.newInstance())
                .permitKeepAliveTime(30, TimeUnit.SECONDS)
                .maxInboundMessageSize(1 << 20)
                .build()
                .start();

        ManagedChannel registryChannel = NettyChannelBuilder.forTarget(p.getProperty("registry.address"))
                .sslContext(Tls.client(cert, key, ca))
                .keepAliveTime(60, TimeUnit.SECONDS)
                .build();
        HeartbeatClient heartbeat = new HeartbeatClient(RegistryGrpc.newBlockingStub(registryChannel), jobs, p.getProperty("store.id"), VERSION);
        heartbeat.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            health.enterTerminalState();
            heartbeat.stop();
            server.shutdown();
            try {
                server.awaitTermination(20, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            jobs.shutdown();
            registryChannel.shutdownNow();
        }));
        System.out.println("batchagent " + VERSION + " listening on " + server.getPort());
        server.awaitTermination();
    }

    private Main() {}
}
