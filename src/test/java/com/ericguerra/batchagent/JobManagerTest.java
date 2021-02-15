package com.ericguerra.batchagent;

import com.ericguerra.batchagent.jobs.JobManager;
import com.ericguerra.batchagent.jobs.JobManager.Outcome;
import com.ericguerra.batchagent.jobs.JobManager.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@DisabledOnOs(OS.WINDOWS) // jobs are shell scripts, as on the store servers
class JobManagerTest {
    @TempDir Path tmp;
    JobManager jobs;

    static class Capture implements JobManager.Listener {
        final List<String> lines = new CopyOnWriteArrayList<>();
        final CompletableFuture<Outcome> done = new CompletableFuture<>();
        volatile boolean reattached;
        @Override public void started(long pid, long startedAtMs, boolean reattached) { this.reattached = reattached; }
        @Override public void output(long seq, boolean stderr, String text) { lines.add((stderr ? "E:" : "") + text); }
        @Override public void finished(Outcome o) { done.complete(o); }
        Outcome await() throws Exception { return done.get(20, TimeUnit.SECONDS); }
    }

    Path script(String name, String body) throws Exception {
        Path p = tmp.resolve(name);
        Files.writeString(p, "#!/bin/sh\n" + body + "\n");
        Files.setPosixFilePermissions(p, PosixFilePermissions.fromString("rwxr-xr-x"));
        return p;
    }

    @BeforeEach
    void setUp() throws Exception {
        JobManager.Config c = new JobManager.Config();
        c.allowedCommands = Map.of(
                "echo_args", script("echo_args.sh", "echo \"ds=$1 store=$STORE\"; echo warn >&2"),
                "fail", script("fail.sh", "echo partial; exit 3"),
                "slow", script("slow.sh", "echo begin; sleep 30; echo never"));
        c.workRoot = tmp.resolve("work");
        c.maxConcurrent = 2;
        jobs = new JobManager(c, Clock.systemUTC());
    }

    @AfterEach
    void tearDown() { jobs.shutdown(); }

    @Test
    void runsAllowedCommandAndStreamsBothStreams() throws Exception {
        Capture c = new Capture();
        jobs.run("j1", "echo_args", List.of("2020-06-01"), Map.of("STORE", "S042"), 30, c);
        Outcome o = c.await();
        assertThat(o.status).isEqualTo(Status.SUCCEEDED);
        assertThat(c.lines).contains("ds=2020-06-01 store=S042", "E:warn");
        assertThat(tmp.resolve("work/j1")).isDirectory();
    }

    @Test
    void nonZeroExitFails() throws Exception {
        Capture c = new Capture();
        jobs.run("j2", "fail", List.of(), Map.of(), 30, c);
        Outcome o = c.await();
        assertThat(o.status).isEqualTo(Status.FAILED);
        assertThat(o.exitCode).isEqualTo(3);
    }

    @Test
    void rejectsUnknownCommandsDangerousEnvAndBadTimeouts() throws Exception {
        Capture a = new Capture();
        jobs.run("j3", "rm", List.of("-rf", "/"), Map.of(), 30, a);
        assertThat(a.await().status).isEqualTo(Status.REJECTED);
        Capture b = new Capture();
        jobs.run("j4", "echo_args", List.of(), Map.of("LD_PRELOAD", "/tmp/x.so"), 30, b);
        assertThat(b.await().message).contains("LD_PRELOAD");
        Capture c = new Capture();
        jobs.run("j5", "echo_args", List.of(), Map.of(), 0, c);
        assertThat(c.await().status).isEqualTo(Status.REJECTED);
    }

    @Test
    void timeoutKillsTheJob() throws Exception {
        Capture c = new Capture();
        jobs.run("j6", "slow", List.of(), Map.of(), 1, c);
        Outcome o = c.await();
        assertThat(o.status).isEqualTo(Status.TIMED_OUT);
        assertThat(c.lines).contains("begin").doesNotContain("never");
    }

    @Test
    void cancelStopsRunningJob() throws Exception {
        Capture c = new Capture();
        jobs.run("j7", "slow", List.of(), Map.of(), 60, c);
        Thread.sleep(300);
        assertThat(jobs.cancel("j7")).isTrue();
        assertThat(c.await().status).isEqualTo(Status.CANCELLED);
    }

    @Test
    void reattachReplaysOutputAndKeepsSingleProcess() throws Exception {
        Capture first = new Capture();
        Runnable detach = jobs.run("j8", "slow", List.of(), Map.of(), 2, first);
        Thread.sleep(300);
        detach.run(); // connection dropped
        Capture second = new Capture();
        jobs.run("j8", "slow", List.of(), Map.of(), 2, second);
        assertThat(second.reattached).isTrue();
        assertThat(second.lines).contains("begin");
        assertThat(second.await().status).isEqualTo(Status.TIMED_OUT);
        assertThat(first.done).isNotDone();
        assertThat(jobs.list()).hasSize(1);
    }

    @Test
    void enforcesConcurrencyAndDrain() throws Exception {
        jobs.run("a", "slow", List.of(), Map.of(), 60, new Capture());
        jobs.run("b", "slow", List.of(), Map.of(), 60, new Capture());
        Capture third = new Capture();
        jobs.run("c", "echo_args", List.of(), Map.of(), 60, third);
        assertThat(third.await().message).contains("busy");

        jobs.cancel("a");
        jobs.cancel("b");
        Thread.sleep(500);
        jobs.setDraining(true);
        Capture fourth = new Capture();
        jobs.run("d", "echo_args", List.of(), Map.of(), 60, fourth);
        assertThat(fourth.await().message).contains("draining");
    }
}
