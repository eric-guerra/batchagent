package com.ericguerra.batchagent.jobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;

/**
 * Owns child processes. Jobs outlive the gRPC call that started them: if the scheduler's connection
 * drops (store WAN links do), the job keeps running and a later RunJob with the same id re-attaches,
 * replaying buffered output first.
 */
public class JobManager {
    public interface Listener {
        void started(long pid, long startedAtMs, boolean reattached);
        void output(long seq, boolean stderr, String text);
        void finished(Outcome outcome);
    }

    public enum Status { SUCCEEDED, FAILED, TIMED_OUT, CANCELLED, REJECTED }

    public static final class Outcome {
        public final Status status;
        public final int exitCode;
        public final long durationMs;
        public final String message;

        public Outcome(Status status, int exitCode, long durationMs, String message) {
            this.status = status;
            this.exitCode = exitCode;
            this.durationMs = durationMs;
            this.message = message;
        }
    }

    public static final class Config {
        public Map<String, Path> allowedCommands = Map.of();
        public Path workRoot = Path.of("/var/lib/batchagent/jobs");
        public int maxConcurrent = 2;
        public int replayLines = 2000;
        public int maxTimeoutSeconds = 6 * 3600;
        public java.time.Duration finishedRetention = java.time.Duration.ofHours(6);
    }

    private static final Set<String> BLOCKED_ENV = Set.of("LD_PRELOAD", "LD_LIBRARY_PATH", "PATH", "JAVA_TOOL_OPTIONS");

    private final Config config;
    private final Clock clock;
    private final ConcurrentMap<String, Job> jobs = new ConcurrentHashMap<>();
    private final ExecutorService ioThreads = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "job-io");
        t.setDaemon(true);
        return t;
    });
    private final ScheduledExecutorService timers = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean draining;

    public JobManager(Config config, Clock clock) {
        this.config = config;
        this.clock = clock;
        timers.scheduleAtFixedRate(this::evictFinished, 1, 1, TimeUnit.MINUTES);
    }

    /** Start (or re-attach to) a job. Returns a handle the caller uses to detach. */
    public Runnable run(String jobId, String command, List<String> args, Map<String, String> env, int timeoutSeconds, Listener listener) {
        Job existing = jobs.get(jobId);
        if (existing != null) return existing.attach(listener, true);

        String reject = validate(jobId, command, env, timeoutSeconds);
        if (reject != null) {
            listener.finished(new Outcome(Status.REJECTED, -1, 0, reject));
            return () -> {};
        }
        Job job = new Job(jobId, command);
        Job raced = jobs.putIfAbsent(jobId, job);
        if (raced != null) return raced.attach(listener, true);
        Runnable detach = job.attach(listener, false);
        try {
            job.start(config.allowedCommands.get(command), args, env, timeoutSeconds);
        } catch (IOException e) {
            job.finish(new Outcome(Status.FAILED, -1, 0, "could not start: " + e.getMessage()));
        }
        return detach;
    }

    private synchronized String validate(String jobId, String command, Map<String, String> env, int timeoutSeconds) {
        if (draining) return "agent is draining for maintenance";
        if (jobId == null || !jobId.matches("[A-Za-z0-9_.:-]{1,128}")) return "invalid job id";
        if (!config.allowedCommands.containsKey(command)) return "command not allowed: " + command;
        for (String k : env.keySet()) if (BLOCKED_ENV.contains(k) || !k.matches("[A-Z_][A-Z0-9_]*")) return "env var not allowed: " + k;
        if (timeoutSeconds <= 0 || timeoutSeconds > config.maxTimeoutSeconds) return "timeout must be 1.." + config.maxTimeoutSeconds + "s";
        if (runningCount() >= config.maxConcurrent) return "agent busy (" + config.maxConcurrent + " jobs running)";
        return null;
    }

    public boolean cancel(String jobId) {
        Job j = jobs.get(jobId);
        if (j == null) return false;
        j.cancel();
        return true;
    }

    public long runningCount() {
        return jobs.values().stream().filter(j -> j.outcome == null).count();
    }

    public List<Map<String, Object>> list() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Job j : jobs.values()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("jobId", j.id);
            m.put("command", j.command);
            m.put("state", j.outcome == null ? "RUNNING" : j.outcome.status.name());
            m.put("startedAtMs", j.startedAtMs);
            out.add(m);
        }
        return out;
    }

    public void setDraining(boolean d) { draining = d; }
    public boolean isDraining() { return draining; }

    private void evictFinished() {
        long cutoff = clock.millis() - config.finishedRetention.toMillis();
        jobs.values().removeIf(j -> j.outcome != null && j.finishedAtMs < cutoff);
    }

    public void shutdown() {
        jobs.values().forEach(Job::cancel);
        timers.shutdownNow();
        ioThreads.shutdownNow();
    }

    private final class Job {
        final String id;
        final String command;
        final List<String[]> replay = new ArrayList<>(); // [seq, stderr, text]
        final List<Listener> listeners = new CopyOnWriteArrayList<>();
        Process process;
        long seq;
        long startedAtMs;
        volatile long finishedAtMs;
        volatile Outcome outcome;
        volatile boolean cancelled;
        volatile boolean timedOut;
        final CountDownLatch streamsDone = new CountDownLatch(2);

        Job(String id, String command) {
            this.id = id;
            this.command = command;
        }

        synchronized Runnable attach(Listener l, boolean reattach) {
            if (process != null) l.started(process.pid(), startedAtMs, reattach);
            for (String[] r : replay) l.output(Long.parseLong(r[0]), Boolean.parseBoolean(r[1]), r[2]);
            if (outcome != null) {
                l.finished(outcome);
                return () -> {};
            }
            listeners.add(l);
            return () -> listeners.remove(l);
        }

        void start(Path exe, List<String> args, Map<String, String> env, int timeoutSeconds) throws IOException {
            Path dir = config.workRoot.resolve(id.replace(':', '_'));
            dir.toFile().mkdirs();
            List<String> cmd = new ArrayList<>();
            cmd.add(exe.toString());
            cmd.addAll(args);
            ProcessBuilder pb = new ProcessBuilder(cmd).directory(dir.toFile());
            pb.environment().putAll(env);
            pb.environment().put("BATCH_JOB_ID", id);
            synchronized (this) {
                process = pb.start();
                startedAtMs = clock.millis();
                for (Listener l : listeners) l.started(process.pid(), startedAtMs, false);
            }
            process.getOutputStream().close();
            ioThreads.submit(() -> pump(process.getInputStream(), false));
            ioThreads.submit(() -> pump(process.getErrorStream(), true));
            ScheduledFuture<?> timer = timers.schedule(() -> { timedOut = true; kill(); }, timeoutSeconds, TimeUnit.SECONDS);
            ioThreads.submit(() -> {
                try {
                    int code = process.waitFor();
                    streamsDone.await(5, TimeUnit.SECONDS);
                    timer.cancel(false);
                    long dur = clock.millis() - startedAtMs;
                    Status s = cancelled ? Status.CANCELLED : timedOut ? Status.TIMED_OUT : code == 0 ? Status.SUCCEEDED : Status.FAILED;
                    finish(new Outcome(s, code, dur, s == Status.SUCCEEDED ? "" : "exit " + code));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        private void pump(InputStream in, boolean stderr) {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) emit(stderr, line.length() > 4000 ? line.substring(0, 4000) : line);
            } catch (IOException ignored) {
                // process killed
            } finally {
                streamsDone.countDown();
            }
        }

        private synchronized void emit(boolean stderr, String text) {
            long s = ++seq;
            replay.add(new String[]{Long.toString(s), Boolean.toString(stderr), text});
            if (replay.size() > config.replayLines) replay.remove(0);
            for (Listener l : listeners) l.output(s, stderr, text);
        }

        synchronized void finish(Outcome o) {
            if (outcome != null) return;
            outcome = o;
            finishedAtMs = clock.millis();
            for (Listener l : listeners) l.finished(o);
            listeners.clear();
        }

        void cancel() {
            cancelled = true;
            kill();
        }

        /** SIGTERM the whole tree, then SIGKILL stragglers after a grace period. */
        private void kill() {
            Process p = process;
            if (p == null || !p.isAlive()) return;
            p.descendants().forEach(ProcessHandle::destroy);
            p.destroy();
            timers.schedule(() -> {
                p.descendants().forEach(ProcessHandle::destroyForcibly);
                p.destroyForcibly();
            }, 10, TimeUnit.SECONDS);
        }
    }
}
