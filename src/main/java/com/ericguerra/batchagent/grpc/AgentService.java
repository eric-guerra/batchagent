package com.ericguerra.batchagent.grpc;

import com.ericguerra.batchagent.jobs.JobManager;
import com.ericguerra.batchagent.proto.*;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Map;

public class AgentService extends AgentGrpc.AgentImplBase {
    private final JobManager jobs;

    public AgentService(JobManager jobs) {
        this.jobs = jobs;
    }

    @Override
    public void runJob(JobRequest req, StreamObserver<JobEvent> out) {
        ServerCallStreamObserver<JobEvent> call = (ServerCallStreamObserver<JobEvent>) out;
        String id = req.getJobId();
        Object lock = new Object();

        JobManager.Listener listener = new JobManager.Listener() {
            boolean closed;

            @Override public void started(long pid, long startedAtMs, boolean reattached) {
                send(JobEvent.newBuilder().setJobId(id).setStarted(Started.newBuilder().setPid(pid).setStartedAtMs(startedAtMs).setReattached(reattached)).build());
            }

            @Override public void output(long seq, boolean stderr, String text) {
                send(JobEvent.newBuilder().setJobId(id).setOutput(OutputLine.newBuilder().setSeq(seq).setStderr(stderr).setText(text)).build());
            }

            @Override public void finished(JobManager.Outcome o) {
                send(JobEvent.newBuilder().setJobId(id).setFinished(Finished.newBuilder()
                        .setStatus(Finished.Status.valueOf(o.status.name()))
                        .setExitCode(o.exitCode).setDurationMs(o.durationMs).setMessage(o.message)).build());
                synchronized (lock) {
                    if (!closed && !call.isCancelled()) out.onCompleted();
                    closed = true;
                }
            }

            private void send(JobEvent e) {
                synchronized (lock) { // StreamObserver is not thread-safe; stdout and stderr pump on different threads
                    if (closed || call.isCancelled()) return;
                    out.onNext(e);
                }
            }
        };

        Runnable[] detach = new Runnable[1];
        // Client went away: stop streaming to it, but leave the job running so it can re-attach.
        call.setOnCancelHandler(() -> { if (detach[0] != null) detach[0].run(); });
        detach[0] = jobs.run(id, req.getCommand(), new ArrayList<>(req.getArgsList()), Map.copyOf(req.getEnvMap()), req.getTimeoutSeconds(), listener);
    }

    @Override
    public void cancelJob(CancelRequest req, StreamObserver<CancelReply> out) {
        out.onNext(CancelReply.newBuilder().setFound(jobs.cancel(req.getJobId())).build());
        out.onCompleted();
    }

    @Override
    public void listJobs(ListJobsRequest req, StreamObserver<ListJobsReply> out) {
        ListJobsReply.Builder b = ListJobsReply.newBuilder();
        for (Map<String, Object> j : jobs.list()) {
            b.addJobs(JobSummary.newBuilder()
                    .setJobId((String) j.get("jobId"))
                    .setCommand((String) j.get("command"))
                    .setState((String) j.get("state"))
                    .setStartedAtMs((Long) j.get("startedAtMs")));
        }
        out.onNext(b.build());
        out.onCompleted();
    }
}
