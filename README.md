# BatchAgent

Portfolio reference implementation of a 2020 project, built to show a small, secure agent that runs batch jobs on in-store back-office servers on behalf of the central scheduler (**DagRunner**, see `dagrunner/`).

**Stack:** Java 11, gRPC 1.34 over Netty (shaded), Protocol Buffers, mutual TLS, JUnit 5.

## Why
Stores sit behind flaky WAN links, and their jobs (inventory export, price sync, label printing feeds) have to run *on* the store server. The old approach was SSH plus cron, with no visibility into what ran. This agent replaces it with:

- **A typed RPC API** (`src/main/proto/agent.proto`):
  - `RunJob` streams `Started`, then `OutputLine`s, then `Finished`.
  - `CancelJob` and `ListJobs`.
  - A separate `Registry.Heartbeat` service that the agent dials out to.
- **mTLS everywhere:**
  - The server requires a client certificate signed by the internal CA, over TLS 1.3/1.2 only.
  - `ClientCertAuthInterceptor` maps the certificate CN to allowed methods. `dagrunner-scheduler` can run jobs; `monitoring` can only list them.
  - Plaintext or cert-less calls get `UNAUTHENTICATED`.
- **Safe execution** (`JobManager`):
  - Only allow-listed commands run, mapped from name to an absolute path. The caller never sends a shell string.
  - Environment variables are validated, and `LD_PRELOAD`, `PATH` and similar are blocked.
  - Each job gets its own working directory.
  - Timeouts are bounded, and a concurrency cap prevents overload.
  - Timeout or cancel sends SIGTERM to the whole process tree, then SIGKILL after a grace period.
- **Survives disconnects:** a job outlives the RPC that started it. Re-sending `RunJob` with the same `job_id` re-attaches, replays the buffered output, and continues streaming. It doesn't start a second copy, so the scheduler can retry blindly.
- **Fleet control through heartbeats:**
  - The registry sets the heartbeat interval and can put the agent into **drain** mode before maintenance (new jobs are rejected, running ones finish).
  - Failed heartbeats back off exponentially with jitter, so a registry outage doesn't end in a thundering herd.
- **Operations:**
  - The gRPC health service and reflection are enabled (`grpcurl` works).
  - Shutdown is graceful.
  - Builds as a single shaded jar.

## Configure
`/etc/batchagent/agent.properties`:
```properties
store.id=S042
listen.port=7443
registry.address=registry.internal:7443
tls.cert=/etc/batchagent/tls/agent.crt
tls.key=/etc/batchagent/tls/agent.key
tls.ca=/etc/batchagent/tls/ca.crt
jobs.max-concurrent=2
commands.inventory_export=/opt/batch/bin/inventory_export
commands.price_sync=/opt/batch/bin/price_sync
acl.dagrunner-scheduler=*
acl.monitoring=ListJobs
```

## Build and test
```bash
mvn package     # generates gRPC stubs, runs tests, builds target/batchagent-1.0.0.jar
java -jar target/batchagent-1.0.0.jar /etc/batchagent/agent.properties
```
Tests cover:
- Real child processes: streaming, exit codes, timeouts, cancel, re-attach and replay, concurrency, drain.
- The gRPC service over the in-process transport.
- The auth interceptor.
- Heartbeat drain and backoff.

Process tests run on Linux and macOS (the jobs are shell scripts).

## Dev certificates
```bash
./scripts/dev-certs.sh   # CA + agent cert + scheduler client cert (CN=dagrunner-scheduler)
```
