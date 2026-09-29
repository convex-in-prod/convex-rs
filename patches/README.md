# Maintained Rust client patches

The source branch carries two patches over the reviewed upstream base. The orphan
`patch-history` branch records their exact upstream commit, ordered mail patches,
source IDs and reconstructed result tree.

## Stable reconnect identity

One client worker retains its session ID across reconnects and resends outstanding
mutations with their original request IDs. Separate clients retain independent
sessions. The server's successful-result lookup determines replay behavior; a stable
identity does not add a new client retry policy or extend server retention.

Upstream proposal: [PR #17](https://github.com/get-convex/convex-rs/pull/17).

## Ordered observation and joined shutdown

`ConvexClientBuilder::with_observer` installs a callback before background tasks
start. It exposes received query-set transitions, connection states and terminal
closure. Query callbacks are ordered; connection callbacks can run concurrently.
The callback must not block or panic. Query snapshots remain consistent views and
can already have skipped database transactions on the server.

`ConvexClient::close` closes all clones and joins the worker and transport. Canceling
one caller does not cancel shared shutdown. Dropping the last client requests
cancellation without waiting. Already buffered subscription results may still be
read, and closing cannot undo a mutation submitted to the server.

A closed protocol response channel terminates the query worker and its pending
requests even while client clones remain alive. Shared shutdown joins the transport
and retains a transport panic for `close` to report. The transport also terminates
when its request stream closes, including while waiting for reconnect acknowledgement.

Successful mutations complete only after a query transition reaches their commit
timestamp. Plain and structured mutation errors, including identified terminal OCC,
complete immediately and leave the reconnect replay set. A late repeated mutation
response after completion does not close the usable connection; IDs not yet issued
and mismatched active request types remain protocol errors.

Terminal worker progress handling adapts
[PR #16](https://github.com/get-convex/convex-rs/pull/16) at
`8c0e01c56b4f7f507595ec2eddee22f19fc5e7d4`. Completion ordering and late duplicate
response handling were authored independently.

The existing connection-state channel is best effort and cannot block reconnect or
shutdown when full. Applications that need every received transition can use the
observer and explicitly handle their own bounded queues and lifetime fencing.

## Verification and distribution

Local WebSocket fixtures cover ordered observation, slow/full notification consumers,
shutdown ownership, terminal transport failure, mutation completion ordering, late
repeated responses, terminal OCC and reconnect replay identity. They do not run a real
backend or prove transaction retention or server deduplication retention. The upstream package's testing feature references
omitted generated testing modules; the focused fixtures were also exercised through
a downstream harness importing the actual SDK sources.

Consumers may pin the reviewed source revision or retain an immutable source
snapshot. These patches need no crates.io version change, registry publication or
binary release. Preserve existing PR branches and published source references when
updating the maintained source train. Regenerate patch-history with the canonical
updater and verify it using that branch's `scripts/apply.sh`.
