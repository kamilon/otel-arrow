# Native Prometheus scrape receiver

This document is the Phase 0 contract for the experimental
`urn:otel:receiver:prometheus_scrape` receiver. It is intentionally stricter
than Prometheus in several places so a scrape has bounded cost and produces
one validated OTAP result. A missing feature is an explicit error or
documented semantic drop, never a silent compatibility claim.

The first release scrapes configured static targets on a one-core pipeline.
It does not implement service discovery, Target Allocator assignment,
Prometheus configuration import, protobuf exposition, native histograms, or
cross-process ownership. The receiver uses native OTAP metric records and
existing exporters; it does not construct an SDK metric tree or use a
Prometheus exporter runtime.

## Pinned references

These references define the initial fixture corpus. A protocol or
compatibility change requires a fixture update and a documented semantic
decision:

- Prometheus text exposition: `prometheus/docs` commit
  [`c42ff584469ae1d6f7498788365d20ed6bb6e772`](https://github.com/prometheus/docs/blob/c42ff584469ae1d6f7498788365d20ed6bb6e772/docs/instrumenting/exposition_formats.md),
  text format version `0.0.4`.
- OpenMetrics text: [OpenMetrics `v1.0.0`](https://github.com/prometheus/OpenMetrics/blob/v1.0.0/specification/OpenMetrics.md).
- Prometheus-to-OTLP compatibility: `opentelemetry-specification`
  commit [`44eb1b5a1077a578db856c81af0a15abe25128f8`](https://github.com/open-telemetry/opentelemetry-specification/blob/44eb1b5a1077a578db856c81af0a15abe25128f8/specification/compatibility/prometheus_and_openmetrics.md).
  Sections marked Development are guidance, not Stable guarantees.
- Test-only implementation comparison: Prometheus server `v3.5.0` and
  Collector Contrib `v0.130.0`, where available. Neither implementation
  overrides the pinned format or OTLP semantic contract.

The feature is negotiated and parsed as two separate text dialects. Prometheus
text sample timestamps are integer milliseconds; OpenMetrics timestamps are
decimal seconds. OpenMetrics requires exactly one terminal `# EOF` marker and
no non-whitespace content after it.

## Configuration and capability contract

The native configuration is strict and rejects unknown fields. Configuration
is compiled into immutable per-target plans before any network activity.
Proposed shape (field names are contract examples until Phase 1 adds the
typed schema):

```yaml
receivers:
  prometheus_scrape:
    scrape_interval: 60s
    scrape_timeout: 10s
    targets:
      - endpoint: http://127.0.0.1:9090/metrics
        job_name: local
        labels:
          environment: dev
```

Targets are static URLs. URL user information and fragments are rejected.
Only `http` and verified `https` are supported; redirects, environment
proxies, automatic retries, and insecure TLS verification are disabled.
Authentication is configured outside the URL. Credentials and custom headers
are bounded and redacted.

`job_name` is required and nonempty. The default scrape interval is 60s and
the timeout is 10s; both must be positive, timeout must not exceed interval,
and connect timeout must not exceed scrape timeout. If target relabeling does
not set `instance`, its value is the normalized target authority. A target
whose labels are entirely removed by target relabeling is omitted before
claiming or making a request.

The shared reqwest settings were audited: they provide connect/request
timeouts, concurrency settings, redirects/proxy controls, and per-host idle
pool controls, but do not expose a configurable HTTP parser-buffer or streamed
body-frame cap. The receiver therefore uses `hyper-util` directly, which lets
it set both HTTP/1 header-count and buffer limits and avoids automatic
redirect/proxy/decompression behavior. Reuse the existing TLS configuration
schema and construct a verified Rustls connector for this transport; do not
reuse the reqwest-specific client builder. TLS/provider acceptance remains a
Phase 3 test gate for every declared provider. Use one client per receiver,
disable idle pooling, and rely on the two-slot scheduler to cap active
connections; never construct a client per scrape.

The initial transport advertises only formats implemented by the running
feature stage. Phase 4A advertises Prometheus text `0.0.4` with UTF-8 names;
Phase 4B may add OpenMetrics `1.0.0`. A target that returns an unsupported
format, protobuf, another version, or a non-identity `Content-Encoding` fails
that scrape. Phase 4A sends
`Accept: text/plain; version=0.0.4; escaping=allow-utf-8` and
`Accept-Encoding: identity`; Phase 4B adds the OpenMetrics v1.0.0 media type.
An absent `Content-Type` is interpreted as Prometheus text `0.0.4`; a present
but malformed or unsupported type is an error. UTF-8 charset may be omitted
or specified explicitly. For Prometheus text, the `escaping` parameter may be
omitted or set to `allow-utf-8`; other escaping schemes fail. OpenMetrics
uses its v1.0.0 name grammar without an escaping parameter. Any other
parameter fails.
`Accept` never advertises an unimplemented parser/projection combination.

Phase 4A requires an explicit `TYPE gauge` or `TYPE counter`; it rejects
missing/unknown types, compound families, OpenMetrics, target-info enrichment,
and configured metric relabeling with zero publication. Later phases add
format parsing, family translation, relabeling, and direct OTAP projection.
Each capability is enabled only after its parser, semantic translation, and
builder path are all implemented.

For Prometheus text, accept the `allow-utf-8` escaping scheme and reject
`underscores` and `dots` escaping, whose normalization can make distinct
names collide. OpenMetrics quoted names follow its v1.0.0 grammar. ASCII names
are unchanged. Do not rewrite metric names except where structural suffixes
are consumed to assemble histogram and summary families.

The supported metric-relabel actions are `keep`, `drop`, `replace`,
`labelkeep`, and `labeldrop`. Compile with the Rust `regex` crate, which does
not use unbounded backtracking. Cap each pattern at 4 KiB, each compiled
regex/DFA at 64 KiB, and all pattern/source bytes at 64 KiB per target.
Unsupported actions and malformed/oversized regex programs fail
configuration. The crate is dual-licensed MIT/Apache-2.0, compatible with
this repository's Apache-2.0 license; the fixture test compiles a nested
quantifier pattern under the selected limits. Phase 1 promotes it from a
test-only dependency when receiver relabeling is implemented.
Replacement output is checked against label byte caps before it is retained.
Target relabeling is compiled before scheduling; metric relabeling runs after
syntax/type validation but before family assembly or OTAP allocation.
Structural labels and typed family identity cannot be mutated incompatibly.

## Identity, timestamps, and projection

### Target and source identity

Parse each URL with the workspace `url` crate. Canonical request identity
lowercases scheme and DNS host, normalizes IP literals and default ports, and
uses the serialized normalized path and query. Query order remains significant.
Apply target relabeling before constructing the final request identity.
Fragments and user information are rejected rather than normalized away.
The same canonical request with different output-only job names, labels, or
scrape intervals is still the same scrape source.

The process-local source claim compares canonical request bytes, not only a
hash. It also includes representation-affecting authentication, explicit
request headers, and a conservative TLS-client profile identity. Fingerprints
are internal and never exposed. Claims are acquired for all targets before
starting any request and live until their tasks and publication are settled.
DNS aliases and distinct URLs serving the same data are not deduplicated;
cross-process duplication remains an operator responsibility.

Output identity is separate from source identity. Final `job_name` becomes
resource `service.name`; final `instance` becomes `service.instance.id`; the
endpoint contributes `server.address`, `server.port`, and `url.scheme`.
Identity fields are resource attributes, not repeated point labels.
`otel_scope_name`, `otel_scope_version`, and `otel_scope_schema_url` labels
define scope metadata and are removed from point attributes. Other
`otel_scope_*` labels become scope attributes. A stable receiver scope is
used when the labels are absent.

Target labels win by default when they collide with sample labels; the sample
label is retained under `exported_<name>`. If that name already exists,
append `_1`, `_2`, and so on, choosing the first unused name after considering
all target and sample labels. `honor_labels` is an explicit opt-in: sample
labels win and the colliding target label is discarded. Resource identity
conflicts after relabeling fail the scrape.

### Target information and sample names

Enrich resource attributes only from either:

1. an OpenMetrics `target` family of type `info`; or
2. a Prometheus-text `target_info` gauge whose sample value is exactly `1`.

An arbitrary `_info` metric, an absent/other type, or a differently named
family is not target information. The recognized sample is omitted from
metric output. Its final labels are applied to the whole scrape even if it
appears late. Equal duplicate attributes coalesce; conflicting values,
multiple non-identical target-info points, or conflicts with established
target identity fail the scrape. Metric relabeling happens first, so dropping
the target-info sample prevents enrichment.

Preserve the exposition sample name for gauges and counters, including a
literal `_total` suffix. Use `# TYPE` to determine the data type and store it
as metric metadata `prometheus.type`; HELP is the OTAP description. UNIT maps
the pinned commonly used unit words to UCUM abbreviations; unknown units are
preserved unchanged. Counters become cumulative monotonic OTAP Sums. Do not
infer resets across scrapes, synthesize zeroes, or add a start time in the
first scalar milestone; a counter decrease on a later scrape is emitted
unchanged. A valid `_created` component supplies the start timestamp when
created-sample support lands. Gauges become OTAP Gauges; untyped/unknown
families are not inferred from their names; unknown types map to Gauges under
the pinned compatibility rule. OpenMetrics Info other than target info
becomes a non-monotonic Sum of ones.

Classic histogram structural `_bucket`, `_sum`, and `_count` samples assemble
under the family name after removing `le`; the `+Inf` bound is omitted from
explicit bounds but its count is retained. Summary `_sum` and `_count`
assemble under the family name after removing `quantile`. Missing required
count drops that family point with a semantic-drop reason; malformed,
contradictory, or over-budget input fails the entire unpublished scrape.
Optional histogram sum is unset; optional summary sum follows the pinned
compatibility rule and is zero. Duplicate boundaries, count inconsistencies,
overflow, and post-relabel collisions are rejected.

Before filtering, validate each complete exposition according to its
dialect: OpenMetrics totals/counts are nonnegative and non-NaN, histogram
counts are integral with a `+Inf` bucket, histogram sums obey the negative
threshold rule, summaries have valid counts/sums/quantiles, StateSet values
are 0 or 1, and timestamped counter-valued sequences are monotonic except
for spec-permitted resets with valid Created semantics. Prometheus-text
validity differs only where its pinned format permits it. Validate raw
components before metric relabeling, then assemble/collision-check retained
components afterward.

Prometheus-text duplicate series in one exposition are invalid. OpenMetrics
may repeat a series only with distinct, monotonically increasing timestamps.
Reject duplicates before and after relabeling rather than summing unrelated
samples or applying last-write-wins. The bounded duplicate table is charged
to the family arena.

- **Finite histogram bucket:** Keep a coarsened histogram and difference
  remaining cumulative counts.
- **Histogram `+Inf` bucket or `_count`:** Drop the affected point as
  incomplete.
- **Histogram `_sum`:** Keep the histogram with sum unset.
- **Summary quantile:** Keep remaining quantiles, including none.
- **Summary `_count` / `_sum`:** Missing count drops the point; missing sum
  uses zero.
- **Counter `_created`:** Keep the point with start time unset.
- **Entire family point:** Drop before OTAP allocation and count as filtered,
  not malformed.

Filtering never exempts input from syntax, timestamp, or global resource
limits. Grouping and duplicate checks use final labels after relabeling.

Prometheus sample timestamps convert from integer milliseconds and
OpenMetrics timestamps from decimal seconds using checked integer arithmetic,
never float-based nanosecond rounding. Missing sample timestamps use one
recorded scrape timestamp. An absent exemplar timestamp inherits the
associated sample's effective timestamp. Unrepresentable negative or
overflowing OTAP timestamps fail the scrape.

OpenMetrics exemplars with valid trace/span IDs are projected with their
filtered labels. Invalid IDs follow the pinned compatibility rule and are
dropped as exemplars, not as samples. StateSet is explicitly unsupported and
reported as a semantic drop; GaugeHistogram is dropped with its own semantic
reason, not treated as an ordinary gauge. Protobuf, native histograms, and
textual stale-marker interpretation are unsupported; an ordinary textual
`NaN` remains a NaN gauge value. Missing series emit nothing.

The receiver does not generate `up`, `scrape_duration_seconds`,
`scrape_samples_scraped`, `scrape_samples_post_metric_relabeling`, or
`scrape_series_added`. Health and counts belong to receiver telemetry and
target state, not the ingested sample stream. Failure publishes no synthetic
health metric.

## Narrow runtime and test seams

The clock seam supplies a monotonic tick for deadlines/scheduling and a wall
clock Unix-nanosecond value recorded once per scrape. Production uses the
system clock; scheduler tests inject both values and never sleep.

The HTTP-source seam accepts one immutable target request plan and yields a
response head followed by bounded byte frames. It does not own retries,
redirects, decoding, scheduling, or target identity. Dropping its local
request future cancels work. Production uses the bounded Hyper transport;
tests use an ephemeral loopback peer or a deterministic source fake. These
interfaces stay receiver-specific rather than becoming a generic service
framework.

## Scheduling, ownership, and publication

The initial placement guard requires exactly one allocated pipeline core.
There is at most one active scrape per target and at most two receiver-wide
work slots. Each target has one due entry; missed intervals coalesce and
never queue catch-up scrapes. Intervals use deterministic staggering.
Admission pauses as soon as a downstream publication send becomes pending
or full, even if work slots remain. Admission resumes after pending
publication pressure clears, at a future interval.

The scrape timeout covers DNS, connect, TLS, headers, body, parse, and build.
It does not cover publication. A fully validated result waiting on a
downstream send retains its slot and budget until accepted, channel
failure/closure, or a drain/shutdown deadline. It is not dropped at the
normal scrape timeout and is not periodically re-scraped.

Parsing and building withhold output until the whole response, metadata,
families, and target enrichment validate. A syntax, transport, or limit error
publishes none of the scrape. Multiple native batches publish in order after
validation. Publication is not transactional: if channel failure or a
lifecycle deadline occurs after some batches were enqueued, those batches
remain visible, unsent batches are discarded, and status reports partial
publication without retry or rollback claims.

Drain closes admission and cancels future due entries. Admitted fetch/parse/
build work settles by the earlier of scrape and drain deadlines. Validated
publication waits until accepted or the drain/shutdown deadline. The
receiver-drained notification is sent only after local work and source
claims are settled. Synchronous Arrow finish and decoder calls are not
preemptible; their input quanta are bounded and their real-clock latency is
qualified by a runtime watchdog. This is a tolerance, not a hard real-time
guarantee.

## Limits and allocation accounting

Every limit is checked before work or memory growth. A response-byte limit is
not an RSS limit. The configured ceilings below are initial contract defaults;
changing one requires updating its boundary test and the receiver-owned memory
envelope.

- **Configuration:** 1 MiB total. Reject before deserialization or plan
  compilation.
- **Targets and jobs:** 1,000 each. Reject at validation; compile no partial
  plan.
- **Concurrent work:** two receiver-wide slots, one per target. A slot
  includes fetch, parse, build, and blocked publication.
- **Request credentials and headers:** 32 custom headers and 16 KiB combined
  header bytes; 4 KiB maximum per value; 8 KiB maximum bearer token. TLS
  certificate/key files are limited to 1 MiB each when read.
- **HTTP protocol:** HTTP/1.1, at most 64 response headers and an 8 KiB
  parser buffer per active connection. The bounded `hyper-util` client
  exposes both caps. `reqwest` 0.13.5 exposes a header-count cap but not a
  configurable parser-buffer/body-frame cap, so the receiver uses
  `hyper-util` directly. Keep at most two active connections.
- **Body:** 16 MiB of actual response-body bytes. Count streamed bytes, not
  `Content-Length`; reject before retaining a chunk that crosses the limit.
  Request identity encoding and reject any other content encoding.
- **Stream frame:** At most 8 KiB per HTTP/1 parser buffer. Process each
  frame before requesting the next. The transport probe verifies the
  configured cap against a loopback peer sending a much larger body frame.
- **Non-yielding work:** Parse at most 8 KiB before yielding; finish at most
  8,192 datapoints per native Arrow batch. Runtime tests must qualify the
  synchronous finish quantum and control responsiveness.
- **Line and metadata:** 64 KiB per line and 10,000 families. Check while
  scanning bytes, before allocating a line or family entry; cap metric names
  at 256 bytes and HELP/UNIT metadata at 16 KiB each.
- **Samples and series:** 200,000 input samples and 100,000 retained series
  per scrape. Count filtered samples; reject the first item over either cap.
- **Labels:** 64 per sample; 256 bytes per name; 4 KiB per value; 16 KiB
  aggregate bytes per sample. Validate before copying or relabeling.
- **Compound samples:** 200,000 bucket/quantile components and 10,000
  exemplars per scrape; at most 1,024 buckets or quantiles and 64 exemplars
  per point.
- **Relabel programs:** 64 target actions and 128 metric actions; 64 KiB
  compiled pattern/source bytes per target. Validate and compile once; use
  no unbounded-backtracking regex engine.
- **Family arena:** 16 MiB per work slot. Charge keys, strings, duplicate
  tracking, and pre/post-filter assembly state before insertion.
- **Staged OTAP result:** 32 MiB per work slot, including builder/finish
  overlap. Charge before append and retain through blocked publication.
  Cap at 1,024 batches; split before `u16` metric/resource/scope or `u32`
  point ID overflow; fail an individually oversized point.
- **Diagnostics and state:** 1 KiB per target x 1,000 targets; 512-byte
  error; bounded pagination. Store opaque IDs and sanitized errors only.

The receiver-owned allocation envelope is therefore at most
`2 x (16 KiB transport/parser workspace + 16 MiB family arena + 32 MiB
staged OTAP)` plus bounded plans, target state, HTTP client state, and
allocator overhead. HTTP body bytes are streamed and are not retained as a
second full-body buffer. TCP socket/kernel buffers, TLS provider buffers,
runtime tasks, and downstream channel payloads are separate process-level
terms; the receiver does not claim an exact RSS ceiling.

Phase 0 probe observations on `aarch64-apple-darwin`:

- The direct Hyper client rejected a 65-header response and a 512 KiB
  response header with limits set to 64 headers and an 8 KiB parser buffer.
  A 1 MiB chunked body arrived in frames no larger than 8 KiB.
- For 65,536 number points in 8,192-row batches and 1,024 unique attribute
  keys, retained Arrow arrays used 2,592,984 bytes and measured peak live
  allocation was 2,649,404 bytes (about 2.2% above retained buffers). The
  output key dictionary used `UInt16`, confirming the index upgrade.
- Across repeated runs, synchronous finish measured about 0.5 to 2.5 ms for
  one 8,192-point batch and 0.14 to 0.31 ms for the 1,024-key attribute batch.

These are feasibility measurements for the exercised shapes, not portable
latency guarantees or proof of the proposed charge for every schema. The
precharge and 8,192-row limits remain subject to the later boundary and
runtime-watchdog tests.

Arrow builders do not expose reservation/accounting. The initial charging
policy therefore uses checked conservative precharges before every append,
not post-finish `get_array_memory_size()` as a substitute. For each append,
charge four times the fixed-width and variable payload bytes plus 128 bytes
per row and 128 bytes per new dictionary entry. This covers geometric buffer
growth and simultaneous builder/finish/dictionary conversion storage with
slack for offsets, validity, indices, and dictionary lookup overhead. Keep
builder and finished-batch charges live simultaneously. Reject before the
charge would exceed the per-slot cap. The Phase 0 allocator probe
exercises 65,536 number points in batches of 8,192 plus 1,024 unique
attribute keys, forcing the `UInt8` to `UInt16` dictionary upgrade. It
records append/finish high water, retained Arrow bytes, and real-clock
synchronous finish durations; later builder/runtime tests must show this
charge dominates every admitted append shape and that the 8,192-row
quantum meets the drain-latency budget. If not, tighten the charge or batch
size, or add a budget-aware pdata helper before any receiver
implementation gate passes.

## Failure, privacy, and test contract

Configuration/placement errors fail startup before network I/O. Per-target
transport, parse, or resource-limit failures are visible and do not stop
other targets. No broad catch or success-shaped fallback is permitted.
Diagnostics use opaque target IDs, bounded reason enums, and capped
sanitized messages; never log credentials, full URLs with query secrets,
response snippets, or unbounded labels.

The bounded fixture corpus lives at
`crates/core-nodes/tests/fixtures/prometheus_scrape/`. The fixture test pins
input bytes, exact semantic expectations, point multiplicity, resource/scope
identity, and absence of generated scrape series. A deterministic generator creates
bounded label-series input, including a deterministic 98% drop-heavy case, for
later parser/property/benchmark tests. Phase 4A adds the
capture/decode adapter and compares actual native pdata to these independent
goldens; production parsing never depends on golden data or test decoding.

The transport probe exercises Hyper's actual HTTP/1 limits through the
checked-in `hyper-util` dependency and a loopback peer. It demonstrates why
the shared reqwest helper is not the receiver transport: reqwest's body
stream can deliver frames larger than its internal HTTP read buffer. The
pdata allocation probe uses an isolated integration-test binary with a
counting allocator, so it can observe geometric growth, dictionary
conversion, synchronous finish, and simultaneous builder/finished-batch
retention without affecting other test binaries. Probe results are recorded
in the PR description; they are measurements, not claims that ordinary unit
tests can guarantee process RSS.

Required fixture and probe checks:

```bash
cargo test -p otap-df-core-nodes --test prometheus_scrape_fixture_contract
cargo test -p otap-df-otap --test prometheus_transport_budget_probe
cargo test -p otap-df-pdata --test prometheus_arrow_budget_probe -- --nocapture
```

The runtime, parser, and receiver-capture tests do not exist until their
respective implementation phases. Phase 0 establishes their independent
goldens and reproducible feasibility probes; it does not claim that an
unimplemented receiver passes end-to-end tests.
