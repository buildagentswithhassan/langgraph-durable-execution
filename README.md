# LangGraph Durable Execution

> A LangGraph run is a sequence of node calls. If the process holding that
> sequence dies at node three, what brings it back?

Temporal's LangGraph integration answers it by turning the graph into a
workflow. Each node becomes an activity, so its result is recorded in an event
history on the server. A worker that dies and restarts replays the history,
skips the nodes that already returned, and resumes at the one that did not.

This repo is that idea at its smallest: a two-node graph that answers a
question with an LLM, run durably, and traced end to end into Langfuse.

## Files

| File | What it holds |
|---|---|
| `nodes.py` | the node functions, the only place that touches the outside world |
| `graph.py` | the `StateGraph` and the workflow that invokes it |
| `telemetry.py` | the OpenTelemetry wiring that sends spans to Langfuse |
| `worker.py` | the process that registers the graph and runs it |
| `main.py` | the client that starts the workflow and waits for a result |

The split matters. `build_graph()` is called in `worker.py`, outside the
Temporal sandbox, and handed to `LangGraphPlugin` under the name
`hello-world`. Inside the workflow, `graph("hello-world")` looks the graph back
up by that name rather than constructing it, which keeps graph-building code
and its imports out of the sandbox's determinism rules. `nodes.py` is separate
for the same reason: the workflow module imports it under
`workflow.unsafe.imports_passed_through()` so the LLM client never enters the
sandbox.

## How a node becomes an activity

Node metadata is where the Temporal settings live:

```python
g.add_node(
    "draft_answer",
    draft_answer,
    metadata={
        "execute_in": "activity",
        "start_to_close_timeout": timedelta(seconds=60),
        "retry_policy": RetryPolicy(maximum_attempts=3),
    },
)
```

`execute_in: "activity"` is what moves the node out of the workflow and into an
activity, so it may do I/O, call a model, and be retried. Without it the node
runs inline in the workflow and inherits the sandbox's rules. Timeout and retry
policy are per node, so a cheap step and an expensive model call do not have to
share one budget. `LangGraphPlugin` also accepts `default_activity_options` for
whatever the nodes do not set themselves.

Calls go through litellm, so the provider is a config change rather than a code
change: set `LLM_MODEL` to `anthropic/claude-sonnet-4-5` or
`gemini/gemini-2.5-flash` and nothing else moves. `num_retries=0` hands retries
to Temporal's retry policy, so every attempt shows up in the Temporal UI and in
Langfuse instead of being hidden inside the client.

`HelloWorldWorkflow` runs the whole thing and reports where it is:

```python
stream = graph("hello-world").compile().astream(query, stream_mode="updates")
async for update in stream:
    for node, output in update.items():
        self.stage = f"completed: {node}"
        result = output
```

`ainvoke` would be one line, but it returns only at the end and leaves nothing
to report in between. `astream` with `stream_mode="updates"` yields one
`{node_name: output}` mapping per completed node, so `stage` advances with the
graph: `starting`, `running`, `completed: <node>`, `done`. The last update's
value is the final result.

That is what the `status` query reads. Queries read replayed workflow state and
add nothing to the history, so polling one costs nothing on the server side.

## Tracing with Langfuse

### Why the usual setup does not fit

Tracing a plain LangGraph app is one line: attach Langfuse's callback handler to
the graph, and every node and LLM call lands in one trace. That works because a
plain graph is one function call in one process, running once.

Temporal breaks all three of those assumptions:

| Assumption | What Temporal actually does |
|---|---|
| One process | The client starts the run, a worker executes it, and each node is an activity that can land on a different worker. |
| One run | Workflow code is **replayed** from event history after any crash or restart. |
| Callbacks live in memory | In-memory state does not survive the process it was created in. |

So a callback handler would produce a broken trace: the client's span and the
worker's spans would be unrelated, and every replay would re-emit spans that had
already been sent, turning one run into ten copies of itself.

### The two fixes

**1. Trace context travels in Temporal headers.**

```python
plugins=[OpenTelemetryPlugin(add_temporal_spans=True)]
```

Both `main.py` and `worker.py` connect with this. It emits spans for Temporal
operations and attaches the current trace context to the Temporal headers that
already move between client, workflow and activities. Those headers are stored
in the event history, so the parenting is durable: a worker that picks the run
back up hours later still attaches its spans to the original trace.

**2. The tracer provider is replay-safe.**

```python
provider = create_tracer_provider(resource=Resource.create({...}))
provider.add_span_processor(FilteringSpanProcessor(exporter, schedule_delay_millis=500))
trace.set_tracer_provider(provider)
```

`create_tracer_provider` comes from `temporalio.contrib.opentelemetry`, not from
the OTel SDK. It derives span IDs deterministically from workflow state and
suppresses export while a workflow is replaying. A workflow that replays ten
times still produces one clean trace, and that is what makes it safe to open a
plain OTel span (`run-graph`) inside workflow code at all.

The LLM calls themselves need nothing Temporal-specific:
`LiteLLMInstrumentor().instrument()` records each one as a span with model,
tokens and messages, which Langfuse renders as a generation.

### In short

| | Plain LangGraph | LangGraph on Temporal |
|---|---|---|
| Wiring | Langfuse callback handler | OTel + `OpenTelemetryPlugin` |
| Trace context | in-process | Temporal headers, persisted in history |
| Provider | any OTel provider | `create_tracer_provider` (replay-safe) |
| A crashed run | trace ends mid-way | trace resumes and completes |

A run comes out as one trace:

<img width="1210" height="626" alt="AI Engineer" src="https://github.com/user-attachments/assets/132c5ed5-fa6f-4dab-8f03-985602ceda94" />

Three things were worth tuning past the default wiring.

**Drop the poll spans.** `main.py` queries `status` once a second, and the
plugin emits a `QueryWorkflow` / `HandleQuery` pair for every poll. On a
four-second run that was eighteen spans of bookkeeping burying the six that
described the graph. `FilteringSpanProcessor` drops both halves of the pair
before export, which took the trace from 28 spans to 10.

**Give the activity spans something to say.** By default a
`RunActivity:...` observation has a correct duration and nothing to read. Each
node sets `langfuse.observation.input` and `langfuse.observation.output` on its
current span, so the node step is readable one level above the raw LLM payload.

**Put the trace attributes on the root span.** `main.py` opens
`answer-question` around the whole interaction and hangs the trace name,
session, user and tags off it. The workflow id doubles as the session id, so
the Sessions view groups every trace for a given run, and every Temporal span
carries `temporalWorkflowID` and `temporalRunID` for jumping back to the
Temporal UI.

Both processes call `provider.force_flush()` before exiting, because the OTLP
processor batches in the background.

## Run it

Copy `.env.example` to `.env` and fill in the Langfuse keys and a provider key
for whatever `LLM_MODEL` points at, then:

```bash
temporal server start-dev     # UI on localhost:8233
uv run worker.py              # terminal 2
uv run main.py                # terminal 3
```

`main.py` starts the workflow, then polls `status` once a second while it waits
on `handle.result()`:

```
started: hello-world-86fa270e
status: running
status: completed: draft_answer
status: done
result: Durable execution is a computing concept that ensures consistent and
reliable program or transaction execution despite failures or interruptions...
```

Open the run in the Temporal UI at localhost:8233 and the graph shows up as
history: one `ActivityTaskScheduled` / `Started` / `Completed` triple per node,
with the node's input and output stored in the event payloads. Open the same
run in Langfuse and you get the tree above, with cost and token counts on both
generations.
