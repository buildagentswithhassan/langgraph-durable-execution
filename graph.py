from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from langgraph.graph import START, StateGraph
    from opentelemetry import trace
    from temporalio.contrib.langgraph import graph

    from nodes import draft_answer, polish_answer

# Bounded retries, so a bad API key fails fast instead of retrying forever.
LLM_RETRY_POLICY = RetryPolicy(maximum_attempts=3)


def build_graph() -> StateGraph:
    """Construct the two-node graph."""
    g = StateGraph(str)
    g.add_node(
        "draft_answer",
        draft_answer,
        metadata={
            "execute_in": "activity",
            "start_to_close_timeout": timedelta(seconds=60),
            "retry_policy": LLM_RETRY_POLICY,
        },
    )
    g.add_node(
        "polish_answer",
        polish_answer,
        metadata={
            "execute_in": "activity",
            "start_to_close_timeout": timedelta(seconds=60),
            "retry_policy": LLM_RETRY_POLICY,
        },
    )

    g.add_edge(START, "draft_answer")
    g.add_edge("draft_answer", "polish_answer")
    return g


@workflow.defn
class HelloWorldWorkflow:
    def __init__(self) -> None:
        self.stage = "starting"

    @workflow.query
    def status(self) -> str:
        return self.stage

    @workflow.run
    async def run(self, query: str) -> str:
        self.stage = "running"
        result = query

        # A custom span grouping the whole graph run. Plain OpenTelemetry works
        # inside workflow code because the tracer provider is replay-safe: this
        # span gets a deterministic ID and is emitted exactly once, however many
        # times Temporal replays the workflow.
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(
            "run-graph",
            attributes={"langfuse.observation.input": query},
        ) as span:
            # astream yields one {node_name: output} mapping per completed node,
            # so the stage advances as the graph does instead of sitting on the
            # value set in __init__.
            stream = (
                graph("hello-world").compile().astream(query, stream_mode="updates")
            )
            async for update in stream:
                for node, output in update.items():
                    self.stage = f"completed: {node}"
                    result = output

            span.set_attribute("langfuse.observation.output", result)

        self.stage = "done"
        return result
