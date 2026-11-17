import asyncio
import os

from telemetry import setup_tracing

# Tracing must be configured before the client and worker are built, so the
# OpenTelemetry plugin picks up the Langfuse-backed provider.
provider = setup_tracing()

from temporalio.client import Client
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.contrib.opentelemetry import OpenTelemetryPlugin
from temporalio.worker import Worker

from graph import HelloWorldWorkflow, build_graph


async def main() -> None:
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
        # Emits spans for Temporal operations and carries trace context across
        # the client, workflow and activity boundaries, so the client's root
        # span, the workflow, the node activities and the LLM calls all land in
        # one Langfuse trace.
        plugins=[OpenTelemetryPlugin(add_temporal_spans=True)],
    )
    langgraph = LangGraphPlugin(
        graphs={"hello-world": build_graph()},
        # default_activity_options={
        #     "start_to_close_timeout": timedelta(seconds=60),
        #     "retry_policy": RetryPolicy(maximum_attempts=3),
        # },
    )

    worker = Worker(
        client,
        task_queue="langgraph-hello-world",
        workflows=[HelloWorldWorkflow],
        plugins=[langgraph],
    )
    try:
        await worker.run()
    finally:
        provider.force_flush()


if __name__ == "__main__":
    asyncio.run(main())
