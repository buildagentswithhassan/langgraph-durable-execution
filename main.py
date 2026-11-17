import asyncio
import os
from uuid import uuid4

from telemetry import setup_tracing

provider = setup_tracing()

from opentelemetry import trace
from temporalio.client import Client
from temporalio.contrib.opentelemetry import OpenTelemetryPlugin

from graph import HelloWorldWorkflow

QUERY = "What is durable execution?"


async def main():
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
        plugins=[OpenTelemetryPlugin(add_temporal_spans=True)],
    )

    # A fresh id per run, so Temporal does not reject a duplicate and each run
    # gets its own Langfuse session.
    workflow_id = f"hello-world-{uuid4().hex[:8]}"

    # The trace's root span. Langfuse reads the trace name, session, user and
    # tags off it, and takes the trace-level input and output from it too.
    with trace.get_tracer(__name__).start_as_current_span(
        "answer-question",
        attributes={
            "langfuse.trace.name": "answer-question",
            "langfuse.session.id": workflow_id,
            "langfuse.user.id": "local-dev",
            "langfuse.trace.tags": ["temporal", "langgraph"],
            "langfuse.observation.input": QUERY,
            "langfuse.trace.metadata.task_queue": "langgraph-hello-world",
        },
    ) as span:
        handle = await client.start_workflow(
            HelloWorldWorkflow.run,
            QUERY,
            id=workflow_id,
            task_queue="langgraph-hello-world",
        )
        print("started:", handle.id)

        result_task = asyncio.create_task(handle.result())
        while not result_task.done():
            print("status:", await handle.query(HelloWorldWorkflow.status))
            await asyncio.sleep(1)

        print("status:", await handle.query(HelloWorldWorkflow.status))
        result = await result_task
        print("result:", result)

        span.set_attribute("langfuse.observation.output", result)

    # The OTLP processor batches in the background, so flush before exiting.
    provider.force_flush()


if __name__ == "__main__":
    asyncio.run(main())
