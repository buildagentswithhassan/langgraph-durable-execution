"""OpenTelemetry wiring that sends Temporal and LLM spans to Langfuse.

Both the worker and the client call `setup_tracing()`. The provider comes from
`temporalio.contrib.opentelemetry.create_tracer_provider`, which is what makes
tracing safe under durable execution: span IDs are derived deterministically
from workflow state and spans are not re-exported when Temporal replays a
workflow, so a run that replays ten times still lands as one clean trace.
"""

import base64
import os

from dotenv import load_dotenv

load_dotenv()  # must run before anything reads LANGFUSE_* / OPENAI_API_KEY

from openinference.instrumentation.litellm import LiteLLMInstrumentor
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from temporalio.contrib.opentelemetry import create_tracer_provider

SERVICE = "langgraph-durable-execution"

# `main.py` polls the status query once a second, and the plugin emits a
# QueryWorkflow/HandleQuery span pair for every poll. They say nothing about
# what the graph did and would bury the six spans that do, so they never reach
# Langfuse. Both spans of a pair are dropped together, so nothing is orphaned.
DROPPED_SPAN_PREFIXES = ("QueryWorkflow:", "HandleQuery:")


class FilteringSpanProcessor(BatchSpanProcessor):
    """A batch processor that drops bookkeeping spans before they are exported."""

    def on_end(self, span: ReadableSpan) -> None:
        if span.name.startswith(DROPPED_SPAN_PREFIXES):
            return
        super().on_end(span)


def setup_tracing():
    """Point OpenTelemetry at Langfuse and instrument the OpenAI SDK.

    Returns the provider so callers can `force_flush()` before exiting.
    """
    for key in ("LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY", "LANGFUSE_BASE_URL"):
        if not os.environ.get(key):
            raise RuntimeError(f"{key} is not set; copy .env.example to .env")

    auth = base64.b64encode(
        f"{os.environ['LANGFUSE_PUBLIC_KEY']}:{os.environ['LANGFUSE_SECRET_KEY']}".encode()
    ).decode()

    exporter = OTLPSpanExporter(
        # Langfuse's OTLP endpoint is HTTP, so this is the http exporter.
        endpoint=f"{os.environ['LANGFUSE_BASE_URL'].rstrip('/')}/api/public/otel/v1/traces",
        headers={
            "Authorization": f"Basic {auth}",
            "x-langfuse-ingestion-version": "4",  # real-time ingestion
        },
    )

    provider = create_tracer_provider(
        resource=Resource.create(
            {
                SERVICE_NAME: SERVICE,
                # Keeps local runs out of production dashboards.
                "deployment.environment.name": os.environ.get(
                    "LANGFUSE_ENVIRONMENT", "development"
                ),
            }
        )
    )
    provider.add_span_processor(
        FilteringSpanProcessor(exporter, schedule_delay_millis=500)
    )
    trace.set_tracer_provider(provider)

    # Records every litellm call as a span carrying model, tokens and messages.
    # Langfuse renders those as generation observations.
    LiteLLMInstrumentor().instrument()

    return provider
