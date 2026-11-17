"""The graph's node functions. All I/O and LLM calls live here.

These run as Temporal activities, so they are the only place in the app allowed
to touch the outside world. They are kept in their own module so the workflow
module can import them without dragging the LLM client into Temporal's sandbox.
"""

import os

from litellm import acompletion
from opentelemetry import trace


def _record(input_value: str, output_value: str) -> None:
    """Put a node's input and output on its Temporal activity span.

    Without this the `RunActivity:...` observations in Langfuse are empty
    shells: correct durations, nothing to read. With it each node step shows
    what went in and what came out, one level above the raw LLM payload.
    """
    span = trace.get_current_span()
    span.set_attribute("langfuse.observation.input", input_value)
    span.set_attribute("langfuse.observation.output", output_value)


def _model() -> str:
    # litellm routes on the model string, so switching provider is a config
    # change: "anthropic/claude-sonnet-4-5", "gemini/gemini-2.5-flash", etc.
    return os.environ.get("LLM_MODEL", "openai/gpt-4o-mini")


async def _complete(system: str, user: str) -> str:
    response = await acompletion(
        model=_model(),
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        # num_retries=0 hands retries to Temporal's retry policy, so every
        # attempt shows up in the Temporal UI and in Langfuse instead of being
        # hidden inside the client.
        num_retries=0,
    )
    return response.choices[0].message.content or ""


async def draft_answer(query: str) -> str:
    """Answer the user's question in a first pass."""
    answer = await _complete(
        "Answer the question directly. Three sentences at most.", query
    )
    _record(query, answer)
    return answer


async def polish_answer(draft: str) -> str:
    """Tighten the draft into a single clear sentence."""
    polished = await _complete(
        "Rewrite the text as one clear sentence. Return only the sentence.", draft
    )
    _record(draft, polished)
    return polished
