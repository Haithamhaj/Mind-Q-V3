from __future__ import annotations
import asyncio
from typing import AsyncGenerator


async def event_stream(session_id: str) -> AsyncGenerator[str, None]:
    # dummy progress events for UI demo; not tied to real jobs
    steps = [
        ("progress", "Loading policies"),
        ("progress", "Scanning artifacts"),
        ("progress", "Aggregating KPIs"),
        ("warn", "Some phases have missing artifacts"),
        ("done", "Ready")
    ]
    for evt, msg in steps:
        yield f"event: {evt}\ndata: {msg}\n\n"
        await asyncio.sleep(0.8)
