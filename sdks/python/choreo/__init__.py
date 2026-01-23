"""
Choreo - Durable workflow orchestration with BYO database

Usage:
    from choreo import Choreo, step

    choreo = Choreo("http://localhost:8080")

    @choreo.function("process-order", trigger="order.created")
    async def process_order(ctx, step):
        user = await step.run("fetch-user", lambda: fetch_user(ctx.event.user_id))
        await step.run("send-email", lambda: send_email(user.email))
        return {"processed": True}

    # Start worker
    await choreo.start_worker()
"""

from .client import Choreo, ChoreoClient
from .function import function, FunctionDef, TriggerDef
from .step import StepContext, StepError
from .event import Event, EventContext
from .run import FunctionRun, RunStatus

__version__ = "0.1.0"
__all__ = [
    "Choreo",
    "ChoreoClient",
    "function",
    "FunctionDef",
    "TriggerDef",
    "StepContext",
    "StepError",
    "Event",
    "EventContext",
    "FunctionRun",
    "RunStatus",
]
