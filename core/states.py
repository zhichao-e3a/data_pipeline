from __future__ import annotations
from typing import Optional
from collections import defaultdict
import asyncio
from logging.handlers import QueueListener

LOOP        : Optional[asyncio.AbstractEventLoop] = None
LOG_LISTEN  : Optional[QueueListener] = None
PROGRESS    : dict[str, dict[str, str | int]] = {}
CANCELLED   : set[str] = set()

SUBSCRIBERS = defaultdict(set)

PROCESS_SEM = asyncio.Semaphore(1)
