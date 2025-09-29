from __future__ import annotations

import asyncio

from concurrent.futures.process import ProcessPoolExecutor
from typing import Optional
from collections import defaultdict
from logging.handlers import QueueListener

LOOP        : Optional[asyncio.AbstractEventLoop] = None
LOG_LISTEN  : Optional[QueueListener] = None
PROC_POOL   : Optional[ProcessPoolExecutor] = None
PROGRESS    : dict[str, dict[str, str | int]] = {}
CANCELLED   : set[str] = set()

SUBSCRIBERS = defaultdict(set)

PROCESS_SEM = asyncio.Semaphore(1)
