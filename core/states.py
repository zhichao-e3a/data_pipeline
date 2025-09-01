# File for shared runtime states

from __future__ import annotations
from collections import defaultdict

import asyncio

LOOP: asyncio.AbstractEventLoop | None = None

# Stores Job IDs
PROGRESS    : dict[str, dict[str, str|int]] = {}
CANCELLED   : set[str] = set()
subscribers = defaultdict(set)

# Can use for controlling processing stage
PROCESS_SEM = asyncio.Semaphore(1)
