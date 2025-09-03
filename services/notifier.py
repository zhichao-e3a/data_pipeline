from core import states

import asyncio, traceback
from starlette.websockets import WebSocket, WebSocketDisconnect

async def _publish(
        job_id: str,
        payload: dict
) -> None:

    dead = []

    # Iterate through current sockets and send update to them
    for ws in list(states.SUBSCRIBERS.get(job_id, [])):

        try:
            await ws.send_json(payload)

        except Exception as e:
            print(e)
            dead.append(ws)

    # Discard dead sockets
    for ws in dead:
        states.SUBSCRIBERS[job_id].discard(ws)

def emit(
        job_id: str,
        payload: dict
) -> None:

    coro = _publish(job_id, payload)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and (states.LOOP is None or loop is states.LOOP):
        loop.create_task(coro)

    elif states.LOOP is not None:
        asyncio.run_coroutine_threadsafe(coro, states.LOOP)

def set_progress(
        job_id: str,
        *,
        progress    : int | None = None,
        message     : str | None = None,
        state       : str | None = None
) -> None:

    st = states.PROGRESS.setdefault(
        job_id,
        {
            "progress": 0,
            "state": "running",
            "message": ""
        }
    )

    if progress is not None: st["progress"] = int(progress)
    if message is not None: st["message"] = str(message)
    if state is not None: st["state"]   = state

    # Push update to client
    emit(
        job_id,
        payload={
            "type": "status",
            "job_id": job_id,
            **st
        }
    )

async def ws_status(
        websocket: WebSocket,
        job_id: str
) -> None:

    # Complete WS handshake (WS protocol)
    await websocket.accept()

    # Stores socket in a per-job subscriber set
    states.SUBSCRIBERS[job_id].add(websocket)

    # Grab current status from in-memory
    snapshot = states.PROGRESS.get(
        job_id, {"progress": 0, "state": "unknown", "message": "No such job"}
    )

    # Send to client
    await websocket.send_json({"type": "status", "job_id": job_id, **snapshot})

    try:
        while True:
            # Keep connection alive by pinging every 25 seconds
            await asyncio.sleep(25)
            await websocket.send_json(
                {"type": "ping"}
            )

    except WebSocketDisconnect:
        pass

    except Exception as e:
        print(e)
        traceback.print_exc()

    finally:
        states.SUBSCRIBERS[job_id].discard(websocket)
