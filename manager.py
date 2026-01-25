import asyncio
import weakref
from collections.abc import AsyncIterator
from contextlib import suppress
from functools import partial
from typing import Any

from fastapi import WebSocket
from fastapi.websockets import WebSocketDisconnect, WebSocketState
from redis.asyncio import Redis as AioRedis

from src.logger import log


class WSPubSubManager:
    __slots__ = (
        "__weakref__",
        "_background_tasks",
        "_channel",
        "_connections",
        "_decode_response",
        "_pubsub",
        "_pubsub_task",
        "_redis",
    )

    def __init__(
        self,
        redis: AioRedis,
        channel: str,
        decode_response: bool = True,
    ) -> None:
        self._redis = redis
        self._channel = channel
        self._decode_response = decode_response

        self._connections = weakref.WeakSet()
        self._pubsub = self._redis.pubsub()

        self._pubsub_task: asyncio.Task | None = None
        self._background_tasks: set[asyncio.Task] = set()

    async def _pubsub_listener(self) -> None:
        try:
            await self._pubsub.subscribe(self._channel)
            async for message in self._pubsub.listen():
                if message.get("type") != "message" or not (data := message.get("data")):  # type: str | bytes
                    continue
                await self._broadcast(data)
        except Exception:
            log.app.exception("Redis pubsub listener crashed")
        finally:
            await self._shutdown_pubsub()

    def _schedule_disconnect(self, ws: WebSocket) -> None:
        disconnect_task = asyncio.create_task(self.disconnect(ws))
        self._background_tasks.add(disconnect_task)
        disconnect_task.add_done_callback(lambda t: self._background_tasks.discard(t))

    def _handle_broadcast_done(self, task: asyncio.Task, ws: WebSocket) -> None:
        if task.exception() is not None:
            self._schedule_disconnect(ws)

    async def _broadcast(self, data: str | bytes) -> None:
        if not self._connections:
            return

        tasks = []
        # Делаем сильную копию, для того чтобы не изменялись подключения во время цикла
        for ws in tuple(self._connections):  # type: WebSocket
            coro = (
                ws.send_text(data.decode() if isinstance(data, bytes) else data)
                if self._decode_response
                else ws.send_bytes(data.encode() if isinstance(data, str) else data)
            )
            task = asyncio.create_task(coro)
            task.add_done_callback(partial(self._handle_broadcast_done, ws=ws))
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _shutdown_pubsub(self) -> None:
        with suppress(Exception):
            await self._pubsub.unsubscribe(self._channel)
            await self._pubsub.aclose()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.add(ws)

    async def disconnect(self, ws: WebSocket) -> None:
        self._connections.discard(ws)
        if ws.client_state in (WebSocketState.CONNECTED, WebSocketState.CONNECTING):
            with suppress(Exception):
                await ws.close()

    async def publish(self, message: str | bytes) -> None:
        await self._redis.publish(channel=self._channel, message=message)

    async def iter_json(self, ws: WebSocket) -> AsyncIterator[dict[str, Any]]:
        try:
            while True:
                yield await ws.receive_json()
        except WebSocketDisconnect:
            await self.disconnect(ws)
        except Exception:
            log.app.exception("Error while receiving from websocket")
            await self.disconnect(ws)

    async def start(self) -> None:
        if self._pubsub_task is None or self._pubsub_task.done():
            self._pubsub_task = asyncio.create_task(
                self._pubsub_listener(),
                name=f"pubsub-listener-{self._channel}",
            )

    async def shutdown(self) -> None:
        if self._pubsub_task and not self._pubsub_task.done():
            self._pubsub_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._pubsub_task

        if self._connections:
            # Делаем сильную копию, для того чтобы не изменялись подключения во время цикла
            await asyncio.gather(
                *(self.disconnect(ws) for ws in tuple(self._connections)),
                return_exceptions=True,
            )
            self._connections.clear()

        await self._shutdown_pubsub()
