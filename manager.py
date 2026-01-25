import asyncio
import contextlib
import json
import logging
import uuid
from _weakrefset import WeakSet
from collections import defaultdict
from collections.abc import AsyncIterator
from enum import IntEnum
from typing import Any, Literal, Iterable
from weakref import WeakKeyDictionary

from fastapi import WebSocket
from redis.asyncio import Redis as AioRedis

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

type _UserID = str
type _JSON_SERIALIZABLE = Any


class WebSocketRoomEnum(IntEnum):
    ORDERS = 0


class WebSocketConnectionManager:
    __slots__ = (
        "__weakref__",
        "_active_connections",
        "_channel_prefix",
        "_pubsub",
        "_pubsub_listener_task",
        "_redis",
        "_send_mode",
        "_user_connection",
        "_websocket_rooms",
    )

    def __init__(
        self,
        redis: AioRedis,
        channel_prefix: str = "ws_channel",
        send_mode: Literal["text", "binary"] = "text",
    ) -> None:
        self._redis = redis
        self._send_mode = send_mode
        self._channel_prefix = channel_prefix

        self._active_connections: defaultdict[WebSocketRoomEnum, WeakSet[WebSocket]] = defaultdict(WeakSet)
        self._websocket_rooms: WeakKeyDictionary[WebSocket, set[WebSocketRoomEnum]] = WeakKeyDictionary()
        self._user_connection: WeakKeyDictionary[WebSocket, _UserID] = WeakKeyDictionary()

        self._pubsub = self._redis.pubsub()
        self._pubsub_listener_task: asyncio.Task | None = None

        log.info(
            "%r initialized with channel prefix: %r and send mode: %r",
            self.__class__.__name__,
            self._channel_prefix,
            self._send_mode,
        )

    def initialize(self):
        if self._pubsub_listener_task is None or self._pubsub_listener_task.done():
            self._pubsub_listener_task = asyncio.create_task(self._pubsub_listener())
            log.info("Pub/sub listener task initialized")

    async def cleanup(self):
        if self._pubsub_listener_task and not self._pubsub_listener_task.done():
            self._pubsub_listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._pubsub_listener_task
            log.info("Pub/sub listener task cleaned up")

    async def connect(
        self,
        ws: WebSocket,
        user_id: str,
        room_id: WebSocketRoomEnum | Iterable[WebSocketRoomEnum],
    ) -> None:
        await ws.accept()
        ws.state.id = uuid.uuid4().hex[:10]

        rooms = room_id if isinstance(room_id, Iterable) else (room_id,)

        for room in rooms:
            self._active_connections[room].add(ws)
            self._websocket_rooms.setdefault(ws, set()).add(room)

        self._user_connection[ws] = user_id

        log.info(
            "User %s connected to room(s) %r via WebSocket %s",
            user_id,
            room_id,
            ws.state.id,
        )

    async def disconnect(self, ws: WebSocket):
        rooms = tuple(self._websocket_rooms.get(ws, ()))  # Copy to avoid modification during iteration
        user_id = self._user_connection.get(ws)

        for room_id in rooms:
            self._active_connections[room_id].discard(ws)
            if not self._active_connections[room_id]:
                del self._active_connections[room_id]
                log.debug("Removed empty room %s after disconnecting WebSocket %s", room_id, ws.state.id)
            log.info("User %r disconnected from room %r via WebSocket %s", user_id, room_id, ws.state.id)

        self._user_connection.pop(ws, None)
        self._websocket_rooms.pop(ws, None)

    async def publish(self, room_id: WebSocketRoomEnum, message: str | bytes | _JSON_SERIALIZABLE) -> None:
        if not isinstance(message, (str, bytes)):
            try:
                message = json.dumps(message, separators=(",", ":"))
            except json.JSONDecodeError as e:
                raise TypeError("'message' must be in (str, bytes, or JSON SERIALIZABLE)") from e
        channel = f"{self._channel_prefix}:{room_id}"
        await self._redis.publish(channel=channel, message=message)
        log.info("Published message to room %d via channel %s", room_id, channel)

    async def _pubsub_listener(self) -> None:
        async def _handle_message() -> AsyncIterator[tuple[WebSocketRoomEnum, Any]]:
            async for m in self._pubsub.listen():
                if not all(
                    [
                        m.get("type") == "pmessage",
                        (data := m.get("data")) is not None,
                        (channel := m.get("channel")) is not None,
                    ]
                ):
                    continue
                try:
                    payload_ = json.loads(data)
                except json.JSONDecodeError:
                    log.warning("Failed to decode JSON from pub/sub message on channel %r", channel)
                    continue
                # channel = _channel_prefix:room_id
                room_id_ = WebSocketRoomEnum(int(channel.split(":", maxsplit=1)[-1]))
                yield room_id_, payload_

        async def _cleanup_pubsub():
            with contextlib.suppress(Exception):
                await self._pubsub.punsubscribe()
                await self._pubsub.aclose()

        try:
            # channel = _channel_prefix:room_id
            await self._pubsub.psubscribe(f"{self._channel_prefix}:*")
            log.info("Subscribed to pub/sub channel pattern: %s:*", self._channel_prefix)

            async for room_id, payload in _handle_message():
                await self._broadcast_to_room(room_id=room_id, message=payload)

        except Exception:
            log.exception("Error while receiving from pub/sub listener")
        finally:
            await _cleanup_pubsub()
            log.info("Unsubscribed from pub/sub and closed connection")

    async def _broadcast_to_room(
        self,
        room_id: WebSocketRoomEnum,
        message: Any,
        exclude_websocket: WebSocket | None = None,
    ):
        connections = self._active_connections.get(room_id)
        if not connections:
            log.debug("No active connections in room %s; skipping broadcast", room_id)
            return

        disconnected = []
        for ws in tuple(connections):  # Copy to avoid modification during iteration
            if ws == exclude_websocket:
                continue
            try:
                await ws.send_json(message, mode=self._send_mode)
            except Exception:
                log.warning("Failed to send message to WebSocket %d in room %s", ws.state.id, room_id, exc_info=True)
                disconnected.append(ws)

        await asyncio.gather(*(self.disconnect(ws) for ws in disconnected))
        log.debug(
            "Broadcasted message to %d connections in room %r", len(connections) - len(disconnected), room_id
        )
