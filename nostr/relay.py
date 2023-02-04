import asyncio
import json
from loguru import logger
from queue import Queue
from threading import Lock

import websockets.client
from websockets.legacy.client import WebSocketClientProtocol

from .event import Event
from .filter import Filters
from .message_pool import MessagePool
from .message_type import RelayMessageType
from .subscription import Subscription


class RelayPolicy:
    def __init__(self, should_read: bool = True, should_write: bool = True) -> None:
        self.should_read = should_read
        self.should_write = should_write

    def to_json_object(self) -> dict[str, bool]:
        return {
            "read": self.should_read,
            "write": self.should_write
        }


class Relay:
    ws: WebSocketClientProtocol
    connected: bool = False
    broken: bool = False
    to_publish: Queue[str] = Queue()
    url: str
    policy: RelayPolicy
    message_pool: MessagePool
    subscriptions: dict[str, Subscription] = {}
    lock: Lock

    def __init__(
            self,
            url: str,
            policy: RelayPolicy,
            message_pool: MessagePool,
            subscriptions: dict[str, Subscription] = {}) -> None:
        self.url = url
        self.policy = policy
        self.message_pool = message_pool
        self.subscriptions = subscriptions
        self.lock = Lock()

        # self.ws = WebSocketApp(
        #    url,
        #    on_open=self._on_open,
        #    on_message=self._on_message,
        #    on_error=self._on_error,
        #    on_close=self._on_close)

    async def publish(self, message: str):
        if self.broken:
            return
        if not self.connected:
            self.to_publish.put(message)
            return
        try:
            await self.ws.send(message)
        except:
            self.broken = True
            self.connected = False

    def add_subscription(self, id, filters: Filters):
        # TODO ... send messages for subscriptions to relay
        with self.lock:
            self.subscriptions[id] = Subscription(id, filters)

    def close_subscription(self, id: str) -> None:
        # TODO ... send messages for subscriptions to relay
        with self.lock:
            if id in self.subscriptions.keys():
                self.subscriptions.pop(id)

    def to_json_object(self) -> dict:
        return {
            "url": self.url,
            "policy": self.policy.to_json_object(),
            "subscriptions": [subscription.to_json_object() for subscription in self.subscriptions.values()]
        }

    async def connect(self):
        self.broken = False
        try:
            self.ws = await websockets.client.connect(self.url, open_timeout=2)
            self.connected = True
        except:
            self.connected = False

        logger.debug(self.url + ": connected " + str(self.connected))

    async def receive(self):
        logger.debug("relay started receiving [" + self.url + "]")
        while self.connected:
            try:
                message = await self.ws.recv()

                if message:
                    logger.debug("relay received message [" + self.url + "]")
                    self._on_message(message)
            except Exception as e:
                logger.error("relay had an error: "+str(e)+" [" + self.url + "]")
                self._on_error(e)
            await asyncio.sleep(0.1)
        logger.debug("relay no longer receiving [" + self.url + "]")

    async def close(self):
        if self.connected and self.ws:
            await self.ws.close()
            self.connected = False
            self.broken = False

    def _on_message(self, message: str):
        if self._is_valid_message(message):
            self.message_pool.add_message(message, self.url)

    def _on_error(self, error: Exception):
        self.broken = True
        self.connected = False

    def _is_valid_message(self, message: str) -> bool:
        message = message.strip("\n")
        if not message or message[0] != '[' or message[-1] != ']':
            return False

        message_json = json.loads(message)
        message_type = message_json[0]
        if not RelayMessageType.is_valid(message_type):
            return False
        if message_type == RelayMessageType.EVENT:
            if not len(message_json) == 3:
                return False

            subscription_id = message_json[1]
            with self.lock:
                if subscription_id not in self.subscriptions:
                    return False

            e = message_json[2]
            event = Event(e['pubkey'], e['content'], e['created_at'], e['kind'], e['tags'], e['id'], e['sig'])
            if not event.verify():
                return False

            with self.lock:
                subscription = self.subscriptions[subscription_id]

            if not subscription.filters.match(event):
                return False

        return True
