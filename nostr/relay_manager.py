import asyncio
import time
from asyncio import Task
from loguru import logger

from .filter import Filters
from .message_pool import MessagePool
from .relay import Relay, RelayPolicy


class RelayManager:
    relays: dict[str, Relay] = {}
    message_pool: MessagePool = MessagePool()
    running: bool = False
    tasks: list[Task] = []

    async def add_relay(self, url: str, read: bool = True, write: bool = True, subscriptions={}):
        self.relays[url] = Relay(url, RelayPolicy(read, write), self.message_pool, subscriptions)

    async def remove_relay(self, url: str):
        self.relays.pop(url)

    async def add_subscription(self, id: str, filters: Filters):
        for relay in self.relays.values():
            relay.add_subscription(id, filters)

    async def close_subscription(self, id: str):
        for relay in self.relays.values():
            relay.close_subscription(id)

    async def open_connections(self, ssl_options: dict = None):
        self.running = True
        for relay_url in self.relays.keys():
            relay = self.relays.get(relay_url)
            if relay:
                try:
                    await relay.connect()
                    self.tasks.append(asyncio.create_task(relay.receive()))
                except Exception as e:
                    logger.error(str(e))

    async def close_connections(self):
        for relay in self.relays.values():
            await relay.close()
        if len(self.tasks)>0:
            logger.debug("Waiting for tasks to end")
            st = time.time()
            await asyncio.wait(self.tasks)
            logger.debug("Tasks ended after "+str(time.time()-st)+" seconds")
            self.tasks.clear()
        self.running = False

    async def publish_message(self, message: str):
        for relay in self.relays.values():
            if relay.policy.should_write:
                await relay.publish(message)

    async def clear(self):
        await self.close_connections()
        # remove old relay urls
        self.relays.clear()
