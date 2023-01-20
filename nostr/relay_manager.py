import threading
import time

from .filter import Filters
from .message_pool import MessagePool
from .relay import Relay, RelayPolicy


class RelayManager:
    def __init__(self) -> None:
        self.relays: dict[str, Relay] = {}
        self.message_pool = MessagePool()
        self.relay_threads: dict[str, threading.Thread] = {}

    def add_relay(self, url: str, read: bool = True, write: bool = True, subscriptions={}):
        policy = RelayPolicy(read, write)
        relay = Relay(url, policy, self.message_pool, subscriptions)
        self.relays[url] = relay

    def remove_relay(self, url: str):
        self.relays.pop(url)

    def add_subscription(self, id: str, filters: Filters):
        for relay in self.relays.values():
            relay.add_subscription(id, filters)

    def close_subscription(self, id: str):
        for relay in self.relays.values():
            relay.close_subscription(id)

    def open_connection(self, relay: Relay, ssl_options: dict = None):
        if relay.url in self.relay_threads.keys():
            return

        self.relay_threads[relay.url] = threading.Thread(
            target=relay.connect,
            args=(ssl_options,),
            name=f"{relay.url}-thread"
        )
        self.relay_threads[relay.url].daemon = True
        self.relay_threads[relay.url].start()

    def close_connection(self, relay):
        if relay.url in self.relay_threads.keys():
            relay.ws.keep_running = False
            relay.ws.close()
            self.relay_threads[relay.url].join()
            time.sleep(0.1) # WHY?!?
            print( relay.url+" closed connection")
            del (self.relay_threads[relay.url])
        relay.close()

    def open_connections(self, ssl_options: dict = None):
        for relay in self.relays.values():
            self.open_connection(relay, ssl_options)

    def close_connections(self):
        for relay in self.relays.values():
            self.close_connection(relay)

    def publish_message(self, message: str):
        for relay in self.relays.values():
            if relay.policy.should_write:
                relay.publish(message)
