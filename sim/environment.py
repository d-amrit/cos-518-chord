import simpy
import random
from sim.config import BASE_LATENCY
from sim.node import Node
from sim.events import (
    NodeJoin,
    NodeFail,
    SendMessage,
    ReceiveMessage,
    TimeoutExpired,
    StabilizeTick,
    # SuccessorTimeout,
    LookupEvent,
)


class SimEnvironment:
    """
    Wrapper around simpy.Environment that provides:
      - Deterministic seeding
      - Scheduling of high‑level Event objects
      - Automatic dispatch to node handlers
      - Message routing with a fixed base latency
      - A registry of all nodes in the simulation
    """
    def __init__(self, seed=None):
        # Initialize the underlying SimPy environment
        self.env = simpy.Environment()
        self.seed = seed
        if seed is not None:
            random.seed(seed)
        self.nodes = {}
        self.pending_timeouts = set()

    def register_node(self, node):
        """
        Register a newly created Chord node, so we can route events to it.
        """
        self.nodes[node.node_id] = node

    def schedule_event(self, delay: float, event):
        """
        Schedule a high‑level Event (e.g. NodeFail, SendMessage) to be dispatched after delay simulated seconds.
        """
        def _process():
            yield self.env.timeout(delay)
            self._dispatch_event(event)

        return self.env.process(_process())

    def _dispatch_event(self, event):
        """
        Deliver a scheduled event to the appropriate handler on its node.
        """
        if isinstance(event, NodeJoin):
            if not self.nodes:
                Node(self, bootstrap_id=None)
            else:
                bootstrap = min(self.nodes)
                Node(self, bootstrap_id=bootstrap)

            self.latest_ring = sorted(nid for nid, n in self.nodes.items() if n.active)

        elif isinstance(event, NodeFail):
            if event.node_id is not None:
                failed_node = event.node_id
            else:
                # Keep at least one node alive
                live = [nid for nid, n in self.nodes.items() if n.active]
                if len(live) <= 1:
                    return

                failed_node = random.choice(live)
            self.nodes[failed_node].active = False

            self.latest_ring = sorted(nid for nid, n in self.nodes.items() if n.active)

        elif isinstance(event, SendMessage):
            recv = ReceiveMessage(
                src_id=event.src_id,
                dst_id=event.dst_id,
                rpc_name=event.rpc_name,
                args=event.args,
                kwargs=event.kwargs,
            )
            self.schedule_event(BASE_LATENCY, recv)

        elif isinstance(event, ReceiveMessage):
            node = self.nodes.get(event.dst_id)
            if event.rpc_name == 'find_successor_response' and 'timer_id' in event.kwargs:
                self.pending_timeouts.discard((event.dst_id, event.kwargs['timer_id']))

            if node and node.active:
                node.receive_message(
                    event.src_id,
                    event.rpc_name,
                    *event.args,
                    **event.kwargs,
                )

        elif isinstance(event, TimeoutExpired):
            node = self.nodes.get(event.node_id)
            if node and node.active:
                node.handle_timeout(event.timer_id)

        elif isinstance(event, StabilizeTick):
            node = self.nodes.get(event.node_id)
            if node and node.active:
                node.handle_stabilize_tick()

        elif isinstance(event, LookupEvent):
            node = self.nodes.get(event.start_id)
            if node and node.active:
                node.lookup_iterative(event.key_id, count_stats=True)
        else:
            raise ValueError(f"Unknown event type: {event!r}")

    def send_message(self, src_id, dst_id, rpc_name, *args, **kwargs):
        evt = SendMessage(
            src_id=src_id,
            dst_id=dst_id,
            rpc_name=rpc_name,
            args=args,
            kwargs=kwargs,
        )
        self.schedule_event(0, evt)

    def run(self, until=None):
        """
        Advance the simulation:
          - If until is a timestamp (float), run until that simulated time.
          - If until is None, run until no more events remain.
        """
        if until is not None:
            self.env.run(until=until)
        else:
            self.env.run()
