import random
import math
from bisect import bisect_left

from sim.config import M, STABILIZE_INTERVAL, SUCCESSOR_LIST_SIZE, BASE_LATENCY
from sim.events import (
    StabilizeTick,
)
from sim.utils import in_interval, MAX_ID


class Node:
    """
    Chord node with join, lookup, stabilization, timeout handling,
    and successor-list maintenance for fault tolerance.
    """

    def __init__(self, env, bootstrap_id=None, physical_id=None, count_stats=False):
        """
        env:           SimEnvironment
        bootstrap_id:  node_id to join through, or None for the first node
        physical_id:   identifier for grouping virtual nodes (defaults to node_id)
        """
        self.env = env
        self.bootstrap_id = bootstrap_id
        self.node_id = random.getrandbits(M)
        self.physical_id = physical_id if physical_id is not None else self.node_id
        self.count_stats = count_stats

        # Finger table and ring pointers
        self.finger = [None] * M
        self.successor = None
        self.predecessor = None

        # Successor list for r-fault tolerance
        self.successor_list = []

        # For rotating finger fixes
        self.next_finger = 0

        # Track outstanding fix_fingers timers
        self.outstanding_fix_requests = set()

        # Active flag
        self.active = True

        # Register and schedule first stabilization tick
        self.env.register_node(self)
        self._schedule_stabilize()

        # Initialize ring or join existing
        if bootstrap_id is None:
            self._create_ring()
        else:
            self._join(bootstrap_id)

        # Book‑keeping for recursive lookups
        self.lookups = 0
        self.lookup_fail = 0
        self._pending_key_id = None
        self.pending_key = None
        self.pending = {}

    def _schedule_stabilize(self):
        self.env.schedule_event(
            STABILIZE_INTERVAL,
            StabilizeTick(node_id=self.node_id)
        )

    def _create_ring(self):
        """Initialize finger table and successor list for the first node."""
        self.predecessor = None
        self.successor = self.node_id
        for i in range(M):
            self.finger[i] = self.node_id
        self.successor_list = [self.node_id] * SUCCESSOR_LIST_SIZE

    def _join(self, bootstrap_id):
        """
        Join the ring via bootstrap_id.

        We populate *only* finger[0] (the successor) here.
        All longer‑range finger entries start as unknown (self.node_id) and will
        converge over time via fix_one_finger_sync ticks. This reproduces the
        finger‑staleness window measured in the original Chord experiments.
        """
        bootstrap = self.env.nodes[bootstrap_id]

        # Find our immediate successor using the bootstrap node/
        successor_node = bootstrap.find_successor_local(self.node_id)
        self.successor = successor_node.node_id

        # Initialise finger table: Successor in slot 0, others unknown. Wait for stabilize() to fill it.
        self.finger = [self.successor] + [self.node_id] * (M - 1)

        self.successor_list = [self.successor]

        self.predecessor = None

        # Notify successor of our presence.
        self.env.send_message(
            self.node_id,
            self.successor,
            'notify',
        )

    def find_successor_local(self, id_key):
        """
        Purely in‑memory search used by warm‑up, joins, and synchronous helpers.
        No network activity; returns the Node object responsible for id_key.
        """
        # Check if you are the owner.
        if id_key == self.node_id:
            return self

        # Am I the predecessor of id_key?
        if in_interval(id_key, self.node_id, self.successor, inc_end=True):
            return self.env.nodes[self.successor]

        # Otherwise recurse on closest preceding finger
        cp_id = self.closest_preceding_finger_local(id_key)
        return self.env.nodes[cp_id].find_successor_local(id_key)

    def closest_preceding_finger_local(self, id_key):
        """
        Synchronous helper: scan finger table for the closest preceding node.
        """
        for i in range(M - 1, -1, -1):
            f = self.finger[i]
            if f is not None and in_interval(f, self.node_id, id_key, False, False):
                return f
        return self.node_id

    def timeout_duration(self):
        """
        Compute a per-node timeout: 4 * base_latency * log2(active_nodes).
        """
        no_of_nodes = max(1, sum(1 for node in self.env.nodes.values() if node.active))
        return 4 * BASE_LATENCY * math.log2(no_of_nodes)

    # === Event Handlers ===

    def join(self):
        """Handler for explicit NodeJoin events."""
        self._join(self.bootstrap_id)

    def fail(self):
        """Handler for NodeFail: mark this node inactive."""
        self.active = False

    def receive_message(self, src_id, rpc_name, *args, **kwargs):
        """Dispatch incoming RPC messages by name."""
        handler = getattr(self, f"rpc_{rpc_name}", None)
        if handler:
            handler(src_id, *args, **kwargs)

    # === Periodic Tasks ===

    def stabilize(self):
        """
        Check and update successor pointer, then request
        successor's predecessor and successor list.
        """
        if self.successor is not None:
            # Ask successor for its predecessor
            self.env.send_message(
                self.node_id,
                self.successor,
                'get_predecessor'
            )
            # Also request the successor’s successor list
            self.env.send_message(
                self.node_id,
                self.successor,
                'get_successor_list'
            )

    def check_predecessor(self):
        """Clear predecessor if it has failed."""
        if self.predecessor is not None:
            node = self.env.nodes.get(self.predecessor)
            if node is None or not node.active:
                self.predecessor = None

    # === RPC Handlers ===
    def rpc_find_successor(self, src_id, id_key, req_id):
        successor = self.successor
        if in_interval(id_key, self.node_id, successor, inc_start=False, inc_end=True):
            self.env.send_message(
                self.node_id,
                src_id,
                'find_successor_response',
                id_key, req_id
            )
        else:
            cp = self.closest_preceding_finger_local(id_key)
            self.env.send_message(
                self.node_id,
                cp,
                'find_successor',
                id_key, req_id
            )

    def rpc_find_successor_response(self, src_id, successor_id, req_id):
        if req_id in self.pending:
            key = self.pending.pop(req_id)
            ring = getattr(self.env, "latest_ring", [])
            idx = bisect_left(ring, key)
            if idx == len(ring):
                idx = 0
            if successor_id != ring[idx]:
                self.lookup_fail += 1
            return

        self.env.send_message(self.node_id, src_id,
                              'find_successor_response', successor_id, req_id)

    def rpc_notify(self, src_id):
        """Potential predecessor notification."""
        if (self.predecessor is None or
                in_interval(src_id, self.predecessor, self.node_id, False, False)):
            self.predecessor = src_id

    def rpc_get_predecessor(self, src_id):
        """Reply with our current predecessor."""
        self.env.send_message(
            self.node_id,
            src_id,
            'get_predecessor_response',
            self.predecessor
        )

    def rpc_get_successor_list(self, src_id):
        """Provide our current successor list."""
        self.env.send_message(
            self.node_id,
            src_id,
            'get_successor_list_response',
            list(self.successor_list)
        )

    def rpc_get_predecessor_response(self, pred_id):
        """
        Process the successor’s predecessor, possibly updating our
        own successor, then notify and request its successor list.
        """
        if (pred_id is not None and
                in_interval(pred_id, self.node_id, self.successor, False, False)):
            self.successor = pred_id

        # Notify new (or existing) successor
        self.env.send_message(
            self.node_id,
            self.successor,
            'notify',
        )
        # Request its successor list
        self.env.send_message(
            self.node_id,
            self.successor,
            'get_successor_list'
        )

    def rpc_get_successor_list_response(self, successor_list):
        """
        Update our successor list from the received list:
        take our direct successor plus up to r-1 entries.
        """
        updated = [self.successor] + successor_list[:SUCCESSOR_LIST_SIZE - 1]
        self.successor_list = updated

    # === Lookup API ===
    def lookup_iterative(self, key_id, count_stats=False):
        if count_stats:
            self.lookups += 1

        nid = self.node_id
        visited = set()
        hop_budget = 2 * math.ceil(math.log2(len(self.env.nodes) + 1))

        for _ in range(hop_budget):
            node = self.env.nodes.get(nid)
            if node is None or not node.active or nid in visited:
                if count_stats:
                    self.lookup_fail += 1
                return None
            visited.add(nid)

            if key_id == node.node_id:
                return node.node_id

            successor = node.successor
            if in_interval(key_id, node.node_id, successor, inc_end=True):
                # Only for Experiment 4, count stats.
                if count_stats:
                    # ring = getattr(self.env, "latest_ring", [])
                    # idx = bisect_left(ring, key_id)
                    # actual = ring[idx if idx < len(ring) else 0]
                    live_ring = sorted(nid for nid, n in self.env.nodes.items() if n.active)
                    idx = bisect_left(live_ring, key_id)
                    if idx == len(live_ring):
                        idx = 0
                    actual = live_ring[idx]
                    if successor != actual:
                        # self.lookup_fail += 1
                        if successor != actual:
                            # try to skip over dead or stale successors
                            found_valid_successor = False
                            cur = successor
                            for _ in range(SUCCESSOR_LIST_SIZE - 1):
                                nxt = self.env.nodes.get(cur)
                                if not nxt or not nxt.active:
                                    break
                                cur = nxt.successor
                                if cur == actual:
                                    found_valid_successor = True  # successor list bridged the gap
                                    break
                            if not found_valid_successor:
                                self.lookup_fail += 1


                return successor

            nid = node.closest_preceding_finger_local(key_id)

        # Exceeded hop budget
        if count_stats:
            self.lookup_fail += 1
        return None

    # === Synchronous (no‐SimPy) APIs for custom churn loop ===
    def rpc_notify_sync(self, src_id: int):
        """Synchronous notify: update predecessor if needed."""
        if (self.predecessor is None
                or in_interval(src_id, self.predecessor, self.node_id, False, False)):
            self.predecessor = src_id

    def stabilize_sync(self, nodes):
        """
        Synchronous, incremental version of Chord's stabilize procedure.

        Steps
        -----
        0. While my current successor is dead, repeatedly promote the first
           live entry from my successor list.  If the list empties, pick a
           random live node (if any) and use *its* successor list, else stop
           (isolated node).

        1. Ask the (live) successor for its predecessor.  If that predecessor
           falls strictly between me and the successor on the identifier
           circle, adopt it as my new successor (one‑step ring tightening).

        2. Notify the (possibly new) successor, so it can update its predecessor
           pointer to me.

        After step 0 or 1 we also refresh self.successor_list from the current
        live successor to keep the backup list fresh.
        """
        # 0. Ensure live successor.
        while (self.successor not in nodes) or (not nodes[self.successor].active):
            # Promote the next backup, if any
            if self.successor_list:
                self.successor = self.successor_list.pop(0)
            else:
                # No backups: pick an arbitrary live node (if any)
                live = [nid for nid, n in nodes.items() if n.active and nid != self.node_id]
                if not live:
                    # I am alone in the ring
                    self.successor = self.node_id
                    return
                self.successor = random.choice(live)

        if self.successor == self.node_id:
            return

        # Refresh successor list from the (now live) successor
        successor_node = nodes[self.successor]
        r = SUCCESSOR_LIST_SIZE
        self.successor_list = [self.successor] + successor_node.successor_list[: r - 1]

        # 1. Check predecessor.
        pred_of_successor = successor_node.predecessor
        if (pred_of_successor is not None and pred_of_successor in nodes
                and nodes[pred_of_successor].active
                and in_interval(pred_of_successor,
                                self.node_id,
                                self.successor,
                                inc_start=False,
                                inc_end=False)):
            # Adopt the closer predecessor as new successor
            self.successor = pred_of_successor
            successor_node = nodes[self.successor]
            self.successor_list = [self.successor] + successor_node.successor_list[: r - 1]

        # 2. Notify successor.
        successor_node.rpc_notify_sync(self.node_id)

    def check_predecessor_sync(self, nodes: dict):
        """
        Synchronous check_predecessor: clear if predecessor is down.
        """
        if (self.predecessor is None
                or self.predecessor not in nodes
                or not nodes[self.predecessor].active):
            self.predecessor = None

    def fix_specific_finger_sync(self, idx):
        start = (self.node_id + (1 << idx)) % MAX_ID
        successor = self.find_successor_local(start).node_id
        self.finger[idx] = successor
