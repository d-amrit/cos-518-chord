from enum import Enum, auto
from dataclasses import dataclass
from typing import Optional


class EventType(Enum):
    NODE_JOIN = auto()
    NODE_FAIL = auto()
    SEND_MESSAGE = auto()
    RECEIVE_MESSAGE = auto()
    TIMEOUT_EXPIRED = auto()
    STABILIZE_TICK = auto()
    SUCCESSOR_TIMEOUT = auto()
    LOOKUP = auto()


@dataclass
class NodeJoin:
    """Event indicating that a node should join the ring."""
    node_id: Optional[int] = None


@dataclass
class NodeFail:
    """Event indicating that a node has crashed."""
    node_id: Optional[int] = None


@dataclass
class SendMessage:
    """Event representing an RPC call in transit."""
    src_id: int
    dst_id: int
    rpc_name: str
    args: tuple
    kwargs: dict


@dataclass
class ReceiveMessage:
    """Event delivered to the destination node to invoke an RPC handler."""
    src_id: int
    dst_id: int
    rpc_name: str
    args: tuple
    kwargs: dict


@dataclass
class TimeoutExpired:
    """Event fired when a previously scheduled timer expires."""
    node_id: int
    timer_id: str


@dataclass
class StabilizeTick:
    """Periodic event triggering stabilize()/fix_fingers()/check_predecessor()."""
    node_id: int



@dataclass
class LookupEvent:
    """
    Schedules a recursive lookup that will be initiated by `start_id`
    for the identifier `key_id`.
    """
    start_id: int   # node initiating the lookup
    key_id:   int   # identifier being looked up
