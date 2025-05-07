import datetime
import random
import csv
from bisect import bisect_left
import numpy as np
import math

from sim.environment import SimEnvironment
from sim.events import LookupEvent, NodeFail, NodeJoin
from sim.config import (SUCCESSOR_LIST_SIZE, STABILIZE_INTERVAL, WARMUP_END, EXP_4_NO_OF_NODES, EXP_4_KEY_LOOKUP_RATE,
                        EXP_4_FAIL_JOIN_RATE, EXPT_4_SIMULATION_TIME, EXP_4_REPLICATES, SEED)
from sim.utils import MAX_ID
from sim.node import Node
from analysis import plot_figures
# ------------------------------------------------------------------
# For time-related reasons, we will run a global batch stabilizer instead of asking SimPy to schedule it separately.
import sim.node
sim.node.Node._schedule_stabilize = lambda self: None
# ------------------------------------------------------------------


def warm_up(env):
    ring_ids = sorted(nid for nid, n in env.nodes.items() if n.active)
    env.latest_ring = ring_ids

    n_nodes = len(ring_ids)
    r       = SUCCESSOR_LIST_SIZE
    m       = len(env.nodes[next(iter(env.nodes))].finger)

    for idx, nid in enumerate(ring_ids):
        node = env.nodes[nid]
        node.successor     = ring_ids[(idx + 1) % n_nodes]
        node.predecessor   = ring_ids[idx - 1]
        node.successor_list = [
            ring_ids[(idx + j) % n_nodes] for j in range(1, r + 1)
        ]

        # Populate all finger tables
        for b in range(m):
            start = (nid + (1 << b)) % MAX_ID
            j     = bisect_left(ring_ids, start)
            if j == n_nodes:
                j = 0
            node.finger[b] = ring_ids[j]


def global_stabilizer(env, rng, max_time=float('inf')):
    while env.env.now < max_time:
        yield env.env.timeout(STABILIZE_INTERVAL)

        ring = sorted(nid for nid, n in env.nodes.items() if n.active)
        env.latest_ring = ring
        n_nodes = len(ring)
        if n_nodes == 0:
            break

        for idx, nid in enumerate(ring):
            node = env.nodes[nid]
            if not node.active:
                continue

            # Refresh the successor pointer only if the current one is dead
            if (node.successor not in env.nodes) or (not env.nodes[node.successor].active):
                node.successor = ring[(idx + 1) % n_nodes]

            # Regular Chord maintenance
            node.stabilize_sync(env.nodes)  # may update successor_list
            max_no_of_fingers = max(1, math.ceil(math.log2(n_nodes)))
            for idx_to_fix in range(max_no_of_fingers):
                node.fix_specific_finger_sync(idx_to_fix)
            # idx_to_fix = rng.randrange(max_no_of_fingers)
            node.check_predecessor_sync(env.nodes)

        if env.env.now < WARMUP_END:
            continue


def run_churn(no_of_keys, join_rate, lookup_rate, simulation_time, no_of_replicates, seed):
    results = []
    for run in range(no_of_replicates):
        rng = random.Random(seed + run)
        env = SimEnvironment(seed=seed + run)

        # 1.  Bootstrap Chord
        first = Node(env, bootstrap_id=None, count_stats=True)
        for _ in range(no_of_keys - 1):
            Node(env, bootstrap_id=first.node_id, count_stats=True)

        # 2. For first 10 seconds, ensure that successor, finger tables, etc. are right. 
        warm_up(env)

        # 3.  Stabilize chord.
        env.env.process(global_stabilizer(env, rng, max_time=simulation_time))

        # 4.  Generate Poisson‑timed events up to horizon simulation_time
        def poisson(rate):
            t = 0.0
            while True:
                t += rng.expovariate(rate)
                if t > simulation_time:
                    break
                yield t

        events = (
            [(t, 'join')   for t in poisson(join_rate)] +
            [(t, 'fail')   for t in poisson(join_rate)] +
            [(t, 'lookup') for t in poisson(lookup_rate)]
        )
        events.sort(key=lambda x: x[0])

        for t, typ in events:
            if typ == 'lookup':
                if t <= simulation_time and env.latest_ring:
                    start = rng.choice(env.latest_ring)
                    key   = rng.randrange(MAX_ID)
                    env.schedule_event(t, LookupEvent(start_id=start, key_id=key))
            elif typ == 'join':
                env.schedule_event(t, NodeJoin())
            else:
                env.schedule_event(t, NodeFail())

        # 5.  Run simulation up to simulation_time seconds.
        env.run(until=simulation_time)

        # 6. Calculate failed %
        issued = sum(n.lookups     for n in env.nodes.values())
        failed = sum(n.lookup_fail for n in env.nodes.values())
        fail_perc = failed / issued if issued else 0.0
        results.append(fail_perc)
        print(f"{run + 1}. {datetime.datetime.now().strftime('%H:%M:%S')}: "
              f"N = {no_of_keys}, "
              f"mu = {join_rate}, "
              f"Issued = {issued}, "
              f"Failed = {round(failed, 5)}, "
              f"Fail % = {round(fail_perc, 3)}")

    return results


def run():
    results = []
    for join_rate in EXP_4_FAIL_JOIN_RATE:
        fraction_of_failures = run_churn(EXP_4_NO_OF_NODES, join_rate, EXP_4_KEY_LOOKUP_RATE,
                                         EXPT_4_SIMULATION_TIME, EXP_4_REPLICATES, SEED)
        _mean = np.mean(fraction_of_failures)
        _std = np.std(fraction_of_failures)
        results.append((join_rate, _mean, _std))

    # Write results to CSV
    with open('../analysis/data/figure_12_data.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['churn_rate', 'mean', 'std'])
        writer.writerows(results)

    # Plot figures
    plot_figures.plot_95_percent_confidence_interval(fig_no='12')


if __name__ == '__main__':
    # env = SimEnvironment(seed=1)
    # nodes = [Node(env, bootstrap_id=None)]
    # for _ in range(7):
    #     Node(env, bootstrap_id=nodes[0].node_id)
    # env.latest_ring = sorted(env.nodes)
    #
    # n0 = env.nodes[env.latest_ring[0]]
    # n0.finger[2] = n0.node_id  # deliberately stale
    #
    # n0.lookup_iterative((n0.node_id + 3) % MAX_ID, count_stats=True)
    # print("lookups:", n0.lookups, "fails:", n0.lookup_fail)  # expect 1 / 1

    run()
