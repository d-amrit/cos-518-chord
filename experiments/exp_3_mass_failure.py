import datetime
import random
import csv
from bisect import bisect_left
import numpy as np

from sim.environment import SimEnvironment
from sim.node import Node
from sim.utils import MAX_ID
from sim.config import SEED, EXP_3_NO_OF_NODES, EXP_3_NO_OF_KEYS, EXP_3_REPLICATES, EXP_3_LIST_OF_FRAC_OF_FAILED_NODES
from experiments.exp_4_churn import warm_up
from analysis import plot_figures


def owner_before_crash(keys, live_ring):
    """
    Return a dict {key_id: node_id} for the ring _before_ we fail nodes.
    Live_ring must be a sorted list of node IDs.
    """
    owners = {}
    for k in keys:
        idx = bisect_left(live_ring, k)
        if idx == len(live_ring):
            idx = 0
        owners[k] = live_ring[idx]
    return owners


def run_mass_failure(no_of_nodes, frac_failed, no_of_keys, no_of_replicates, seed):
    fractions = []

    for r in range(no_of_replicates):
        print(f"{r + 1}. {datetime.datetime.now().strftime('%H:%M:%S')}: no_of_nodes = {no_of_nodes}, "
              f"X = {frac_failed}, no_of_keys = {no_of_keys}.")
        rng = random.Random(seed + r)
        env = SimEnvironment(seed=seed + r)

        # 1.  Bootstrap Chord.
        first = Node(env, bootstrap_id=None)
        for _ in range(no_of_nodes - 1):
            Node(env, bootstrap_id=first.node_id)

        # 2.  Ensure successors, successor lists, fingers are all correct.
        warm_up(env)

        # 3. Pick {{no_of_keys}} random keys and record original owners.
        keys   = [rng.randrange(MAX_ID) for _ in range(no_of_keys)]
        owners = owner_before_crash(keys, env.latest_ring)

        # 4. Crash pN nodes simultaneously.
        failed_nodes = rng.sample(env.latest_ring, int(frac_failed * len(env.latest_ring)))
        for nid in failed_nodes:
            env.nodes[nid].active = False
        env.latest_ring = [nid for nid in env.latest_ring if nid not in failed_nodes]

        # 5. Rebuild perfect routing for surviving nodes (warm_up uses only active nodes, so it's safe to reuse)
        warm_up(env)

        # 6. Perform no_of_keys iterative look‑ups and count failures
        fail  = 0
        live  = env.latest_ring
        for k in keys:
            start = env.nodes[rng.choice(live)]
            _successor  = start.lookup_iterative(k)     # returns successor or None
            if _successor != owners[k]:
                fail += 1

        fractions.append(fail / no_of_keys)

    return fractions


def run():
    results = []
    for frac_failed in EXP_3_LIST_OF_FRAC_OF_FAILED_NODES:
        fraction_of_failures = run_mass_failure(
            EXP_3_NO_OF_NODES, frac_failed, EXP_3_NO_OF_KEYS, EXP_3_REPLICATES, SEED)
        _mean = np.mean(fraction_of_failures)
        _std  = np.std(fraction_of_failures)
        results.append((frac_failed, _mean, _std))

    # Save results.
    with open('../analysis/data/figure_11_data.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['frac_failed', 'mean', 'std'])
        writer.writerows(results)

    # Plot figures
    plot_figures.plot_95_percent_confidence_interval(fig_no='11')


if __name__ == '__main__':
    run()
