import pandas as pd
import random
import bisect
import csv
from collections import defaultdict
import numpy as np
import datetime

from sim.environment import SimEnvironment
from sim.config import (STABILIZE_INTERVAL, M, SEED, EXP_1_NO_OF_NODES, EXP_1b_NO_OF_KEYS, 
                        EXP_1_REPLICATES, EXP_1b_NO_OF_VIRTUAL_NODES)
from sim.utils import MAX_ID
from analysis import plot_figures


def wait_for_stability(env: SimEnvironment):
    """
    Wait until the network to no changes to any active node's successor or predecessor.
    """
    while True:
        before = {
            nid: (node.successor, node.predecessor)
            for nid, node in env.nodes.items() if node.active
        }
        t0 = env.env.now
        env.run(until=t0 + STABILIZE_INTERVAL)
        after = {
            nid: (node.successor, node.predecessor)
            for nid, node in env.nodes.items() if node.active
        }
        if before == after:
            break


def run_load_balance(no_of_nodes, no_of_keys, no_of_replicates, seed, use_virtual=False, v=1):
    no_of_keys_for_each_node = []
    for r in range(no_of_replicates):
        seed += r
        random.seed(seed)

        # 1. Generate node IDs (and track physical IDs)
        node_ids = []
        phys_ids = []
        for pid in range(no_of_nodes):
            for _ in range(use_virtual and v or 1):
                nid = random.getrandbits(M)
                node_ids.append(nid)
                phys_ids.append(pid)

        # 2. Sort by node_id
        sorted_pairs = sorted(zip(node_ids, phys_ids), key=lambda x: x[0])
        ids, pids = zip(*sorted_pairs)

        # 3. Count no_of_keys keys via bisect
        loads = defaultdict(int)
        for _ in range(no_of_keys):
            key = random.randrange(MAX_ID)
            idx = bisect.bisect_left(ids, key)
            if idx == len(ids):
                idx = 0
            loads[pids[idx]] += 1
        no_of_keys_for_each_node.append(loads)

    return no_of_keys_for_each_node


def aggregate_loads(no_of_keys_for_each_node, no_of_nodes):
    """
    Convert a list of {pid:count} dicts into a (no_of_replicates x no_of_nodes) numpy array.
    """
    no_of_replicates = len(no_of_keys_for_each_node)
    _agg_array = np.zeros((no_of_replicates, no_of_nodes), dtype=int)
    for i, loads in enumerate(no_of_keys_for_each_node):
        for pid, cnt in loads.items():
            _agg_array[i, pid] = cnt
    return _agg_array


def run():
    # Part a.
    list_of_no_of_keys = list(range(10 * EXP_1_NO_OF_NODES, 100 * EXP_1_NO_OF_NODES + 1, 10 * EXP_1_NO_OF_NODES))
    figure_8a_data = []
    for no_of_keys in list_of_no_of_keys:
        no_of_keys_for_each_node = run_load_balance(EXP_1_NO_OF_NODES, no_of_keys, EXP_1_REPLICATES, SEED,
                                                    use_virtual=False)
        if no_of_keys == 50 * EXP_1_NO_OF_NODES:
            _df = pd.DataFrame({'number_of_keys': no_of_keys_for_each_node[0].values()})
            _df.to_csv('../analysis/data/figure_8b_data.csv', index=False)

        _agg_array = aggregate_loads(no_of_keys_for_each_node, EXP_1_NO_OF_NODES)
        _mean = _agg_array.mean()
        figure_8a_data.append((
            no_of_keys,
            _mean,
            np.percentile(_agg_array, 1),
            np.percentile(_agg_array, 99),
        ))
        print(f"{datetime.datetime.now().strftime('%H:%M:%S')}: no_of_keys = {no_of_keys}, Mean #keys/node = {_mean}")

    # Save results.
    with open('../analysis/data/figure_8a_data.csv', 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['no_of_keys', 'mean', 'p1', 'p99'])
        w.writerows(figure_8a_data)

    # Part b
    figure_9_data = []
    for v in EXP_1b_NO_OF_VIRTUAL_NODES:
        no_of_keys_for_each_node = run_load_balance(EXP_1_NO_OF_NODES, EXP_1b_NO_OF_KEYS, EXP_1_REPLICATES, SEED,
                                                    use_virtual=True, v=v)
        _agg_array = aggregate_loads(no_of_keys_for_each_node, EXP_1_NO_OF_NODES)
        _mean = _agg_array.mean()
        figure_9_data.append((
            v,
            np.percentile(_agg_array, 1),
            np.percentile(_agg_array, 99),
        ))
        print(f"{datetime.datetime.now().strftime('%H:%M:%S')}: no_of_virtual_nodes = {v}, Mean #keys/node = {_mean}")

    # Save results
    with open('../analysis/data/figure_9_data.csv', 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['v', 'mean', 'p1', 'p99'])
        w.writerows(figure_9_data)

    # Plot figures
    plot_figures.plot_mean_and_percentile_1_and_99(fig_no='8a')
    plot_figures.plot_pdf(fig_no='8b')
    plot_figures.plot_mean_and_percentile_1_and_99(fig_no='9')


if __name__ == '__main__':
    run()
