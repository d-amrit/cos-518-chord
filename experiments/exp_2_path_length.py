import random
import bisect
import csv
import numpy as np

from sim.config import M, SEED, EXP_2_REPLICATES
from sim.utils import MAX_ID
from analysis import plot_figures


def run_path_length(no_of_nodes, no_of_replicates, seed):
    all_hops = []
    for r in range(no_of_replicates):
        seed += r
        random.seed(seed)

        # 1. Generate and sort {{no_of_nodes}} random node IDs
        nodes = sorted(random.getrandbits(M) for _ in range(no_of_nodes))

        # 2. Build finger tables statically
        finger_map = [[None]*M for _ in range(no_of_nodes)]
        for idx, nid in enumerate(nodes):
            for i in range(M):
                target = (nid + (1 << i)) % MAX_ID
                j = bisect.bisect_left(nodes, target)
                if j == no_of_nodes: j = 0
                finger_map[idx][i] = j

        # 3. For each node, do one lookup for a random key
        for start_idx in range(no_of_nodes):
            key = random.randrange(MAX_ID)
            hops = 0
            curr = start_idx
            while True:
                hops += 1
                successor_idx = finger_map[curr][0]
                successor_id  = nodes[successor_idx]
                
                # 1. Check interval (exclusive current, inclusive successor)
                if ((nodes[curr] < successor_id and nodes[curr] < key <= successor_id)
                        or (nodes[curr] > successor_id and (key > nodes[curr] or key <= successor_id))):
                    break
                
                # 2. Otherwise, find the highest finger before key.
                for i in reversed(range(M)):
                    next_node = finger_map[curr][i]
                    nid_i = nodes[next_node]
                    if ((nodes[curr] < key and nodes[curr] < nid_i < key)
                            or (nodes[curr] > key and (nid_i > nodes[curr] or nid_i < key))):
                        curr = next_node
                        break
                else:
                    # No finger helps: fall back to successor.
                    curr = successor_idx
            all_hops.append(hops)
    return all_hops


def run():
    results = []
    hops_for_fig_b = []

    for k in range(3, 15):
        no_of_nodes = 2 ** k
        hops = run_path_length(no_of_nodes, EXP_2_REPLICATES, SEED)
        mean_hops = np.mean(hops)
        p1_hops = np.percentile(hops, 1)
        p99_hops = np.percentile(hops, 99)
        results.append((no_of_nodes, mean_hops, p1_hops, p99_hops))
        if no_of_nodes == 2 ** 12:
            hops_for_fig_b = hops.copy()

    # Write summary metrics for Figure 10a
    with open('../analysis/data/figure_10a_data.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['no_of_nodes', 'mean', 'p1', 'p99'])
        writer.writerows(results)

    # Write raw hop counts for no_of_nodes=4096 for Figure 10b
    if hops_for_fig_b:
        with open('../analysis/data/figure_10b_data.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['hops'])
            for h in hops_for_fig_b:
                writer.writerow([h])

    # Plot figures
    plot_figures.plot_mean_and_percentile_1_and_99(fig_no='10a')
    plot_figures.plot_pdf(fig_no='10b')


if __name__ == '__main__':
    run()
