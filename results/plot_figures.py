import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sim import config

INPUT_PATH = '../analysis/data/'
OUTPUT_PATH = '../analysis/figures/'


def plot_mean_and_percentile_1_and_99(fig_no):
    # Figure 8a
    if fig_no == '8a':
        x_col = 'K'
        x_label = 'Total number of keys (x 10,000)'
        y_label = 'Number of keys per node'
        title = 'Replicating Figure 8a: Measuring load balance'
        legend_position = 'upper left'

    # Figure 9
    elif fig_no == '9':
        x_col = 'v'
        x_label = 'Number of virtual nodes'
        y_label = 'Number of keys per node'
        title = 'Replicating Figure 9: Measuring load balance with virtual nodes. #Nodes = $10^4$ and #Keys = $10^6$.'
        legend_position = 'upper right'

    # Figure 10a
    elif fig_no == '10a':
        x_col = 'N'
        x_label = 'Number of nodes'
        y_label = 'Path Length'
        title = 'Replicating Figure 10a: Measuring path length as a function of network size'
        legend_position = 'upper left'

    else:
        raise NotImplementedError

    df = pd.read_csv(INPUT_PATH + f'figure_{fig_no}_data.csv')
    # Prepare data for plotting
    if x_col == 'K':
        x = df['K'] / 10000
    else:
        x = df[x_col]
    y = df['mean']
    y_err = [y - df['p1'], df['p99'] - y]

    # Plot
    plt.figure(figsize=(10, 8))
    plt.errorbar(
        x,
        y,
        yerr=y_err,
        fmt='D',
        mfc='none',
        capsize=0,
        linestyle='none',
        label='1st and 99th percentiles'
    )

    # Labels and ticks
    if fig_no == '10a':
        plt.xscale('log')
    plt.title(title, fontsize=14)
    plt.xlabel(x_label, fontsize=12)
    plt.ylabel(y_label, fontsize=12)
    if fig_no == '8a':
        plt.xticks(range(0, 101, 10))
        plt.xlim(0, 110)
    elif fig_no == '9':
        plt.xticks(config.EXP_1b_NO_OF_VIRTUAL_NODES)

        # Legend
    plt.legend(loc=legend_position, fontsize=12)

    plt.tight_layout()
    # plt.show()
    plt.savefig(OUTPUT_PATH + f'figure_{fig_no}.png', dpi=300, bbox_inches="tight")


def plot_pdf(fig_no):
    if fig_no == '8b':
        col_name = 'number_of_keys'
        x_label = 'Number of keys per node'
        title = 'Replicating Figure 8b: PDF of number of keys/node when #Nodes = $10^4$ and #Keys = $5 \cdot 10^5$'

    elif fig_no == '10b':
        col_name = 'hops'
        x_label = 'Path Length'
        title = 'Replicating Figure 10b: PDF of the path length in a network with $2^{12}$ nodes'

    else:
        raise NotImplementedError

    df = pd.read_csv(INPUT_PATH + f'figure_{fig_no}_data.csv')
    data = df[col_name].values

    # Build a PMF: counts at each integer path‑length, divided by total
    values, counts = np.unique(data, return_counts=True)
    pmf = counts / counts.sum()

    fig, ax = plt.subplots(figsize=(10, 8))

    ax.plot(values, pmf, linewidth=1)

    ax.set_xlabel(x_label, fontsize=12)
    ax.set_ylabel("PDF", fontsize=12)
    plt.title(title, fontsize=14)

    if fig_no == '8b':
        ax.set_xlim(0, 500)
        ax.set_xticks(np.arange(0, 501, 50))

    plt.tight_layout()
    plt.savefig(OUTPUT_PATH + f'figure_{fig_no}.png', dpi=300, bbox_inches="tight")


def plot_95_percent_confidence_interval(fig_no, no_of_replicates=10):
    if fig_no == '11':
        title = ('Replicating Figure 11: The fraction of lookups that fail as a function of the fraction of nodes that '
                 'fail.')
        x_label = 'Failed Nodes (fraction of total)'
        x_col = 'frac_failed'

    elif fig_no == '12':
        title = ('Replicating Figure 12: The fraction of lookups that fail as a function of the rate (over time) at '
                 'which nodes fail and join.')
        x_label = 'Node Fail/Join Rate (per second)'
        x_col = 'churn_rate'

    else:
        raise NotImplementedError

    df = pd.read_csv(INPUT_PATH + f'figure_{fig_no}_data.csv')

    confidence_interval_95 = 1.96 * df['std'] / np.sqrt(no_of_replicates)

    # -- plot --
    fig, ax = plt.subplots(figsize=(10, 8))

    ax.errorbar(
        df[x_col],
        df['mean'],
        yerr=confidence_interval_95,
        fmt='D',
        color='k',
        linestyle='--',
        mfc='none',
        markersize=6,
        capsize=4,
        capthick=1,
        label='95% confidence interval'
    )

    ax.set_xlabel(x_label, fontsize=12)
    ax.set_ylabel("Failed Lookups (fraction of total)", fontsize=12)
    plt.title(title, fontsize=14)

    ax.legend(loc='upper left')
    plt.tight_layout()
    # plt.show()
    plt.savefig(OUTPUT_PATH + f'figure_{fig_no}.png', dpi=300, bbox_inches="tight")


if __name__ == '__main__':
    pass
    # plot_pdf(fig_no='8b')
    plot_95_percent_confidence_interval(fig_no='11')
