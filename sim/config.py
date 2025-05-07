"""
Constants used.
"""

# Seed used for all experiments
SEED = 42

# Number of bits in the identifier/key space (m)
M = 160

# Base per‐hop network latency in seconds (5 ms)
BASE_LATENCY = 0.005

# Interval between periodic stabilization routines in seconds (30 s)
STABILIZE_INTERVAL = 1

# Number of seconds at the start of simulation to warm up Chord (ensure figures, successors, etc. are correct).
WARMUP_END = 10

# Size of the successor list (r) for fault tolerance
SUCCESSOR_LIST_SIZE = 16

# Experiment parameters
EXP_1_NO_OF_NODES = 10_000
EXP_1b_NO_OF_KEYS = 1_000_000
EXP_1b_NO_OF_VIRTUAL_NODES = [1, 2, 5, 10, 20]
EXP_1_REPLICATES = 20

EXP_2_REPLICATES = 10

EXP_3_NO_OF_NODES = 10_000
EXP_3_NO_OF_KEYS = 1_000_000
EXP_3_REPLICATES = 10
EXP_3_LIST_OF_FRAC_OF_FAILED_NODES = [0.01, 0.02, 0.05, 0.1, 0.15, 0.2]

EXP_4_NO_OF_NODES = 500
EXP_4_KEY_LOOKUP_RATE = 1
EXP_4_FAIL_JOIN_RATE = [0.01, 0.02, 0.05, 0.1]
EXPT_4_SIMULATION_TIME = 7_200   # 7_200 seconds = 2 hours.
EXP_4_REPLICATES = 10
