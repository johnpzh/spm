#!/bin/bash
#
# Driver script to submit IOR sweeps across node counts and two storages.
#
# This submits `run_ior_sweep.slurm` as Slurm job arrays so the full grid
# from `perf_profiles/perlmutter_storage.md` can be executed in parallel.
#
# You MUST fill in the paths below for your cluster/user allocation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SBATCH_SCRIPT="${SCRIPT_DIR}/run_ior_sweep.slurm"

############################################
# User-fillable paths (edit these)
############################################

# Where IOR should create its *test files* for each storage.
# These MUST be on the storage being benchmarked.
BEEGFS_TEST_ROOT="${BEEGFS_TEST_ROOT:-/path/to/BEEGFS/ior_test}"
SSD_TEST_ROOT="${SSD_TEST_ROOT:-/path/to/SSD/ior_test}"

# Where JSON+logs will be written. This can be anywhere with enough space.
# Recommended: a shared filesystem (e.g., scratch) so results persist.
RESULTS_ROOT="${RESULTS_ROOT:-/path/to/store/ior_results}"

# Optional: module load IOR if needed (leave empty if `ior` is already in PATH).
MODULE_LOAD_CMD="${MODULE_LOAD_CMD:-}"

# Optional: set a specific ior binary.
IOR_BIN="${IOR_BIN:-ior}"

############################################
# Sweep controls
############################################

# From perlmutter_storage.md: test all available node counts.
# Adjust for your cluster/account/queue limits.
NUM_NODES_LIST=(${NUM_NODES_LIST:-1 2 4 8 16 32})

# Number of repetitions per unique configuration.
REPS="${REPS:-5}"

# Patterns to test (sequential + random).
PATTERNS_CSV="${PATTERNS_CSV:-sequential,random}"

# Limit submitted work if you want a smaller pilot.
# Set MAX_ARRAY_TASKS to a positive integer to cap array length (per job).
MAX_ARRAY_TASKS="${MAX_ARRAY_TASKS:-0}"

############################################
# Helpers
############################################

compute_array_len() {
  # Computes total configs per storage for a given node count:
  # patterns * transferSizes * aggSizes * numTasks * reps
  # (read+write are both done inside each config)
  local storage="$1"

  local transfer_count agg_count tasks_count pattern_count
  pattern_count=$(awk -F, '{print NF}' <<< "${PATTERNS_CSV}")

  if [[ "${storage}" == "beegfs" ]]; then
    transfer_count=5   # 64B, 1KB, 1MB, 64MB, 256MB
    agg_count=3        # 1GB, 5GB, 50GB
    tasks_count=8      # 2,4,16,32,256,512,1024,2048
  elif [[ "${storage}" == "ssd" ]]; then
    transfer_count=6   # 64B, 1KB, 4KB, 16MB, 256MB, 1GB
    agg_count=4        # 1GB, 5GB, 50GB, 100GB
    tasks_count=8      # 2,4,8,32,64,128,2048,4096
  else
    echo "Unknown storage '${storage}'" >&2
    exit 2
  fi

  echo $((pattern_count * transfer_count * agg_count * tasks_count * REPS))
}

submit_one() {
  local storage="$1"
  local nodes="$2"
  local test_root="$3"

  local total
  total="$(compute_array_len "${storage}")"
  local last=$((total - 1))
  local array_spec="0-${last}"
  if [[ "${MAX_ARRAY_TASKS}" != "0" && "${MAX_ARRAY_TASKS}" -gt 0 ]]; then
    local capped_last=$((MAX_ARRAY_TASKS - 1))
    if (( capped_last < last )); then
      array_spec="0-${capped_last}"
      echo "Capping ${storage} N=${nodes} array to ${array_spec} (of full 0-${last})"
    fi
  fi

  sbatch -N "${nodes}" \
    --array="${array_spec}" \
    --export=ALL,STORAGE_TYPE="${storage}",TEST_ROOT="${test_root}",RESULTS_ROOT="${RESULTS_ROOT}",REPS="${REPS}",PATTERNS_CSV="${PATTERNS_CSV}",MODULE_LOAD_CMD="${MODULE_LOAD_CMD}",IOR_BIN="${IOR_BIN}" \
    "${SBATCH_SCRIPT}"
}

############################################
# Submissions
############################################

for n in "${NUM_NODES_LIST[@]}"; do
  submit_one "beegfs" "${n}" "${BEEGFS_TEST_ROOT}"
  submit_one "ssd" "${n}" "${SSD_TEST_ROOT}"
done

echo "All submissions queued."

