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

echo "SCRIPT_DIR: ${SCRIPT_DIR}"
echo "SBATCH_SCRIPT: ${SBATCH_SCRIPT}"

############################################
# User-fillable paths (edit these)
############################################

# Where IOR should create its *test files* for each storage.
# These MUST be on the storage being benchmarked.
BEEGFS_TEST_ROOT="${BEEGFS_TEST_ROOT:-/pscratch/sd/j/johnpzh}"
SSD_TEST_ROOT="${SSD_TEST_ROOT:-/images}"
NFS_TEST_ROOT="${NFS_TEST_ROOT:-/global/cfs/cdirs/m5314/zhen.peng}"
TMPFS_TEST_ROOT="${TMPFS_TEST_ROOT:-/tmp}"

echo "BEEGFS_TEST_ROOT: ${BEEGFS_TEST_ROOT}"
echo "SSD_TEST_ROOT: ${SSD_TEST_ROOT}"
echo "NFS_TEST_ROOT: ${NFS_TEST_ROOT}"
echo "TMPFS_TEST_ROOT: ${TMPFS_TEST_ROOT}"

# Where JSON+logs will be written. This can be anywhere with enough space.
# Recommended: a shared filesystem (e.g., scratch) so results persist.
RESULTS_ROOT="${RESULTS_ROOT:-/global/cfs/cdirs/m5314/zhen.peng/Data/ior_results_root}"

echo "RESULTS_ROOT: ${RESULTS_ROOT}"

# Optional: module load IOR if needed (leave empty if `ior` is already in PATH).
MODULE_LOAD_CMD="${MODULE_LOAD_CMD:-}"

# Optional: set a specific ior binary.
IOR_BIN="${IOR_BIN:-/global/homes/j/johnpzh/local/bin/ior}"

# for perlmutter, the max jobs is 5000
# stay under 5000 limit with margin; 
# because array_spec is 0-1919 or 0-1199, 1 line is about 2000 jobs, so 1 job at a time. 
# If there are 2 lines, the `NODES NODELIST(REASON)` becomes `(None)`, which seems not good.
MAX_JOBS=1
SLEEP_SEC=120

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

# Split the logical sweep into multiple Slurm job arrays with at most this many
# array tasks per sbatch (e.g. 5 => sbatch --array=0-4, then 5-9, ...).
# Set to 0 for one sbatch with the full 0-(total-1) range (previous behavior).
ARRAY_CHUNK_SIZE="${ARRAY_CHUNK_SIZE:-5}"

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
  elif [[ "${storage}" == "nfs" ]]; then
    transfer_count=5   # 64B, 1KB, 1MB, 64MB, 256MB
    agg_count=3        # 1GB, 5GB, 50GB
    tasks_count=8      # 2,4,16,32,256,512,1024,2048
  elif [[ "${storage}" == "tmpfs" ]]; then
    transfer_count=5   # 64B, 1KB, 4KB, 64MB, 1GB
    agg_count=3        # 1GB, 5GB, 50GB
    tasks_count=5      # 2, 4, 32, 128, 1024
  else
    echo "Unknown storage '${storage}'" >&2
    exit 2
  fi

  echo $((pattern_count * transfer_count * agg_count * tasks_count * REPS))
}

wait_for_slot() {
  while true; do
    n=$(squeue -u "$USER" -h -t PD,R,CG,CF,RS 2>/dev/null | wc -l)
    echo "running jobs: ${n}"
    if [ "$n" -lt "$MAX_JOBS" ]; then
      echo "less than MAX_JOBS: ${MAX_JOBS}, submitting more..."
      break
    else
      echo "more than MAX_JOBS: ${MAX_JOBS}, waiting for ${SLEEP_SEC} seconds..."
    fi
    sleep "$SLEEP_SEC"
  done
}

submit_one() {
  local storage="$1"
  local nodes="$2"
  local test_root="$3"

  local total
  total="$(compute_array_len "${storage}")"
  local last=$((total - 1))
  if [[ "${last}" -lt 0 ]]; then
    echo "Nothing to run for ${storage} N=${nodes} (total=${total})" >&2
    return 0
  fi

  local capped_last="${last}"
  if [[ "${MAX_ARRAY_TASKS}" != "0" && "${MAX_ARRAY_TASKS}" -gt 0 ]]; then
    local cap_last=$((MAX_ARRAY_TASKS - 1))
    if (( cap_last < capped_last )); then
      capped_last="${cap_last}"
      echo "Capping ${storage} N=${nodes} indices to 0-${capped_last} (full grid would end at ${last})"
    fi
  fi
  if (( capped_last < 0 )); then
    echo "MAX_ARRAY_TASKS cap left nothing to run for ${storage} N=${nodes}" >&2
    return 0
  fi

  # Submit only a chunk of the whole job array. The premium qos has a limit of 5 jobs. So default chunk size is 5.
  submit_array_chunk() {
    local chunk_start="$1"
    local chunk_end="$2"
    local array_spec="${chunk_start}-${chunk_end}"
    echo "Submitting ${storage} N=${nodes}: array=${array_spec} (chunk of full 0-${capped_last})"
    set -x
    sbatch --nodes="${nodes}" \
      --array="${array_spec}" \
      --export=ALL,STORAGE_TYPE="${storage}",TEST_ROOT="${test_root}",RESULTS_ROOT="${RESULTS_ROOT}",REPS="${REPS}",PATTERNS_CSV="${PATTERNS_CSV}",MODULE_LOAD_CMD="${MODULE_LOAD_CMD}",IOR_BIN="${IOR_BIN}" \
      --qos=premium \
      --account=m5314 \
      --time=04:00:00 \
      --constraint=cpu \
      --output=output.storage-${storage}.nodes-${nodes}.%x.%A.%a.out.log \
      --error=output.storage-${storage}.nodes-${nodes}.%x.%A.%a.err.log \
      --mail-type=FAIL \
      --mail-user=zhen.peng@pnnl.gov \
      --exclusive \
      "${SBATCH_SCRIPT}"
    set +x
    wait_for_slot
  }

  if [[ "${ARRAY_CHUNK_SIZE}" == "0" || "${ARRAY_CHUNK_SIZE}" -ge "${capped_last}" ]]; then
    submit_array_chunk 0 "${capped_last}"
  else
    local s=0
    while (( s <= capped_last )); do
      local e=$((s + ARRAY_CHUNK_SIZE - 1))
      if (( e > capped_last )); then e="${capped_last}"; fi
      submit_array_chunk "${s}" "${e}"
      s=$((e + 1))
    done
  fi
}

############################################
# Submissions
############################################

for n in "${NUM_NODES_LIST[@]}"; do
  submit_one "beegfs" "${n}" "${BEEGFS_TEST_ROOT}"
  submit_one "ssd" "${n}" "${SSD_TEST_ROOT}"
  submit_one "nfs" "${n}" "${NFS_TEST_ROOT}"
  submit_one "tmpfs" "${n}" "${TMPFS_TEST_ROOT}"
done

echo "All submissions queued."

