#!/bin/bash
#
# IOR parameter sweep (JSON output) for a single storage + node count.
#
# Usage (example):
#   sbatch -N 4 -t 02:00:00 perf_profiles/run_ior_sweep.slurm
#
# Then override variables via --export, e.g.:
#   sbatch -N 4 -t 02:00:00 \
#     --export=ALL,STORAGE_TYPE=beegfs,TEST_ROOT=/pscratch/sd/$USER/ior_test,RESULTS_ROOT=/pscratch/sd/$USER/ior_results \
#     perf_profiles/run_ior_sweep.slurm
#
# Notes:
# - This script writes IOR *data files* under $TEST_ROOT and writes JSON+logs under $RESULTS_ROOT.
# - It produces one JSON file per (pattern, transfer, aggSize, numTasks, rep, op) run.
# - It is portable: set IOR_BIN / module loads for your cluster.
#
#SBATCH -J ior-sweep
#SBATCH -o %x-%j.out
#SBATCH -e %x-%j.err
#
# Recommended: run as a job array to split configs.
# Example: sbatch -N 4 --array=0-99 ... (see submit script)

set -eu

TOTAL_TIME_START=$(date +%s.%N)

############################################
# User-fillable cluster/environment settings
############################################

# Path to the IOR executable (or rely on PATH after module load).
IOR_BIN="${IOR_BIN:-ior}"

# Optional module commands for systems that use environment modules.
# Example (Perlmutter): module load ior
MODULE_LOAD_CMD="${MODULE_LOAD_CMD:-}"

# Where IOR should create its test files (MUST be on the target storage).
# Fill this in with the storage-specific path you want to benchmark.
TEST_ROOT="${TEST_ROOT:-/path/to/STORAGE/ior_test}"

# Where to store JSON outputs + logs (can be different from TEST_ROOT).
RESULTS_ROOT="${RESULTS_ROOT:-/path/to/store/ior_results}"

echo "#################"
echo "#### Inside ${BASH_SOURCE[0]} ####"
echo "IOR_BIN=${IOR_BIN}"
echo "TEST_ROOT=${TEST_ROOT}"
echo "RESULTS_ROOT=${RESULTS_ROOT}"
echo "#### End Inside ${BASH_SOURCE[0]} ####"
echo "#################"

# Storage label used in output directory names; expected by ior_utils.py:
# it extracts storageType from directory prefix before first underscore.
STORAGE_TYPE="${STORAGE_TYPE:-beegfs}"   # e.g. beegfs or ssd

# Repetitions per unique configuration (from perlmutter_storage.md tables).
REPS="${REPS:-5}"

# Which I/O patterns to run. "sequential" => IOR default; "random" => adds -z.
PATTERNS_CSV="${PATTERNS_CSV:-sequential,random}"

# Whether to run write and read for each config.
RUN_WRITE="${RUN_WRITE:-1}"
RUN_READ="${RUN_READ:-1}"

# IOR API/backend. POSIX is the most portable default.
IOR_API="${IOR_API:-POSIX}"

# IOR additional flags (advanced tuning). Keep empty by default for portability.
# Examples: "-k -e" (keep file, fsync), "-O stoneWallingStatusFile=..."
IOR_EXTRA_FLAGS="${IOR_EXTRA_FLAGS:-}"

############################################
# Parameter grids (from perlmutter_storage.md)
############################################

# NOTE: numNodes comes from the Slurm allocation (-N). This script records it in the output path.
# To sweep multiple node counts, submit multiple jobs (see submit_ior_sweep.sh).

# These defaults match the markdown tables for beegfs + ssd.
# You can override any of these via --export, e.g. TRANSFER_SIZES_CSV="1m,64m".

if [[ "${STORAGE_TYPE}" == "beegfs" ]]; then
  TRANSFER_SIZES_CSV="${TRANSFER_SIZES_CSV:-64b,1k,1m,64m,256m}"
  AGG_FILE_SIZES_GB_CSV="${AGG_FILE_SIZES_GB_CSV:-1,5,50}"
  NUM_TASKS_CSV="${NUM_TASKS_CSV:-2,4,16,32,256,512,1024,2048}"
elif [[ "${STORAGE_TYPE}" == "ssd" ]]; then
  TRANSFER_SIZES_CSV="${TRANSFER_SIZES_CSV:-64b,1k,4k,16m,256m,1g}"
  AGG_FILE_SIZES_GB_CSV="${AGG_FILE_SIZES_GB_CSV:-1,5,50,100}"
  NUM_TASKS_CSV="${NUM_TASKS_CSV:-2,4,8,32,64,128,2048,4096}"
elif [[ "${STORAGE_TYPE}" == "nfs" ]]; then
  TRANSFER_SIZES_CSV="${TRANSFER_SIZES_CSV:-64b,1k,1m,64m,256m}"
  AGG_FILE_SIZES_GB_CSV="${AGG_FILE_SIZES_GB_CSV:-1,5,50}"
  NUM_TASKS_CSV="${NUM_TASKS_CSV:-2,4,16,32,256,512,1024,2048}"
elif [[ "${STORAGE_TYPE}" == "tmpfs" ]]; then
  TRANSFER_SIZES_CSV="${TRANSFER_SIZES_CSV:-64b,1k,4k,16m,1g}"
  AGG_FILE_SIZES_GB_CSV="${AGG_FILE_SIZES_GB_CSV:-1,5,50}"
  NUM_TASKS_CSV="${NUM_TASKS_CSV:-2,4,32,128,1024}"
else
  # Generic fallback for other clusters/storages.
  TRANSFER_SIZES_CSV="${TRANSFER_SIZES_CSV:-1m,64m}"
  AGG_FILE_SIZES_GB_CSV="${AGG_FILE_SIZES_GB_CSV:-1,5}"
  NUM_TASKS_CSV="${NUM_TASKS_CSV:-2,8,32}"
fi

############################################
# Helpers
############################################

split_csv() {
  local csv="$1"
  # shellcheck disable=SC2206
  IFS=',' read -r -a _arr <<< "${csv}"
  printf '%s\n' "${_arr[@]}"
}

bytes_from_size_token() {
  # Converts tokens like: 64b, 1k, 4k, 1m, 64m, 256m, 1g into bytes (integer).
  # Input is case-insensitive.
  local tok
  tok="$(echo "$1" | tr '[:upper:]' '[:lower:]')"
  if [[ "${tok}" =~ ^([0-9]+)(b|k|m|g)$ ]]; then
    local n="${BASH_REMATCH[1]}"
    local u="${BASH_REMATCH[2]}"
    case "${u}" in
      b) echo $((n)) ;;
      k) echo $((n * 1024)) ;;
      m) echo $((n * 1024 * 1024)) ;;
      g) echo $((n * 1024 * 1024 * 1024)) ;;
    esac
  else
    echo "ERROR: Unsupported size token '${tok}' (expected e.g. 64b, 1k, 1m, 1g)" >&2
    return 2
  fi
}

join_by() { local IFS="$1"; shift; echo "$*"; }

############################################
# Setup
############################################

if [[ -n "${MODULE_LOAD_CMD}" ]]; then
  # shellcheck disable=SC2086
  eval "${MODULE_LOAD_CMD}"
fi

command -v "${IOR_BIN}" >/dev/null 2>&1 || {
  echo "ERROR: IOR not found: IOR_BIN='${IOR_BIN}'" >&2
  exit 1
}

NUM_NODES="${SLURM_JOB_NUM_NODES:-1}"
TASKS_PER_NODE_DEFAULT="${TASKS_PER_NODE_DEFAULT:-1}"
TOTAL_TASKS_DEFAULT="$((NUM_NODES * TASKS_PER_NODE_DEFAULT))"

mkdir -p "${RESULTS_ROOT}"
mkdir -p "${TEST_ROOT}"

OUT_DIR="${RESULTS_ROOT}/ior_data/${STORAGE_TYPE}_pior_buffered_${NUM_NODES}n"
mkdir -p "${OUT_DIR}"

############################################
# Build config list (optionally for job arrays)
############################################

mapfile -t PATTERNS < <(split_csv "${PATTERNS_CSV}")
mapfile -t TRANSFER_SIZES < <(split_csv "${TRANSFER_SIZES_CSV}")
mapfile -t AGG_GB < <(split_csv "${AGG_FILE_SIZES_GB_CSV}")
mapfile -t NUM_TASKS_LIST < <(split_csv "${NUM_TASKS_CSV}")

CONFIGS=()
for pattern in "${PATTERNS[@]}"; do
  for tr in "${TRANSFER_SIZES[@]}"; do
    for agg_gb in "${AGG_GB[@]}"; do
      for ntasks in "${NUM_TASKS_LIST[@]}"; do
        for rep in $(seq 1 "${REPS}"); do
          CONFIGS+=("${pattern}|${tr}|${agg_gb}|${ntasks}|${rep}")
        done
      done
    done
  done
done

IDX="${SLURM_ARRAY_TASK_ID:-}"
if [[ -n "${IDX}" ]]; then
  if (( IDX < 0 || IDX >= ${#CONFIGS[@]} )); then
    echo "ERROR: SLURM_ARRAY_TASK_ID=${IDX} out of range (0..$(( ${#CONFIGS[@]} - 1 )))" >&2
    exit 2
  fi
  CONFIGS=( "${CONFIGS[$IDX]}" )
fi

############################################
# Run sweep
############################################

echo "IOR sweep starting"
echo "  STORAGE_TYPE=${STORAGE_TYPE}"
echo "  NUM_NODES=${NUM_NODES}"
echo "  TEST_ROOT=${TEST_ROOT}"
echo "  RESULTS_ROOT=${RESULTS_ROOT}"
echo "  OUT_DIR=${OUT_DIR}"
echo "  PATTERNS=${PATTERNS_CSV}"
echo "  TRANSFER_SIZES=${TRANSFER_SIZES_CSV}"
echo "  AGG_FILE_SIZES_GB=${AGG_FILE_SIZES_GB_CSV}"
echo "  NUM_TASKS=${NUM_TASKS_CSV}"
echo "  REPS=${REPS}"
if [[ -n "${IDX}" ]]; then
  echo "  Array mode: running config index ${IDX} only"
else
  echo "  Non-array mode: running ${#CONFIGS[@]} configs (may be very large)"
fi

echo "CONFIGS=${CONFIGS[@]}"

for cfg in "${CONFIGS[@]}"; do
  IFS='|' read -r pattern tr agg_gb ntasks rep <<< "${cfg}"

  # Convert transfer token to MiB label used in existing filenames (ior_utils.py doesn't depend on it, but README examples do).
  # For tiny sizes (B/KB), keep a descriptive token.
  tr_lc="$(echo "${tr}" | tr '[:upper:]' '[:lower:]')"
  tr_tag="${tr_lc}"
  if [[ "${tr_lc}" =~ ^([0-9]+)m$ ]]; then
    tr_tag="${BASH_REMATCH[1]}m"
  elif [[ "${tr_lc}" =~ ^([0-9]+)g$ ]]; then
    tr_tag="$((BASH_REMATCH[1] * 1024))m"
  fi

  agg_bytes=$((agg_gb * 1024 * 1024 * 1024))
  per_task_bytes=$((agg_bytes / ntasks))

  if (( per_task_bytes <= 0 )); then
    echo "Skipping invalid config (aggregate too small): ${cfg}" >&2
    continue
  fi

  # Ensure per-task size is at least transfer size.
  tr_bytes="$(bytes_from_size_token "${tr_lc}")"
  if (( per_task_bytes <= tr_bytes )); then
    echo "Skipping config where per-task size <= transfer: ${cfg} (per_task_bytes=${per_task_bytes}, tr_bytes=${tr_bytes})" >&2
    continue
  fi
  if (( per_task_bytes % tr_bytes != 0 )); then
    # Round up to prevent IOR failure:
    # "ERROR: block size must be a multiple of transfer size, (ior.c:1520)"
    old_per_task_bytes="${per_task_bytes}"
    per_task_bytes="$(( ((per_task_bytes + tr_bytes - 1) / tr_bytes) * tr_bytes ))"
    echo "Adjusted per-task size to next multiple of transfer: ${cfg} (old_per_task_bytes=${old_per_task_bytes}, per_task_bytes=${per_task_bytes}, tr_bytes=${tr_bytes})" >&2
  fi

  # Keep file naming compatible with existing analysis conventions.
  # Example: pior_64m_1gb_n8_1.json
  # For random, we include a marker but keep the _<N>gb_ and _n<T>_ tokens intact.
  base_name="pior_${tr_tag}_${agg_gb}gb_n${ntasks}_${rep}"
  if [[ "${pattern}" == "random" ]]; then
    base_name="pior_rand_${tr_tag}_${agg_gb}gb_n${ntasks}_${rep}"
  fi

  # Per-config working directory for IOR data files.
  work_dir="${TEST_ROOT}/${STORAGE_TYPE}/${NUM_NODES}n/${base_name}"
  mkdir -p "${work_dir}"

  common_flags=(
    -a "${IOR_API}"
    # -J 4k # This is for HDF5, not for POSIX.
    -t "${tr_lc}"
    -b "${per_task_bytes}"
    -s 1
    -F
    -o "${work_dir}/iorfile"
    -N "${ntasks}"
  )

  if [[ "${pattern}" == "random" ]]; then
    common_flags+=( -z )
  fi

  # shellcheck disable=SC2206
  extra_flags=( ${IOR_EXTRA_FLAGS} )

  if [[ "${RUN_WRITE}" == "1" ]]; then
    json_out="${OUT_DIR}/${base_name}_write.json"
    log_out="${OUT_DIR}/${base_name}_write.log"
    echo "Running WRITE: ${json_out}"
    set -x
    "${IOR_BIN}" "${common_flags[@]}" -w -k "${extra_flags[@]}" > "${json_out}" 2> "${log_out}" # -k is for keep file for the following read.
    set +x
  fi

  if [[ "${RUN_READ}" == "1" ]]; then
    json_out="${OUT_DIR}/${base_name}_read.json"
    log_out="${OUT_DIR}/${base_name}_read.log"
    echo "Running READ: ${json_out}"
    set -x
    "${IOR_BIN}" "${common_flags[@]}" -r "${extra_flags[@]}" > "${json_out}" 2> "${log_out}"
    set +x
  fi

done

echo "IOR sweep complete"

TOTAL_TIME_END=$(date +%s.%N)
TOTAL_TIME_ELAPSED=$(echo "${TOTAL_TIME_END} - ${TOTAL_TIME_START}" | bc -l)
echo "Total time elapsed: ${TOTAL_TIME_ELAPSED} seconds"



