set -eu

#------------------------#
# Options Need to be Set #
#------------------------#

STORAGE_OPTION="tmpfs"
NODES_OPTION=1
STORAGEROOT_OPTION="/tmp/ior_test_root"

#---------------#
# Fixed Options #
#---------------#
export MPICH_GPU_SUPPORT_ENABLED=0

curr_id=$(date +%Y-%m-%dT%H.%M.%S)
workspace_dir="output.workspace.storage-${STORAGE_OPTION}_nodes-${NODES_OPTION}.$curr_id"
output_file="output.storage-${STORAGE_OPTION}_nodes-${NODES_OPTION}.$curr_id.log"

#---------#
# Running #
#---------#

mkdir -p "${workspace_dir}"
cd "${workspace_dir}"

set -x
bash ../../submit_ior_sweep.v2.with_options.sh \
  --storage "${STORAGE_OPTION}" \
  --nodes "${NODES_OPTION}" \
  --storageRoot "${STORAGEROOT_OPTION}" \
  2>&1 | tee "../${output_file}"
set +x