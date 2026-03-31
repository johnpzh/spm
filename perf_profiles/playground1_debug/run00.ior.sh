set -eu

export MPICH_GPU_SUPPORT_ENABLED=0

curr_id=$(date +%Y-%m-%dT%H.%M.%S)
workspace_dir="output.workspace.submit_ior_sweep.$curr_id"
output_file="output.submit_ior_sweep.sh.$curr_id.log"

mkdir -p "${workspace_dir}"
cd "${workspace_dir}"

set -x
bash ../../submit_ior_sweep.sh 2>&1 | tee "../${output_file}"
set +x