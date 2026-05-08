data_dir="/global/cfs/cdirs/m5314/zhen.peng/Data/ior_results_root/ior_data"
storage_type="tmpfs"
transfer_size=64 # Bytes
num_nodes=1

set -x
python ../ior_analysis.py \
    --data-dir "${data_dir}" \
    --storage-type "${storage_type}" \
    --transfer-size "${transfer_size}" \
    --num-nodes "${num_nodes}"
set +x