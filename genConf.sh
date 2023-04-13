
heapsize=524288000
hashpower=20

heapsize=$((heapsize * 2))
hashpower=$((hashpower + 1))

for n_thread in 1 2 4 8 16; do 
    heapsize_updated=$((heapsize * n_thread))
    hashpower_updated=$(echo "${hashpower} + l(${n_thread})/l(2)" | bc -l | cut -d'.' -f1)
    echo """
debug_logging: no
# trace_path: /disk/c.sbin
default_ttl_list: 864000:1
heap_mem: ${heapsize_updated}
hash_power: ${hashpower_updated}
seg_evict_opt: 5
n_thread:${n_thread}
seg_n_thread:${n_thread}
""" > seg_${heapsize}_${n_thread}.conf

numactl --membind=0 ./benchmarks/trace_replay_seg seg_${heapsize}_${n_thread}.conf | tee -a seg4.log
done
