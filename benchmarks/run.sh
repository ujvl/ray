NUM_RAYLETS_L='4 8 16 32 64'
FLUSH_POLICY_L='0 1 2'
GCS_DELAY=0
NUM_SHARDS=2
TASK_DUR=0
OUTPUT_DIR=.

# Lineage stash
for flush_policy in $FLUSH_POLICY_L; do
    for num in $NUM_RAYLETS_L; do
        num_failures=$(( $num / 4 ))
        if [[ $num_failures -lt 1 ]]
        then
            num_failures=1
        fi
        # set workers.txt
        head -n 1 ~/og_workers.txt > ~/workers.txt
        tail -n $num ~/og_workers.txt >> ~/workers.txt
        # Run bench
        ./cluster-scripts/run_thput_microbenchmark_job.sh \
            $num `hostname -i` 0 $GCS_DELAY $NUM_SHARDS $TASK_DUR $num_failures $flush_policy $OUTPUT_DIR
    done
    mv thput.txt thput_"$flush_policy"_lineage_stash.txt
done

# GCS only
flush_policy=0
for num in $NUM_RAYLETS_L; do
    # set workers.txt
    head -n 1 ~/og_workers.txt > ~/workers.txt
    tail -n $num ~/og_workers.txt >> ~/workers.txt
    # Run bench
    ./cluster-scripts/run_thput_microbenchmark_job.sh \
        $num `hostname -i` 1 $GCS_DELAY $NUM_SHARDS $TASK_DUR 1 $flush_policy $OUTPUT_DIR
done
mv thput.txt thput_gcs_only.txt
