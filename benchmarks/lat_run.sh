NUM_RAYLETS_L='64'
GCS_DELAY=1
NUM_SHARDS=1
TASK_DUR=0
OUTPUT_DIR=.
FLUSH_POLICY='0 1 2'

# GCS only
#for num in $NUM_RAYLETS_L; do
#    # set workers.txt
#    head -n 1 ~/og_workers.txt > ~/workers.txt
#    tail -n $num ~/og_workers.txt >> ~/workers.txt
#    # Run bench
#    ./cluster-scripts/run_lat_microbenchmark_job.sh \
#        $num `hostname -i` 1 $GCS_DELAY $NUM_SHARDS $TASK_DUR 1 0 $OUTPUT_DIR
#done

MAX_FAILURES=16
# Lineage stash w/ flushing
for num in $NUM_RAYLETS_L; do
    # set workers.txt
    head -n 1 ~/og_workers.txt > ~/workers.txt
    tail -n $num ~/og_workers.txt >> ~/workers.txt
    # Run bench
    ./cluster-scripts/run_lat_microbenchmark_job.sh \
        $num `hostname -i` 0 $GCS_DELAY $NUM_SHARDS $TASK_DUR $MAX_FAILURES 0 $OUTPUT_DIR
done

# Lineage stash w/ no flushing
#for num in $NUM_RAYLETS_L; do
#    # set workers.txt
#    head -n 1 ~/og_workers.txt > ~/workers.txt
#    tail -n $num ~/og_workers.txt >> ~/workers.txt
#    # Run bench
#    ./cluster-scripts/run_lat_microbenchmark_job.sh \
#        $num `hostname -i` 0 $GCS_DELAY $NUM_SHARDS $TASK_DUR $MAX_FAILURES 1 $OUTPUT_DIR
#done
