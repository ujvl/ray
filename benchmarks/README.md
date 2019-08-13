# Lineage stash benchmarks

## Common setup

Time estimates are listed in parentheses.

1. (5min) For the benchmarks below, I've built an AMI that you can use which has Ray installed, as well as the other baselines and their dependencies (e.g., Flink, Hadoop, OpenMPI).
    Please contact me (swang@cs.berkeley.edu) so that I can share the AMI with you.

    If you would like to build your own image, I recommend that you start with Ubuntu 16.04 and make sure that you can run standalone clusters for Flink, Hadoop, and OpenMPI.
    You will also need to clone this repository, as well as this the [flink-wordcount repository](https://github.com/stephanie-wang/flink-wordcount).

2. (10min) Installing Ray.
    All of the following benchmarks are run with the Ray autoscaler, a utility for launching clusters and deploying Ray jobs from your local machine.
    Before running any of the following commands, please install Ray locally by following the instructions [here](https://github.com/stephanie-wang/ray/blob/lineage-stash/doc/source/installation.rst#building-ray-from-source).
    Make sure to follow the instructions for "Building Ray from source".

3. (up to 1 day, if AWS limit requests required) We'll be using AWS EC2 for all experiments.
    You can check out the autoscaler configurations for the clusters we'll be deploying in `ray/benchmarks/cluster-scripts/*.yaml`.
    The default AWS region in the included autoscaler configs is `us-west-2`, but you can always replace this with your preferred region (just search and replace `us-west-2` in the .yaml files).
    Here are the minimum instance types that we required for the lineage stash paper, but note that you can always decrease the quantity that you use and run a smaller version of the experiment.
    Also, we have listed which instances we recommend you run as spot requests, but note that you can always run with on-demand instead.
    Please check your AWS EC2 dashboard to make sure that your minimum instance limits match these in your specified region.

    | Instance type | On-demand or spot | Quantity |
    | ------------- |:-------------:|:-----:|
    |  m5.4xlarge | on-demand | 1 |
    |  m5.2xlarge | spot | 64 |
    |  m5.xlarge | spot | 32 |

4. (10min) Next, we'll walk through setting up a basic cluster with the autoscaler to get you started.
    Create your first cluster with the `ray up` command.
    ```bash
    cd ray/benchmarks
    ray up -y cluster-scripts/test.yaml
    ```
    This will create a cluster with 2 nodes, one of which will be designated the "head node".
    You should see some output as the autoscaler sets up your cluster, and eventually there should be a message explaining how to SSH into your cluster and run commands.
    You can also check out your EC2 console to make sure that you see the running instances.
    They should be labeled with something like `ray-test-worker` or `ray-test-head`.
    
    For your convenience, the clusters for the benchmarks below will be setup with the script `ray/benchmarks/cluster-scripts/setup_cluster.sh`, which calls `ray up` internally, gathers the workers' IP addresses, and makes sure all workers have the same software.

5. (5min) Now that you've created your first cluster, the Ray autoscaler should have automatically created a new `.pem` file for you in your `~/.ssh` directory.
    It should look something like `ray-autoscaler_1_<region>.pem`.
    It's not strictly necessary to run the benchmarks, but please add this identity to your SSH agent and make sure you have SSH agent forwarding setup on your local machine to make the cluster setup smoother.

6. (5min) You can now tear down your test cluster and get started on the benchmarks!
    ```bash
    ray down cluster-scripts/test.yaml
    ```

## Microbenchmarks
## Allreduce benchmark
## Streaming benchmark

In this benchmark, we will run a streaming wordcount job on Ray and on Flink.
We'll collect the latency distribution when there are no failures, as well as the latency and throughput when a failure is introduced partway between checkpoints.

1. (5min) Make sure you are in the `cluster-scripts` directory for all following commands, and start the cluster with:
    ```bash
    cd ray/benchmarks/cluster-scripts
    bash setup_cluster.sh streaming.yaml 4
    ```
2. (15min) Attach to the cluster, and run the benchmark.
    `ray attach` will connect you to a `screen` session on the head node of the cluster, so you can disconnect while the benchmark runs if you want.
    Just run `ray attach` a second time to reconnect later.
    ```bash
    ray attach streaming.yaml
    bash ~/ray/benchmarks/cluster-scripts/run_streaming_benchmark.sh  # This should run on the head node.
    ```
3. (5min) Once the command is complete, make sure you have the correct output.
    In `~/flink-wordcount/`, there should be 4 `.csv` files, with names like this:
    * failure-flink-latency-4-workers-37500-tput-30-checkpoint-Aug-13-47-50.csv
    * failure-flink-throughput-4-workers-37500-tput-30-checkpoint-Aug-13-47-50.csv
    * flink-latency-4-workers-50000-tput-Aug-13-44-53.csv
    * flink-throughput-4-workers-50000-tput-Aug-13-44-53.csv

    In `~/ray/benchmarks/cluster-scripts/`, there should be 4 `.csv` files, with names like this:
    * failure-latency-4-workers-8-shards-1000-batch-37500-tput-30-checkpoint-Aug-13-52-40.csv
    * failure-throughput-4-workers-8-shards-1000-batch-37500-tput-30-checkpoint-Aug-13-52-40.csv
    * latency-4-workers-8-shards-1000-batch-50000-tput-Aug-13-50-15.csv
    * throughput-4-workers-8-shards-1000-batch-50000-tput-Aug-13-50-15.csv

    Copy the output to your local directory by running:
    ```bash
    scp ubuntu@`ray get_head_ip streaming.yaml`:~/flink-wordcount/*.csv .
    scp ubuntu@`ray get_head_ip streaming.yaml`:~/ray/benchmarks/cluster-scripts/*.csv .
    ```

4. (5min) Plot the results!
    To plot the latency results, find the output filenames that match `flink-latency-*` and `latency-*`.
    We'll pass these into the plotting script.
    For example, you can generate the plots included in `ray/data/streaming` with:
    ```bash
    python plot_latency_cdf.py \
        --flink-filename flink-latency-4-workers-50000-tput-Aug-13-44-53.csv \
        --lineage-stash-filename latency-4-workers-8-shards-1000-batch-50000-tput-Aug-13-50-15.csv
    ```
    This command produces a graph like this:
    ![](https://github.com/stephanie-wang/ray/blob/lineage-stash/data/streaming/latency-4-workers.png "Latency")

    To plot the results from the recovery experiment, find the output filenames that match `failure-flink-latency-*` and `failure-latency-*`.
    We'll pass these into the plotting script.
    For example:
    ```bash
    python plot_recovery.py \
        --flink-filename failure-flink-latency-4-workers-37500-tput-30-checkpoint-Aug-13-47-50.csv \
        --lineage-stash-filename failure-latency-4-workers-8-shards-1000-batch-37500-tput-30-checkpoint-Aug-13-52-40.csv
    ```
    This command produces two graphs, one for latency and one for throughput, like this:
    ![](https://github.com/stephanie-wang/ray/blob/lineage-stash/data/streaming/latency-recovery-4-workers.png "Latency during recovery")
    ![](https://github.com/stephanie-wang/ray/blob/lineage-stash/data/streaming/latency-throughput-4-workers.png "Throughput during recovery")

5. Finally, tear down the cluster with `ray down`:
    ```bash
    ray down streaming.yaml
    ```
