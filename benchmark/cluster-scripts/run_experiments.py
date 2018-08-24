import argparse
import csv
import subprocess
import time

MAX_EXPERIMENT_TIME = 120
SLEEP_TIME = 10

LINEAGE_CACHE_POLICIES = [
        "lineage-cache",
        "lineage-cache-flush",
        "lineage-cache-k-flush",
        ]

TARGET_THROUGHPUTS = [6000, 6500, 7000, 7500, 8000]
RAYLETS = [12]
SHARDS = [12]
K = [1, 10, 100, 1000, 10000]


def get_filename(num_raylets, lineage_cache_policy, max_lineage_size,
        gcs_delay, num_redis_shards, target_throughput):
    if max_lineage_size is None:
        filename = "{}-raylets-{}-policy-{}-gcs-{}-shards-{}-throughput.out".format(
                num_raylets, lineage_cache_policy, gcs_delay, num_redis_shards,
                target_throughput)
    else:
        filename = "{}-raylets-{}-policy-{}-k-{}-gcs-{}-shards-{}-throughput.out".format(
                num_raylets, lineage_cache_policy, max_lineage_size, gcs_delay,
                num_redis_shards, target_throughput)
    return filename

def get_csv_filename(lineage_cache_policy, max_lineage_size, gcs_delay):
    if gcs_delay != -1:
        filename = "gcs.csv"
    elif max_lineage_size is None:
        filename = "{}.csv".format(LINEAGE_CACHE_POLICIES[lineage_cache_policy])
    else:
        filename = "{}-{}.csv".format(LINEAGE_CACHE_POLICIES[lineage_cache_policy], max_lineage_size)
    return filename

def parse_experiment_throughput(num_raylets, lineage_cache_policy, max_lineage_size, gcs_delay, num_redis_shards, target_throughput):
    filename = get_filename(num_raylets, lineage_cache_policy,
            max_lineage_size, gcs_delay, num_redis_shards, target_throughput)
    lineage_overloaded = False
    queue_overloaded = False
    timed_out = False

    with open(filename, 'r') as f:
        header = f.readline()
        if not header.startswith('DONE'):
            timed_out = True
            return -1, lineage_overloaded, queue_overloaded, timed_out
        throughput = float(header.split()[6])
        line = f.readline()
        while line:
            if "Lineage" in line:
                lineage_overloaded = True
                throughput = -1
            elif "Queue" in line:
                queue_overloaded = True
                throughput = -1
            line = f.readline()
        return throughput, lineage_overloaded, queue_overloaded, timed_out

def parse_experiments(lineage_cache_policy, max_lineage_size, gcs_delay):
    filename = get_csv_filename(lineage_cache_policy, max_lineage_size, gcs_delay)
    with open(filename, 'w') as f:
        fieldnames = [
            'num_shards',
            'num_raylets',
            'target_throughput',
            'throughput',
            'lineage',
            'queue',
            'timed_out',
            ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for num_redis_shards in SHARDS:
            for num_raylets in RAYLETS:
                for target_throughput in TARGET_THROUGHPUTS:
                    (throughput, lineage_overloaded, queue_overloaded, timed_out) = parse_experiment_throughput(num_raylets,
                            lineage_cache_policy, max_lineage_size, gcs_delay, num_redis_shards,
                            target_throughput)
                    writer.writerow({
                        'num_shards': num_redis_shards,
                        'num_raylets': num_raylets,
                        'target_throughput': target_throughput * num_raylets,
                        'throughput': throughput,
                        'lineage': lineage_overloaded,
                        'queue': queue_overloaded,
                        'timed_out': timed_out,
                        })

def parse_all_experiments():
    for k in K:
        parse_experiments(2, k, -1)

def run_experiment(num_raylets, lineage_cache_policy, max_lineage_size, gcs_delay, num_redis_shards, target_throughput):
    filename = get_filename(num_raylets, lineage_cache_policy,
                            max_lineage_size, gcs_delay, num_redis_shards,
                            target_throughput)
    print("Running experiment, logging to {}".format(filename))
    command = [
            "bash",
            "./run_job.sh",
            str(num_raylets),
            str(lineage_cache_policy),
            str(max_lineage_size),
            str(gcs_delay),
            str(num_redis_shards),
            str(target_throughput),
            filename
            ]
    with open("job.out", 'a+') as f:
        pid = subprocess.Popen(command, stdout=f, stderr=f)
        start = time.time()

        # Number of items is 200000.
        max_experiment_time = max(MAX_EXPERIMENT_TIME, (200000 / target_throughput) + 20)
        # Allow 5s for each pair of raylets to start.
        max_experiment_time += num_raylets * 5

        time.sleep(SLEEP_TIME)
        sleep_time = SLEEP_TIME
        while pid.poll() is None and (time.time() - start) < max_experiment_time:
            print("job took", sleep_time, "so far. Sleeping...")
            sleep_time += SLEEP_TIME
            time.sleep(SLEEP_TIME)

        if pid.poll() is None:
            pid.kill()
            time.sleep(1)
            pid.terminate()
            f.write("\n")
            f.write("ERROR: Killed job with output {}\n".format(filename))
            print("ERROR: Killed job with output {}\n".format(filename))
            return False
    return True

def run_all_experiments():
    policy = 2
    for k in K:
        for num_redis_shards in SHARDS:
            for target_throughput in TARGET_THROUGHPUTS:
                for num_raylets in RAYLETS:
                    run_experiment(num_raylets, policy, k, -1, num_redis_shards, target_throughput)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--target-throughput', type=int, default=3000)
    parser.add_argument('--parse', action='store_true')
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()

    if args.run:
        run_all_experiments()

    if args.parse:
        parse_all_experiments()
