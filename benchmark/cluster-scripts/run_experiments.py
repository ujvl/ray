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

TARGET_THROUGHPUTS = [5000, 6000, 7000, 8000, 9000, 10000]
RAYLETS = [1, 2, 4, 8, 12]
SHARDS = [1, 2, 4, 8, 16]


def get_filename(num_raylets, lineage_cache_policy, gcs_delay,
        num_redis_shards, target_throughput):
    filename = "{}-raylets-{}-policy-{}-gcs-{}-shards-{}-throughput.out".format(
            num_raylets, lineage_cache_policy, gcs_delay, num_redis_shards,
            target_throughput)
    return filename

def get_csv_filename(lineage_cache_policy, gcs_delay):
    if gcs_delay != -1:
        filename = "gcs.csv"
    else:
        filename = "{}.csv".format(LINEAGE_CACHE_POLICIES[lineage_cache_policy])
    return filename

def parse_experiment_throughput(num_raylets, lineage_cache_policy, gcs_delay, num_redis_shards, target_throughput):
    filename = get_filename(num_raylets, lineage_cache_policy, gcs_delay, num_redis_shards,
                            target_throughput)
    try:
        with open(filename, 'r') as f:
            header = f.readline()
            if not header.startswith('DONE'):
                return -1
            throughput = float(header.split()[6])
            if f.readline():
                # If the next line is not empty, then there were logs from the load
                # building up.
                return -1
            else:
                return throughput
    except:
        return -1

def parse_experiments(lineage_cache_policy, gcs_delay):
    filename = get_csv_filename(lineage_cache_policy, gcs_delay)
    with open(filename, 'w') as f:
        fieldnames = [
            'num_shards',
            'num_raylets',
            'target_throughput',
            'throughput'
            ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for num_redis_shards in SHARDS:
            for num_raylets in RAYLETS:
                for target_throughput in TARGET_THROUGHPUTS:
                    throughput = parse_experiment_throughput(num_raylets,
                            lineage_cache_policy, gcs_delay, num_redis_shards,
                            target_throughput)
                    writer.writerow({
                        'num_shards': num_redis_shards,
                        'num_raylets': num_raylets,
                        'target_throughput': target_throughput * num_raylets,
                        'throughput': throughput,
                        })

def parse_all_experiments():
    parse_experiments(0, -1)
    parse_experiments(1, -1)
    parse_experiments(2, -1)
    parse_experiments(0, 0)

def run_experiment(num_raylets, lineage_cache_policy, gcs_delay, num_redis_shards, target_throughput):
    filename = get_filename(num_raylets, lineage_cache_policy, gcs_delay, num_redis_shards,
                            target_throughput)
    print("Running experiment, logging to {}".format(filename))
    command = [
            "bash",
            "./run_job.sh",
            str(num_raylets),
            str(lineage_cache_policy),
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
    for policy in [0, 1, 2]:
        for num_redis_shards in SHARDS:
            for target_throughput in TARGET_THROUGHPUTS:
                for num_raylets in RAYLETS:
                    run_experiment(num_raylets, policy, -1, num_redis_shards, target_throughput)

    for num_redis_shards in SHARDS:
        for target_throughput in TARGET_THROUGHPUTS:
            for num_raylets in RAYLETS:
                run_experiment(num_raylets, 0, 0, num_redis_shards, target_throughput)


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
