import argparse
import csv
import subprocess
import time
import os

EXPERIMENT_TIME = 60
SLEEP_TIME = 10
NUM_TRIALS = 100

LINEAGE_CACHE_POLICIES = [
        "lineage-cache",
        "lineage-cache-flush",
        "lineage-cache-k-flush",
        ]

STEP_SIZE = 100
TARGET_THROUGHPUTS = [2000]
RAYLETS = [32]
SHARDS = [1, 2, 4, 8, 16]
K = [100]


def get_filename():
    return "min_latency.csv"

def get_csv_filename(lineage_cache_policy, max_lineage_size, gcs_delay):
    if gcs_delay != -1:
        filename = "gcs.csv"
    elif max_lineage_size is None:
        filename = "{}.csv".format(LINEAGE_CACHE_POLICIES[lineage_cache_policy])
    else:
        filename = "{}-{}.csv".format(LINEAGE_CACHE_POLICIES[lineage_cache_policy], max_lineage_size)
    return filename

def parse_experiment_throughput(num_raylets, lineage_cache_policy,
        max_lineage_size, gcs_delay, num_redis_shards, target_throughput,
        trial):
    filename = get_filename(num_raylets, lineage_cache_policy,
            max_lineage_size, gcs_delay, num_redis_shards, target_throughput, trial)
    lineage_overloaded = False
    queue_overloaded = False
    timed_out = False

    try:
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
    except:
        return -1, lineage_overloaded, queue_overloaded, timed_out

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
                    trial = 0
                    (throughput, lineage_overloaded, queue_overloaded, timed_out) = parse_experiment_throughput(num_raylets,
                            lineage_cache_policy, max_lineage_size, gcs_delay, num_redis_shards,
                            target_throughput, trial)
                    while throughput == -1 and trial < NUM_TRIALS:
                        trial += 1
                        (throughput, lineage_overloaded, queue_overloaded, timed_out) = parse_experiment_throughput(num_raylets,
                                lineage_cache_policy, max_lineage_size, gcs_delay, num_redis_shards,
                                target_throughput, trial)
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
    max_throughputs = {
            }
    policies = [
            (0, -1),
            (1, -1),
            (2, -1),
            (0, 0),
            ]
    for policy, gcs_delay in policies:
        parse_experiments(policy, 100, gcs_delay)

def run_experiment(num_raylets, lineage_cache_policy, max_lineage_size, gcs_delay, num_redis_shards, target_throughput, trial, test_local):
    filename = get_filename()
    success = True
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
            filename,
            ]
    if test_local:
        command.append("local")
    else:
        command.append("remote")

    with open("job.out", 'a+') as f:
        pid = subprocess.Popen(command, stdout=f, stderr=f)
        start = time.time()

        time.sleep(SLEEP_TIME)
        sleep_time = SLEEP_TIME
        while pid.poll() is None and (time.time() - start) < EXPERIMENT_TIME:
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
            success = False

    return success

def run_all_experiments():
    filename = get_filename()
    max_lineage_size = K[0]
    policies = [
            (0, 0),
            (0, -1),
            ]
    for num_redis_shards in SHARDS:
        for policy, gcs_delay in policies:
            for num_raylets in RAYLETS:
                for target_throughput in TARGET_THROUGHPUTS:
                    # Run the trials.
                    for trial in range(NUM_TRIALS):
                        # Run one trial. Returns true if the experiment did not
                        # time out.
                        run_experiment(num_raylets, policy, max_lineage_size,
                                gcs_delay, num_redis_shards, target_throughput,
                                trial, True)
    for num_redis_shards in SHARDS:
        for policy, gcs_delay in policies:
            for num_raylets in RAYLETS:
                for target_throughput in TARGET_THROUGHPUTS:
                    # Run the trials.
                    for trial in range(NUM_TRIALS):
                        # Run one trial. Returns true if the experiment did not
                        # time out.
                        run_experiment(num_raylets, policy, max_lineage_size,
                                gcs_delay, num_redis_shards, target_throughput,
                                trial, False)

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
