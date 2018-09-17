import argparse
import csv
import subprocess
import time
import os
import redis
from collections import defaultdict
import numpy as np

EXPERIMENT_TIME = 180
SLEEP_TIME = 10
NUM_TRIALS = 100

WINDOWS = [10]
RAYLETS = [128]
SHARDS = [4]
USE_JSON = [True]
#KILL_MAPPER = [True, False]
KILL_MAPPER = [False]
USE_LINEAGE_STASH = [True, False]


def collect_redis_stats(redis_address, redis_port, campaign_ids=None):
    r = redis.StrictRedis(redis_address, port=redis_port)

    if campaign_ids is None:
        campaign_ids = []
        for key in r.keys():
            if b'seen_count' in r.hgetall(key):
                continue
            campaign_ids.append(key)

    latencies = []
    counts = defaultdict(int)
    for campaign_id in campaign_ids:
        windows = r.hgetall(campaign_id)
        for window, window_id in windows.items():
           seen, time_updated = r.hmget(window_id, [b'seen_count', b'time_updated'])
           seen = int(seen)
           time_updated = float(time_updated)
           window = float(window)
           counts[window] += seen
           latencies.append((window, time_updated - window))
    counts = sorted(counts.items(), key=lambda key: key[0])
    latencies = sorted(latencies, key=lambda key: key[0])
    print("Average latency:", np.mean([latency[1] for latency in latencies]))
    return latencies, counts

def write_stats(filename_prefix, redis_address, redis_port, campaign_ids=None):
    latencies, throughputs = collect_redis_stats(redis_address, redis_port, campaign_ids)
    with open("{}-latency.out".format(filename_prefix), 'w+') as f:
        for window, latency in latencies:
            f.write("{},{}\n".format(window, latency))
    with open("{}-throughput.out".format(filename_prefix), 'w+') as f:
        for window, throughput in throughputs:
            # The number of events is measured per window.
            throughput /= WINDOW_SIZE_SEC
            # We multiply by 3 because the filter step filters out 1/3 of the
            # events.
            throughput *= 3
            f.write("{},{}\n".format(window, throughput))


def get_filename(num_raylets, num_redis_shards, use_lineage_stash, window,
        mapper, json, trial):
    if use_lineage_stash:
        lineage_policy = "lineage"
    else:
        lineage_policy = "gcs"
    json_string = ""
    if json:
        json_string = "json-"
    if mapper:
        failure_type = "mapper"
    else:
        failure_type = "reducer"


    filename = "{}-raylets-{}-shards-{}-{}-window-{}-{}{}".format(
            num_raylets, num_redis_shards, lineage_policy, window, 
            failure_type, json_string, trial)
    return filename

def get_csv_filename(lineage_cache_policy, max_lineage_size, gcs_delay):
    if gcs_delay != -1:
        filename = "gcs.csv"
    elif max_lineage_size is None:
        filename = "{}.csv".format(LINEAGE_CACHE_POLICIES[lineage_cache_policy])
    else:
        filename = "{}-{}.csv".format(LINEAGE_CACHE_POLICIES[lineage_cache_policy], max_lineage_size)
    return filename

def parse_experiment(num_raylets, num_redis_shards, use_lineage_stash, window,
        mapper, json, trial):
    filename = get_filename(num_raylets, num_redis_shards, use_lineage_stash, window,
        mapper, json, trial)

    try:
        with open("{}-latency.out".format(filename), 'r') as f:
            pass
        return True
    except:
        return False

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

def run_experiment(num_raylets, num_redis_shards, use_lineage_stash, window,
        mapper, json, trial):
    filename = get_filename(num_raylets, num_redis_shards,
                                    use_lineage_stash, window, mapper, json, trial)

    print("Running experiment, logging to {}".format(filename))

    command = [
            "bash",
            "./run_job_failure.sh",
            str(num_raylets),
            str(num_redis_shards),
            "4",  # number of reducers
            str(EXPERIMENT_TIME),
            "-1" if use_lineage_stash else "0",
            "1" if mapper else "0",
            str(window),
            "1" if json else "0",
            filename,
            ]
    print(command)
    with open("job.out", 'a+') as f:
        pid = subprocess.Popen(command, stdout=f, stderr=f)
        start = time.time()

        # Allow 90s for startup time.
        max_experiment_time = EXPERIMENT_TIME + 240

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
            success = False

            try:
                write_stats(filename, "localhost", 6380)
            except:
                pass

def run_all_experiments():
    for trial in range(NUM_TRIALS):
        for num_raylets in RAYLETS:
            for num_redis_shards in SHARDS:
                for window in WINDOWS:
                    for json in USE_JSON:
                        for kill_mapper in KILL_MAPPER:
                            for use_lineage_stash in USE_LINEAGE_STASH:
                                trial_ran = parse_experiment(num_raylets, num_redis_shards,
                                        use_lineage_stash, window, kill_mapper, json, trial)
                                if trial_ran:
                                    print("Experiment found, logged at {}".format(get_filename(num_raylets,
                                                num_redis_shards,
                                                use_lineage_stash, window, kill_mapper, json,
                                                trial)))
                                    continue

                                # Run one trial. Returns true if the experiment did not
                                # time out.
                                run_experiment(num_raylets, num_redis_shards,
                                        use_lineage_stash, window, kill_mapper, json, trial)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--parse', action='store_true')
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()

    if args.run:
        run_all_experiments()

    if args.parse:
        parse_all_experiments()
