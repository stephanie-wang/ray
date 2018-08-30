import argparse
import csv
import subprocess
import time
import os

EXPERIMENT_TIME = 10
SLEEP_TIME = 10
NUM_TRIALS = 90

LINEAGE_CACHE_POLICIES = [
        "lineage-cache",
        "lineage-cache-flush",
        "lineage-cache-k-flush",
        ]

TARGET_THROUGHPUTS = [3000]
LEASE_FACTORS = [0.1, 0.25, 0.5, 1, 2, 4, 8, 16]
RAYLETS = [1]
SHARDS = [1]
K = [100]


def get_filename(lease_factor, num_raylets, lineage_cache_policy, max_lineage_size,
        gcs_delay, num_redis_shards, target_throughput, trial, test_failure):
    if max_lineage_size is None:
        filename = "{}-lease-{}-raylets-{}-policy-{}-gcs-{}-shards-{}-throughput{}.out".format(
                lease_factor, num_raylets, lineage_cache_policy, gcs_delay,
                num_redis_shards, target_throughput, trial)
    else:
        filename = "{}-lease-{}-raylets-{}-policy-{}-k-{}-gcs-{}-shards-{}-throughput{}.out".format(
                lease_factor, num_raylets, lineage_cache_policy, max_lineage_size,
                gcs_delay, num_redis_shards, target_throughput, trial)
    if test_failure:
        filename = "failure-" + filename
    return filename

def get_csv_filename():
    return "lease-reconstructions.csv"

def parse_experiment_throughput(lease_factor, num_raylets,
        lineage_cache_policy, max_lineage_size, gcs_delay, num_redis_shards,
        target_throughput, trial):
    filename = get_filename(lease_factor, num_raylets, lineage_cache_policy,
            max_lineage_size, gcs_delay, num_redis_shards, target_throughput, trial)

    num_submissions = 0
    num_reconstructions = 0
    reconstruction_header = False
    try:
        with open(filename, 'r') as f:
            for line in f.readlines():
                if reconstruction_header:
                    num_reconstructions += int(line)
                elif "RECONSTRUCTIONS" in line:
                    reconstruction_header = True
                else:
                    num_submissions += int(line.split(' ')[1])
    except:
        pass

    if reconstruction_header:
        return num_submissions, num_reconstructions
    else:
        return -1, -1


def parse_experiments(writer, lease_factor):
    max_lineage_size = K[0]
    policies = [
            (0, -1),
            ]
    num_raylets = RAYLETS[0]
    num_redis_shards = SHARDS[0]
    policy = 0
    gcs_delay = -1
    max_lineage_size = K[0]
    target_throughput = TARGET_THROUGHPUTS[0]
    lease = int(1000 * lease_factor)

    for trial in range(NUM_TRIALS):
        (num_submitted, num_reconstructions) = parse_experiment_throughput(
                lease_factor, num_raylets, policy, max_lineage_size, gcs_delay,
                num_redis_shards, target_throughput, trial)
        writer.writerow({
            'lease': lease,
            'num_submitted': num_submitted,
            'num_reconstructions': num_reconstructions,
            })

def parse_all_experiments():
    max_throughputs = {
            }

    filename = get_csv_filename()
    f = open(filename, 'w')
    fieldnames = [
            'lease',
            'num_submitted',
            'num_reconstructions',
        ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

    for lease_factor in LEASE_FACTORS:
        parse_experiments(writer, lease_factor)

def run_experiment(lease_factor, num_raylets, lineage_cache_policy,
        max_lineage_size, gcs_delay, num_redis_shards, target_throughput,
        trial, test_failure):
    filename = get_filename(lease_factor, num_raylets, lineage_cache_policy,
                            max_lineage_size, gcs_delay, num_redis_shards,
                            target_throughput, trial, test_failure)
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
            str(EXPERIMENT_TIME),
            str(lease_factor),
            ]
    if test_failure:
        command.append("--failure")
    with open("job.out", 'a+') as f:
        pid = subprocess.Popen(command, stdout=f, stderr=f)
        start = time.time()

        # Allow 90s for startup time.
        max_experiment_time = EXPERIMENT_TIME + 90

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

    # Collect the job's outputs, regardless of whether it completed.
    command = [
            "bash",
            "./collect_output.sh",
            str(num_raylets),
            filename,
            ]
    # Try to collect any coredumps if the job timed out.
    if not success:
        command.append(filename.split('.')[0])
    with open(os.devnull, 'w') as fnull:
        pid = subprocess.Popen(command, stdout=fnull, stderr=fnull)
        pid.wait()

    return success

def run_all_experiments(test_failure):
    max_lineage_size = K[0]
    policies = [
            (0, -1),
            ]
    num_raylets = RAYLETS[0]
    num_redis_shards = SHARDS[0]
    policy = 0
    gcs_delay = -1
    max_lineage_size = K[0]
    target_throughput = TARGET_THROUGHPUTS[0]
    for lease_factor in LEASE_FACTORS:
        # Run the trials.
        for trial in range(NUM_TRIALS):
            # Run one trial. Returns true if the experiment did not
            # time out.
            run_experiment(lease_factor, num_raylets, policy,
                    max_lineage_size, gcs_delay,
                    num_redis_shards, target_throughput, trial, test_failure)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--target-throughput', type=int, default=3000)
    parser.add_argument('--parse', action='store_true')
    parser.add_argument('--run', action='store_true')
    parser.add_argument('--failure', action='store_true')
    args = parser.parse_args()

    if args.run:
        run_all_experiments(args.failure)

    if args.parse:
        parse_all_experiments()
