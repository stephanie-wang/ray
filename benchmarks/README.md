# Lineage stash benchmarks

## Common setup

Time estimates are listed in parentheses.

1. (5min) For the benchmarks below, I've built an AMI that you can use which has Ray installed, as well as the other baselines and their dependencies (e.g., Flink, Hadoop, OpenMPI).
    Please contact me (swang@cs.berkeley.edu) so that I can share the AMI with you.

    If you would like to build your own image, I recommend that you start with Ubuntu 16.04 and make sure that you can run standalone clusters for Flink, Hadoop, and OpenMPI.
    You will also need to clone this repository, as well as the `lineage-stash-artifact` [repo](https://github.com/stephanie-wang/lineage-stash-artifact/), which includes the Flink and OpenMPI baselines.

2. (10min) Installing Ray.
    All of the following benchmarks are run with the Ray autoscaler, a utility for launching clusters and deploying Ray jobs from your local machine.
    Before running any of the following commands, please install Ray locally by following the instructions [here](https://github.com/stephanie-wang/ray/blob/lineage-stash/doc/source/installation.rst#building-ray-from-source).
    Make sure to follow the instructions for "Building Ray from source".

3. (up to 1 day, if AWS limit requests required) We'll be using AWS EC2 for all experiments.
    You can check out the autoscaler configurations for the clusters we'll be deploying in `ray/benchmarks/cluster-scripts/*.yaml`.
    The default AWS region in the included autoscaler configs is `us-west-2`, but you can always replace this with your preferred region (just search and replace `us-west-2` in the .yaml files).
    Here are the instance types that we used for the lineage stash paper.
    The maximum quantity is the quantity used in the SOSP'19 paper, but note that you can always decrease the quantity that you use and run a smaller version of the experiment.
    Also, we have listed which instances we recommend you run as spot requests, but note that you can always run with on-demand instead.
    Please check your AWS EC2 dashboard to make sure that your minimum instance limits match these in your specified region.

    | Instance type | On-demand or spot | Quantity |
    | ------------- |:-------------:|:-----:|
    |  m5.8xlarge | on-demand | 1 |
    |  m5.2xlarge | spot | 4-64 |
    |  m4.xlarge | spot | 4-32 |

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

## Streaming benchmark

In this benchmark, we will run a streaming wordcount job on Ray and on Flink.
We'll collect the latency distribution when there are no failures, as well as the latency and throughput when a failure is introduced partway between checkpoints.

The instructions given are for 4 worker nodes, while the experiment in the lineage stash paper is for 32 nodes.
If you would like to replicate the experiment exactly, then you must modify the lines in `ray/benchmarks/cluster-scripts/streaming.yaml` to the following:
```
min_workers: 32
...
max_workers: 32
```

1. (10min) Make sure you are in the `cluster-scripts` directory for all following commands, and start the cluster with:
    ```bash
    cd ray/benchmarks/cluster-scripts
    bash setup_cluster.sh streaming.yaml 4
    ```
    If you are running the experiment with 32 nodes, you should run this instead:
    ```bash
    cd ray/benchmarks/cluster-scripts
    bash setup_cluster.sh streaming.yaml 32
    ```

2. (20min) Attach to the cluster, and run the benchmark.
    `ray attach` will connect you to a `screen` session on the head node of the cluster.
    ```bash
    ray attach streaming.yaml
    bash ~/ray/benchmarks/cluster-scripts/run_streaming_benchmark.sh 4  # This should run on the head node.
    ```
    If you are running the experiment with 32 nodes, you should run this instead:
    ```bash
    ray attach streaming.yaml
    bash ~/ray/benchmarks/cluster-scripts/run_streaming_benchmark.sh 32  # This should run on the head node.
    ```

3. (5min) Once the command is complete, make sure you have the correct output.
    You can also look for these files while the benchmark runs, to make sure that it's running properly.
    In `~/flink-wordcount/`, there should be 4 `.csv` files, with names like this:
    * failure-flink-latency-4-workers-32000-tput-30-checkpoint-Aug-14-01-38-13.csv
    * failure-flink-throughput-4-workers-32000-tput-30-checkpoint-Aug-14-01-38-13.csv
    * flink-latency-4-workers-40000-tput-Aug-14-01-44-16.csv
    * flink-throughput-4-workers-40000-tput-Aug-14-01-44-16.csv

    In `~/ray/benchmarks/cluster-scripts/`, there should be 8 `.csv` files, with names like this:
    * failure-latency-4-workers-8-shards-1000-batch-32000-tput-30-checkpoint-Aug-14-01-40-37.csv
    * failure-throughput-4-workers-8-shards-1000-batch-32000-tput-30-checkpoint-Aug-14-01-40-37.csv
    * latency-4-workers-8-shards-1000-batch-40000-tput-Aug-14-01-45-32.csv
    * throughput-4-workers-8-shards-1000-batch-40000-tput-Aug-14-01-45-32.csv
    * writefirst-failure-latency-4-workers-8-shards-1000-batch-32000-tput-30-checkpoint-Aug-14-23-18-18.csv
    * writefirst-failure-throughput-4-workers-8-shards-1000-batch-32000-tput-30-checkpoint-Aug-14-23-18-18.csv
    * writefirst-latency-4-workers-8-shards-1000-batch-40000-tput-Aug-14-23-25-50.csv
    * writefirst-throughput-4-workers-8-shards-1000-batch-40000-tput-Aug-14-23-25-50.csv

    Copy the output to your local directory by running:
    ```bash
    scp ubuntu@`ray get_head_ip streaming.yaml`:~/flink-wordcount/*.csv .
    scp ubuntu@`ray get_head_ip streaming.yaml`:~/ray/benchmarks/cluster-scripts/*.csv .
    ```

4. (5min) Plot the results!
    To get the plotting scripts and to see some example data, clone the lineage-stash-artifact repo like this:
    ```
    git clone https://github.com/stephanie-wang/lineage-stash-artifact.git
    ```
    This repo includes some example plots in `lineage-stash-artifact/data/streaming`.

    To plot the latency results:
    ```bash
    cd lineage-stash-artifact/data/streaming
    tar -xzvf 4-workers.tar.gz
    python plot_latency_cdf.py \
        --directory 4-workers/
    ```
    This command produces a graph like this:

    ![](https://github.com/stephanie-wang/lineage-stash-artifact/blob/master/data/streaming/latency-4-workers.png "Latency")

    Here's the same graph, but on 32 workers (data in `lineage-stash-artifact/data/streaming/32-workers.tar.gz`):

    ![](https://github.com/stephanie-wang/lineage-stash-artifact/blob/master/data/streaming/latency-32-workers.png "Latency")

    To plot the results from the recovery experiment:
    ```bash
    cd lineage-stash-artifact/data/streaming
    tar -xzvf 4-workers.tar.gz
    python plot_recovery.py \
        --directory 4-workers/
    ```
    This command produces two graphs, one for latency and one for throughput, like this:

    ![](https://github.com/stephanie-wang/lineage-stash-artifact/blob/master/data/streaming/latency-recovery-4-workers.png "Latency during recovery")
    ![](https://github.com/stephanie-wang/lineage-stash-artifact/blob/master/data/streaming/throughput-recovery-4-workers.png "Throughput during recovery")

    Here's the same graph, but on 32 workers (data in `lineage-stash-artifact/data/streaming/32-workers.tar.gz`):

    ![](https://github.com/stephanie-wang/lineage-stash-artifact/blob/master/data/streaming/latency-recovery-32-workers.png "Latency during recovery")
    ![](https://github.com/stephanie-wang/lineage-stash-artifact/blob/master/data/streaming/throughput-recovery-32-workers.png "Throughput during recovery")

5. Finally, tear down the cluster with `ray down`:
    ```bash
    cd ray/benchmarks/cluster-scripts
    ray down ray/benchmarks/cluster-scripts/streaming.yaml
    ```

## Allreduce benchmark

In this benchmark, we will run the allreduce benchmark on Ray and on Flink.
We'll collect the latency for each iteration of allreduce, with and without failures.

The instructions given are for 4 worker nodes, while the experiment in the lineage stash paper is for 64 nodes.
If you would like to replicate the experiment exactly, then you must modify the lines in `ray/benchmarks/cluster-scripts/allreduce.yaml` to the following:
```
min_workers: 64
...
max_workers: 64
```

1. (10min) Make sure you are in the `cluster-scripts` directory for all following commands, and start the cluster with:
    ```bash
    cd ray/benchmarks/cluster-scripts
    bash setup_cluster.sh allreduce.yaml 64
    ```

2. (25min on 4 nodes, 40min on 64 nodes) Attach to the cluster, and run the benchmark.
    `ray attach` will connect you to a `screen` session on the head node of the cluster.
    ```bash
    ray attach allreduce.yaml
    # These commands should run on the head node, not on your local machine.
    cd ~/ray/benchmarks/cluster-scripts
    bash run_allreduce_jobs.sh 4
    ```

    If you are running the experiment with 64 nodes, you should run this instead:
    ```bash
    ray attach allreduce.yaml
    # These commands should run on the head node, not on your local machine.
    cd ~/ray/benchmarks/cluster-scripts
    bash run_allreduce_jobs.sh 64
    ```

    The `run_allreduce_jobs.sh` command will create a new directory in the current directory with a name like `allreduce-19-08-14-22-30-38`.
    You can `tail` files in this directory to make sure that the benchmark is running properly.
    Also, if you need to restart the benchmark for any reason, you can run the same command, but with the previous directory as an argument.
    This will restart the benchmark and execute any lineage stash jobs that do not already have output.
    For example:
    ```bash
    bash run_allreduce_jobs.sh 4 allreduce-19-08-14-22-30-38
    ```

3. (5min) Once the command is complete, make sure you have the correct output.
    You can also look for these files while the benchmark runs, to make sure that it's running properly.
    In `~/ray/benchmarks/cluster-scripts/allreduce-<date>`, there should be 18 `.txt` files, with names like this:
    * failure-latency-4-workers-1-shards-0-gcs-0-gcsdelay-25000000-bytes-19-08-14-22-33-14.txt
    * failure-latency-4-workers-1-shards-1-gcs-0-gcsdelay-25000000-bytes-19-08-14-22-35-22.txt
    * failure-mpi-latency-4-workers-25000000-bytes-19-08-14-23-00-53.txt
    * latency-4-workers-1-shards-0-gcs-0-gcsdelay-2500000-bytes-19-08-14-22-39-27.txt
    * latency-4-workers-1-shards-0-gcs-0-gcsdelay-25000000-bytes-19-08-14-22-41-08.txt
    * latency-4-workers-1-shards-0-gcs-0-gcsdelay-250000000-bytes-19-08-14-22-51-15.txt
    * latency-4-workers-1-shards-0-gcs-5-gcsdelay-2500000-bytes-19-08-14-22-40-16.txt
    * latency-4-workers-1-shards-0-gcs-5-gcsdelay-25000000-bytes-19-08-14-22-42-50.txt
    * latency-4-workers-1-shards-0-gcs-5-gcsdelay-250000000-bytes-19-08-14-22-53-52.txt
    * latency-4-workers-1-shards-1-gcs-0-gcsdelay-2500000-bytes-19-08-14-22-39-51.txt
    * latency-4-workers-1-shards-1-gcs-0-gcsdelay-25000000-bytes-19-08-14-22-42-00.txt
    * latency-4-workers-1-shards-1-gcs-0-gcsdelay-250000000-bytes-19-08-14-22-52-33.txt
    * latency-4-workers-1-shards-1-gcs-5-gcsdelay-2500000-bytes-19-08-14-22-40-41.txt
    * latency-4-workers-1-shards-1-gcs-5-gcsdelay-25000000-bytes-19-08-14-22-43-38.txt
    * latency-4-workers-1-shards-1-gcs-5-gcsdelay-250000000-bytes-19-08-14-22-55-12.txt
    * mpi-latency-4-workers-2500000-bytes-19-08-14-23-03-24.txt
    * mpi-latency-4-workers-25000000-bytes-19-08-14-23-03-29.txt
    * mpi-latency-4-workers-250000000-bytes-19-08-14-23-04-05.txt

    Copy the output to your local directory by running:
    ```bash
    scp -r ubuntu@`ray get_head_ip allreduce.yaml`:~/ray/benchmarks/cluster-scripts/allreduce-* .
    ```

4. (5min) Plot the results!
    To get the plotting scripts and to see some example data, clone the lineage-stash-artifact repo like this:
    ```
    git clone https://github.com/stephanie-wang/lineage-stash-artifact.git
    ```
    This repo includes some example plots in `lineage-stash-artifact/data/allreduce`.

    To plot the latency results:
    ```bash
    cd lineage-stash-artifact/data/allreduce
    tar -xzvf 4-workers.tar.gz
    python plot_allreduce_latency.py \
        --directory 4-workers/
    ```
    This command produces a graph like this:

    ![](https://github.com/stephanie-wang/lineage-stash-artifact/blob/master/data/allreduce/latency-4-workers.png "Latency")

    Here's the same graph, but on 64 workers (data in `lineage-stash-artifact/data/allreduce/64-workers.tar.gz`):

    ![](https://github.com/stephanie-wang/lineage-stash-artifact/blob/master/data/allreduce/latency-64-workers.png "Latency")

    To plot the results from the recovery experiment:
    ```bash
    cd lineage-stash-artifact/data/allreduce
    tar -xzvf 4-workers.tar.gz
    python plot_allreduce_recovery.py \
        --directory 4-workers/
    ```
    This command produces a graph like this:

    ![](https://github.com/stephanie-wang/lineage-stash-artifact/blob/master/data/allreduce/recovery-4-workers.png "Latency during recovery")

    Here's the same graph, but on 64 workers (data in `lineage-stash-artifact/data/allreduce/64-workers.tar.gz`):

    ![](https://github.com/stephanie-wang/lineage-stash-artifact/blob/master/data/allreduce/recovery-64-workers.png "Latency during recovery")

5. Finally, tear down the cluster with `ray down`:
    ```bash
    cd ray/benchmarks/cluster-scripts
    ray down ray/benchmarks/cluster-scripts/allreduce.yaml
    ```
