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
    From now on, we'll reference the root directory of your `ray` clone with `$RAY_HOME`.
    Make sure to follow the instructions for "Building Ray from source".

    We recommend that you use Python 3.7 from [Anaconda](https://www.anaconda.com/distribution/) and install Ray in a clean [Anaconda environment](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-with-commands).
    Also, once inside your new environment, please install boto3, which is required for the cluster setup here:
    ```bash
    pip install boto3
    ```

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
    RAY_HOME=`~/ray`  # or wherever you have cloned the ray repo.
    cd $RAY_HOME/benchmarks
    ray up -y cluster-scripts/test.yaml
    ```
    This will create a cluster with 2 nodes, one of which will be designated the "head node".
    You should see some output as the autoscaler sets up your cluster, and eventually there should be a message explaining how to SSH into your cluster and run commands.
    You can also check out your EC2 console to make sure that you see the running instances.
    They should be labeled with something like `ray-test-worker` or `ray-test-head`.

    For your convenience, the clusters for the benchmarks below will be setup with the script `$RAY_HOME/benchmarks/cluster-scripts/setup_cluster.sh`, which calls `ray up` internally, gathers the workers' IP addresses, and makes sure all workers have the same software.

5. (5min) Now that you've created your first cluster, the Ray autoscaler should have automatically created a new `.pem` file for you in your `~/.ssh` directory.
    It should look something like `ray-autoscaler_1_<region>.pem`.

    Please add this identity to your SSH agent (via `ssh-add ray-autoscaler_<...>`) and make sure you have SSH agent forwarding setup on your local machine to make the cluster setup smoother.
    [Here](https://developer.github.com/v3/guides/using-ssh-agent-forwarding/) are some good instructions for setting up your SSH agent if you haven't done so before.
    For the hostname, you can this to match all AWS hostnames: `*.<region>.compute.amazonaws.com`.

6. (5min) You can now tear down your test cluster and get started on the benchmarks!
    ```bash
    ray down cluster-scripts/test.yaml
    ```

## Streaming benchmark

In this benchmark, we will run a streaming wordcount job on Ray and on Flink.
We'll collect the latency distribution when there are no failures, as well as the latency and throughput when a failure is introduced partway between checkpoints.

The instructions given are for 4 worker nodes, while the experiment in the lineage stash paper is for 32 nodes.
If you would like to replicate the experiment exactly, then you must modify the lines in `$RAY_HOME/benchmarks/cluster-scripts/streaming.yaml` to the following:
```
min_workers: 32
...
max_workers: 32
```

1. (10min) Make sure you are in the `cluster-scripts` directory for all following commands, and start the cluster with:
    ```bash
    cd $RAY_HOME/benchmarks/cluster-scripts
    bash setup_cluster.sh streaming.yaml 4
    ```
    If you are running the experiment with 32 nodes, you should run this instead:
    ```bash
    cd $RAY_HOME/benchmarks/cluster-scripts
    bash setup_cluster.sh streaming.yaml 32
    ```
    **NOTE:** Running with spot instances is usually much cheaper than with
    on-demand, but in some cases there will not be enough instances available
    for you to use. If the `setup_cluster.sh` command prints `Waiting for x
    more workers to start...` and `x` has not changed for at least a minute,
    then it is likely that your cluster will not finish setup. This is probably
    because: a) your spot instance limit is too low, or b) there are not enough
    spot instances available. In case (a), contact AWS to request an increase
    in your spot instance limit. In case (b), you can either try on-demand
    instances or try again with spot instances later.  To try with on-demand
    instances, comment out [this
    block](https://github.com/stephanie-wang/ray/blob/lineage-stash/benchmarks/cluster-scripts/streaming.yaml#L67)
    in the configuration file. Note that again, you may have to first contact
    AWS to request an increase, this time in your *on-demand* instance limit.

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

    Make a new local directory for the results, and copy the output there by running:
    ```bash
    mkdir streaming-4-workers
    scp ubuntu@`ray get_head_ip streaming.yaml`:~/flink-wordcount/*.csv streaming-4-workers
    scp ubuntu@`ray get_head_ip streaming.yaml`:~/ray/benchmarks/cluster-scripts/*.csv streaming-4-workers
    ```

4. (5min) Plot the latency results!
    To get the plotting scripts and to see some example data, clone the lineage-stash-artifact repo like this:
    ```
    git clone https://github.com/stephanie-wang/lineage-stash-artifact.git
    ```
    This repo includes some example plots in `lineage-stash-artifact/data/streaming`.

    To plot the example latency results for 4 workers:
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

    To produce these plots from your own run, pass in the new directory that you created with all of the CSV files.
    You should see a window pop up with the plots.
    Alternatively, you can pass in a pathname to save the results as, with the `--save-filename` option.
    For example:
    ```bash
    python plot_latency_cdf.py \
        --directory $RAY_HOME/benchmarks/cluster-scripts/streaming-4-workers
        --save-filename latency-4-workers.png
    ```

5. (5min) Plot the recovery results!
    To plot the example results from the recovery experiment:
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

    To produce these plots from your own run, pass in the new directory that you created with all of the CSV files.
    You should see a window pop up with the plots.
    Alternatively, you can pass in a pathname to save the results as, with the `--save-filename` option.
    Note that this command will produce two plots for the recovery experiment, one for latency and one for throughput.
    For example:
    ```bash
    python plot_recovery.py \
        --directory $RAY_HOME/benchmarks/cluster-scripts/streaming-4-workers
        --save-filename recovery-4-workers.png  # Produces latency-recovery-4-workers.png and throughput-recovery-4-workers.png.
    ```

6. Finally, tear down the cluster with `ray down`:
    ```bash
    cd $RAY_HOME/benchmarks/cluster-scripts
    ray down streaming.yaml
    ```

## Allreduce benchmark

In this benchmark, we will run the allreduce benchmark on Ray and on Flink.
We'll collect the latency for each iteration of allreduce, with and without failures.

The instructions given are for 4 worker nodes, while the experiment in the lineage stash paper is for 64 nodes.
If you would like to replicate the experiment exactly, then you must modify the lines in `$RAY_HOME/benchmarks/cluster-scripts/allreduce.yaml` to the following:
```
min_workers: 64
...
max_workers: 64
```

1. (10min) Make sure you are in the `cluster-scripts` directory for all following commands, and start the cluster with:
    ```bash
    cd $RAY_HOME/benchmarks/cluster-scripts
    bash setup_cluster.sh allreduce.yaml 4
    ```
    If you are running the experiment with 64 nodes, you should run this instead:
    ```bash
    cd $RAY_HOME/benchmarks/cluster-scripts
    bash setup_cluster.sh allreduce.yaml 64
    ```
    **NOTE:** Running with spot instances is usually much cheaper than with
    on-demand, but in some cases there will not be enough instances available
    for you to use. If the `setup_cluster.sh` command prints `Waiting for x
    more workers to start...` and `x` has not changed for at least a minute,
    then it is likely that your cluster will not finish setup. This is probably
    because: a) your spot instance limit is too low, or b) there are not enough
    spot instances available. In case (a), contact AWS to request an increase
    in your spot instance limit. In case (b), you can either try on-demand
    instances or try again with spot instances later.  To try with on-demand
    instances, comment out [this
    block](https://github.com/stephanie-wang/ray/blob/lineage-stash/benchmarks/cluster-scripts/allreduce.yaml#L67)
    in the configuration file. Note that again, you may have to first contact
    AWS to request an increase, this time in your *on-demand* instance limit.

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

    Make a new local directory for the results, and copy the output there by running:
    ```bash
    mkdir allreduce-4-workers
    scp -r ubuntu@`ray get_head_ip allreduce.yaml`:~/ray/benchmarks/cluster-scripts/allreduce-* allreduce-4-workers
    ```

4. (5min) Plot the latency results!
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

    To produce these plots from your own run, pass in the new directory that you created with all of the CSV files.
    You should see a window pop up with the plots.
    Alternatively, you can pass in a pathname to save the results as, with the `--save-filename` option.
    For example:
    ```bash
    python plot_allreduce_latency.py \
        --directory $RAY_HOME/benchmarks/cluster-scripts/allreduce-4-workers
        --save-filename latency-4-workers.png
    ```

5. (5min) Plot the recovery results!

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

    To produce these plots from your own run, pass in the new directory that you created with all of the CSV files.
    You should see a window pop up with the plots.
    Alternatively, you can pass in a pathname to save the results as, with the `--save-filename` option.
    For example:
    ```bash
    python plot_allreduce_recovery.py \
        --directory $RAY_HOME/benchmarks/cluster-scripts/allreduce-4-workers
        --save-filename recovery-4-workers.png
    ```

6. Finally, tear down the cluster with `ray down`:
    ```bash
    cd $RAY_HOME/benchmarks/cluster-scripts
    ray down allreduce.yaml
    ```

## Microbenchmarks

In this benchmark, we will run the microbenchmarks to collect the latency distribution for the lineage stash vs a WriteFirst method and to measure the amount of uncommitted lineage that gets forwarded.

1. (10min) Make sure you are in the `cluster-scripts` directory for all following commands, and start the cluster with:
    ```bash
    cd $RAY_HOME/benchmarks/cluster-scripts
    bash setup_cluster.sh microbenchmark.yaml 64
    ```
    **NOTE:** Running with spot instances is usually much cheaper than with
    on-demand, but in some cases there will not be enough instances available
    for you to use. If the `setup_cluster.sh` command prints `Waiting for x
    more workers to start...` and `x` has not changed for at least a minute,
    then it is likely that your cluster will not finish setup. This is probably
    because: a) your spot instance limit is too low, or b) there are not enough
    spot instances available. In case (a), contact AWS to request an increase
    in your spot instance limit. In case (b), you can either try on-demand
    instances or try again with spot instances later.  To try with on-demand
    instances, comment out [this
    block](https://github.com/stephanie-wang/ray/blob/lineage-stash/benchmarks/cluster-scripts/streaming.yaml#L67)
    in the configuration file. Note that again, you may have to first contact
    AWS to request an increase, this time in your *on-demand* instance limit.

2. (30min) Run the latency distribution benchmark.
    First, attach to the cluster.
    `ray attach` will connect you to a `screen` session on the head node of the cluster.
    ```bash
    ray attach microbenchmark.yaml
    # These commands should run on the head node, not on your local machine.
    cd ~/ray/benchmarks/cluster-scripts
    bash run_latency_microbenchmark.sh 64
    ```

    The `run_latency_microbenchmark.sh` command will create a new directory in the current directory with a name like `latency-19-08-26-03-20-32`.
    You can `tail` files in this directory to make sure that the benchmark is running properly.
    Also, if you need to restart the benchmark for any reason, you can run the same command, but with the previous directory as an argument.
    This will restart the benchmark and execute any lineage stash jobs that do not already have output.
    For example:
    ```bash
    bash run_latency_microbenchmark.sh 64 latency-19-08-26-03-20-32
    ```

3. (5min) Once the command is complete, make sure you have the correct output.
    You can also look for these files while the benchmark runs, to make sure that it's running properly.
    In `~/ray/benchmarks/cluster-scripts/`, there should be a directory called
    `latency-<timestamp>` with 15 `.csv` files, with names like this:
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-0-gcs-0-gcsdelay-0-nondeterminism-0-task-1-failures-19-08-26-03-23-13.csv
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-0-gcs-0-gcsdelay-1-nondeterminism-0-task--1-failures-19-08-26-03-20-32.csv
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-0-gcs-0-gcsdelay-1-nondeterminism-0-task-8-failures-19-08-26-04-05-26.csv
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-0-gcs-1-gcsdelay-0-nondeterminism-0-task-1-failures-19-08-26-03-39-41.csv
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-0-gcs-1-gcsdelay-1-nondeterminism-0-task--1-failures-19-08-26-03-37-11.csv
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-0-gcs-1-gcsdelay-1-nondeterminism-0-task-8-failures-19-08-26-04-06-34.csv
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-0-gcs-5-gcsdelay-0-nondeterminism-0-task-1-failures-19-08-26-03-45-38.csv
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-0-gcs-5-gcsdelay-1-nondeterminism-0-task--1-failures-19-08-26-03-43-08.csv
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-0-gcs-5-gcsdelay-1-nondeterminism-0-task-8-failures-19-08-26-04-07-42.csv
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-1-gcs-0-gcsdelay-0-nondeterminism-0-task-1-failures-19-08-26-03-36-07.csv
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-1-gcs-0-gcsdelay-1-nondeterminism-0-task-1-failures-19-08-26-03-34-58.csv
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-1-gcs-1-gcsdelay-0-nondeterminism-0-task-1-failures-19-08-26-03-41-59.csv
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-1-gcs-1-gcsdelay-1-nondeterminism-0-task-1-failures-19-08-26-03-40-42.csv
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-1-gcs-5-gcsdelay-0-nondeterminism-0-task-1-failures-19-08-26-03-48-47.csv
    * latency-19-08-26-03-20-32/latency-64-workers-1-shards-1-gcs-5-gcsdelay-1-nondeterminism-0-task-1-failures-19-08-26-03-46-40.csv

    Tar the directory on the head node like this:
    ```bash
    tar -czvf latency-<timestamp>.tar.gz latency-<timestamp>
    ```

    Copy the output to your local machine by running:
    ```bash
    scp ubuntu@`ray get_head_ip microbenchmark.yaml`:~/ray/benchmarks/cluster-scripts/latency-*.tar.gz .
    tar -xzvf latency-<timestamp>.tar.gz
    ```

4. (5min) Plot the latency results!
    To get the plotting scripts and to see some example data, clone the lineage-stash-artifact repo like this:
    ```
    git clone https://github.com/stephanie-wang/lineage-stash-artifact.git
    ```
    This repo includes some example plots in `lineage-stash-artifact/data/microbenchmark`.

    To plot the example latency results:
    ```bash
    cd lineage-stash-artifact/data/microbenchmark
    tar -xzvf latency-19-08-26-03-20-32.tar.gz
    python plot_latency_cdf.py \
        --directory latency-19-08-26-03-20-32.tar.gz/
    ```
    This command produces three graphs like this:

    ![](https://github.com/stephanie-wang/lineage-stash-artifact/blob/master/data/microbenchmark/deterministic-latency-64-workers.png "Latency")
    ![](https://github.com/stephanie-wang/lineage-stash-artifact/blob/master/data/microbenchmark/nondeterministic-8-failures-latency-64-workers.png "Latency")
    ![](https://github.com/stephanie-wang/lineage-stash-artifact/blob/master/data/microbenchmark/nondeterministic--1-failures-latency-64-workers.png "Latency")
                        
    To produce these plots from your own run, pass in the new directory that you created with all of the CSV files.
    You should see a window pop up, one for each plot.
    Alternatively, you can pass in a pathname to save the results as, with the `--save-filename` option.
    Note that this command will produce three plots for the experiment, one each for the deterministic application, the nondeterministic application with *f*=64, and the nondeterministic application with *f*=8.
    All of them will have the pathname that you passed in as the suffix.
    This will also create a CSV file with some stats from the plotted results, such as the p50 latency, which will have the same pathname that you saved the plot to, but ending with `.csv`.
    For example:
    ```bash
    python plot_latency_cdf.py \
        --directory $RAY_HOME/benchmarks/cluster-scripts/latency-<timestamp>
        --save-filename latency-64-workers.png
    ls *-latency-64-workers.png
    # deterministic-latency-64-workers.png
    # nondeterministic-8-failures-latency-64-workers.png
    # nondeterministic--1-failures-latency-64-workers.png
    cat latency-64-workers.csv
    # workers,shards,gcs,gcsdelay,nondeterminism,task,failures,p50,p90,p95,p99
    # 64,1,0,0,0,0,1,0.4808845,0.5088611,0.5292043500000001,0.5838749100000002
    # 64,1,0,1,0,0,1,0.488071,0.5113015,0.51899715,0.56703822
    # 64,1,0,5,0,0,1,0.483224,0.5190009000000001,0.5424929,0.5819955200000001
    # ...
    ```

6. If you are also running the uncommitted lineage benchmark below, you can leave the cluster running (they use the exact same configuration).
    Otherwise, tear down the cluster with `ray down`:
    ```bash
    cd $RAY_HOME/benchmarks/cluster-scripts
    ray down streaming.yaml
    ```

7. **(5 hours)** Run the uncommitted lineage benchmark.
    First, attach to the cluster.
    `ray attach` will connect you to a `screen` session on the head node of the cluster.
    ```bash
    ray attach microbenchmark.yaml
    # These commands should run on the head node, not on your local machine.
    cd ~/ray/benchmarks/cluster-scripts
    bash run_uncommitted_lineage_microbenchmark.sh 64
    ```

    The `run_uncommitted_lineage_microbenchmark.sh` command will create a new directory in the current directory with a name like `lineage-19-08-25-00-21-56`.
    You can `tail` files in this directory to make sure that the benchmark is running properly.
    Also, if you need to restart the benchmark for any reason, you can run the same command, but with the previous directory as an argument.
    This will restart the benchmark and execute any lineage stash jobs that do not already have output.
    For example:
    ```bash
    bash run_uncommitted_lineage_microbenchmark.sh 64 lineage-19-08-25-00-21-56
    ```

    Note that the full version of this benchmark runs for a very long time.
    It sweeps parameters along 2 axes: task duration and *F*, the number of times a task gets forwarded.
    There are a couple things you can do to shorten the benchmark or collect incremental results.

    * You can reduce the number of values of *F* to collect.
        To do that, modify the line in the benchmark script (`run_uncommitted_lineage_microbenchmark.sh`) found [here](https://github.com/stephanie-wang/ray/blob/lineage-stash/benchmarks/cluster-scripts/run_uncommitted_lineage_microbenchmark.sh#L26):
        ```
            for MAX_FAILURES in 8 16 32 -1; do
        ```
        `MAX_FAILURES` is equivalent to *F*, the maximum number of times that a task gets forwarded.
        The value `-1` is a special value that means forward infinitely many times, or equivalently, forward up to the number of nodes.
        You can take out some values of `MAX_FAILURES` and use something like this, which should halve the total runtime:
        ```
            for MAX_FAILURES in 8 -1; do
        ```

    * The benchmark sweeps task durations from 0ms to 100ms, starting with the endpoints and recursively sampling the midpoint.
        This means that you can collect incremental results while the benchmark is still running.
        The resolution of your plot will not be as high as the exampel plot included here, but you can get a rough estimate of the trend.

    * You can manually remove sample points from the task duration sweep by modifying the line in the benchmark script (`run_uncommitted_lineage_microbenchmark.sh`) found [here](https://github.com/stephanie-wang/ray/blob/lineage-stash/benchmarks/cluster-scripts/run_uncommitted_lineage_microbenchmark.sh#L26):
        ```
        for TASK_DURATION in 000 100 012 005 018 002 008 014 030 001 003 006 009 013 015 019 040 004 007 010 016 020 050; do
        ```
        `TASK_DURATION` is how long, in milliseconds, each (no-op) task in the job executes for.
        The longer the task duration, the longer the job.
        Therefore, you can take out some of the values, especially the higher ones, and use something like this.
        Note that the script expects that all values here are zero-padded to 3 digits:
        ```
        for TASK_DURATION in 000 010 020 030 040; do
        ```

7. (5min) Once the command is complete, or (recommended) while the job is still running, make sure you have the correct output.
    You can also look for these files while the benchmark runs, to make sure that it's running properly.
    In `~/ray/benchmarks/cluster-scripts/`, there should be a directory called
    `lineage-<timestamp>` with many `.csv` files, one for each pair of `TASK_DURATION` and `MAX_FAILURES` values that you ran, with names like this:
    * lineage-19-08-25-23-25-00/lineage-64-workers-1-shards-0-gcs-100-gcsdelay-1-nondeterminism-000-task--1-failures-19-08-25-23-28-34.csv
    * lineage-19-08-25-23-25-00/lineage-64-workers-1-shards-0-gcs-100-gcsdelay-1-nondeterminism-000-task-16-failures-19-08-25-23-26-05.csv
    * lineage-19-08-25-23-25-00/lineage-64-workers-1-shards-0-gcs-100-gcsdelay-1-nondeterminism-000-task-32-failures-19-08-25-23-27-20.csv
    * lineage-19-08-25-23-25-00/lineage-64-workers-1-shards-0-gcs-100-gcsdelay-1-nondeterminism-000-task-8-failures-19-08-25-23-25-00.csv
    * lineage-19-08-25-23-25-00/lineage-64-workers-1-shards-0-gcs-100-gcsdelay-1-nondeterminism-001-task--1-failures-19-08-25-23-34-05.csv
    * lineage-19-08-25-23-25-00/lineage-64-workers-1-shards-0-gcs-100-gcsdelay-1-nondeterminism-001-task-16-failures-19-08-25-23-31-22.csv
    * lineage-19-08-25-23-25-00/lineage-64-workers-1-shards-0-gcs-100-gcsdelay-1-nondeterminism-001-task-32-failures-19-08-25-23-32-39.csv
    * lineage-19-08-25-23-25-00/lineage-64-workers-1-shards-0-gcs-100-gcsdelay-1-nondeterminism-001-task-8-failures-19-08-25-23-30-07.csv
    * ...

    Tar the directory on the head node like this:
    ```bash
    tar -czvf lineage-<timestamp>.tar.gz lineage-<timestamp>
    ```

    Copy the output to your local machine by running:
    ```bash
    scp ubuntu@`ray get_head_ip microbenchmark.yaml`:~/ray/benchmarks/cluster-scripts/lineage-*.tar.gz .
    tar -xzvf latency-<timestamp>.tar.gz
    ```

8. (5min) Plot the latency results!
    To get the plotting scripts and to see some example data, clone the lineage-stash-artifact repo like this:
    ```
    git clone https://github.com/stephanie-wang/lineage-stash-artifact.git
    ```
    This repo includes some example plots in `lineage-stash-artifact/data/microbenchmark`.

    To plot the example uncommitted lineage results:
    ```bash
    cd lineage-stash-artifact/data/microbenchmark
    tar -xzvf lineage-19-08-25-23-25-00.tar.gz
    python plot_uncommitted_lineage.py \
        --directory lineage-19-08-25-23-25-00/
    ```
    This command produces a graph like this:

    ![](https://github.com/stephanie-wang/lineage-stash-artifact/blob/master/data/microbenchmark/64-workers-uncommitted-lineage.png "Uncommitted lineage")
                        
    To produce these plots from your own run, pass in the new directory that you created with all of the CSV files.
    You should see a window pop up with the plot.
    Alternatively, you can pass in a pathname to save the results as, with the `--save-filename` option.
    This will also create a CSV file with some stats from the plotted results, such as the p50 latency, which will have the same pathname that you saved the plot to, but ending with `.csv`.
    For example:
    ```bash
    python plot_uncommitted_lineage.py \
        --directory $RAY_HOME/benchmarks/cluster-scripts/lineage-<timestamp>
        --save-filename uncommitted-lineage-64-workers.png
    cat uncommitted-lineage-64-workers.csv
    # f,task_duration_ms,uncommitted_lineage
    # 8,0,7.996863476030143
    # 8,1,7.996843434343434
    # 8,2,7.996843434343434
    ```
9. Finally, tear down the cluster with `ray down`:
    ```bash
    cd $RAY_HOME/benchmarks/cluster-scripts
    ray down microbenchmark.yaml
    ```
