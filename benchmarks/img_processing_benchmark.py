import asyncio
import cv2
import numpy as np
import os
import ray
from random import randrange
import sys
from tensorflow import keras
import tensorflow as tf
from time import process_time 
import time
import signal

from keras.preprocessing.image import load_img 
from keras.preprocessing.image import img_to_array 
from keras.applications.imagenet_utils import decode_predictions 
import matplotlib.pyplot as plt 
from keras.applications.resnet50 import ResNet50 
from keras.applications import resnet50
from PIL import Image
num_requests_per_client = 100
num_preprocess_calls = 1
batch_size = 10
failure_percentage = 1

@ray.remote
def preprocess(img):
    numpy_image = img_to_array(img) 
    image_batch = np.expand_dims(numpy_image, axis = 0) 
    processed_img = resnet50.preprocess_input(image_batch.copy())
    return processed_img

@ray.remote
def preprocess2(img):
    return img

# Use an actor to keep the model in GPU memory.
class Worker:
    def __init__(self, worker_fp):
        self.worker_fp = worker_fp
        self.model = resnet50.ResNet50(input_shape=(224, 224, 3),weights = 'imagenet')
        self.latencies = []

    def start(self):
        empty_img = np.empty([1, 224, 224, 3])
        predictions = self.model.predict(empty_img)
        print("worker alive")

    def get_latencies(self):
        latencies = self.latencies[:]
        self.latencies = []
        return latencies

    def predict(self, request_starts, *batch):
        # Predict and return results
        # pstart = time.time()
        img_batch = np.array(batch).reshape(len(batch),224, 224,3)
        predictions = self.model.predict(img_batch) 
        # pstop = time.time()
        # print("worker time: ", pstop-pstart)
        # predictions = []
        # for i in range(len(batch)):
        #     predictions.append(1)
        # time.sleep(0.1)
        predictions = resnet50.decode_predictions(predictions, top=1)

        for r in request_starts:
            self.latencies.append(time.time() - r)

        return predictions

    def predict2(self, *batch):
        #print("work")
        rand = np.random.rand() 
        if rand < self.worker_fp:
        #    print("worker failed")
            sys.exit()
        time.sleep(0.1)
        return batch

    def pid(self):
        return os.getpid()


@ray.remote
class CentralizedBatcher:
    def __init__(self, batch_size, batcher_fp, worker_fp, pass_by_value, num_workers, worker_resource):
        self.batcher_fp = batcher_fp
        self.workers = []

        worker_cls = ray.remote(
                num_cpus=0,
                num_gpus=1)(Worker)

        for i in range(num_workers):
            w = worker_cls.options(resources={worker_resource: 1}).remote(worker_fp)
            if not self.workers:
                ray.get(w.start.remote())
            self.workers.append(w)
        self.batch_size = batch_size
        self.queue = []
        self.predictions = []

    def start(self):
        print("batcher alive")
        ray.get([worker.start.remote() for worker in self.workers])

    def request(self, img_refs, request_start):
        self.queue.append((img_refs[0], request_start))
        if len(self.queue) == self.batch_size:
            batch = []
            starts = []
            for i in range(self.batch_size):
                ref, start = self.queue.pop(0)
                batch.append(ref)
                starts.append(start)
            tmp_worker = self.workers.pop(0)
            self.workers.append(tmp_worker)
            self.predictions.append(self.workers[0].predict.remote(starts, *batch))
            return

        rand = np.random.rand() 
        if rand < self.batcher_fp:
            pid = os.getpid()
            os.kill(pid, signal.SIGKILL)
        return

    def complete_requests(self):
        results = ray.get(self.predictions)
        return results

    def get_latencies(self):
        latencies = []
        for worker in self.workers:
            latencies += ray.get(worker.get_latencies.remote())
        return latencies

    def pid(self):
        worker_pid = ray.get(self.workers[0].pid.remote())
        return os.getpid(), worker_pid

class Batcher:
    def __init__(self, batch_size, batcher_fp, worker_fp, pass_by_value, num_workers, worker_resource):
        worker_cls = ray.remote(
                num_cpus=0,
                num_gpus=1,
                max_restarts=-1,
                max_task_retries=-1)(Worker)

        self.batcher_fp = batcher_fp
        self.workers = []
        for i in range(num_workers):
            w = worker_cls.options(resources={worker_resource: 1}).remote(worker_fp)
            #if not self.workers:
            #    ray.get(w.start.remote())
            self.workers.append(w)
        self.batch_size = batch_size
        self.queue = asyncio.Queue(maxsize=batch_size)
        self.pass_by_value = pass_by_value
        self.results = []
        self.futures = []

    def start(self):
        print("batcher alive")
        ray.get([worker.start.remote() for worker in self.workers])

    def pid(self):
        worker_pid = ray.get(self.workers[0].pid.remote())
        return os.getpid(), worker_pid

    async def request(self, img_refs, request_start):
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        self.futures.append(fut)

        if self.pass_by_value:
            await self.queue.put((img_refs, request_start))
        else:
            await self.queue.put((img_refs[0], request_start))

        if self.queue.qsize() == self.batch_size:
            batch = []
            request_starts = []
            futures = []
            for i in range(self.batch_size):
                ref, request_start = await self.queue.get()
                batch.append(ref)
                request_starts.append(request_start)
                futures.append(self.futures.pop(0))
            tmp_worker = self.workers.pop(0)
            self.workers.append(tmp_worker)
            predictions = await self.workers[0].predict.remote(request_starts, *batch)
            for pred, future in zip(predictions, futures):
                future.set_result(pred)

        rand = np.random.rand() 
        if rand < self.batcher_fp:
            pid = os.getpid()
            os.kill(pid, signal.SIGKILL)

        return await fut

    def get_latencies(self):
        latencies = []
        for worker in self.workers:
            latencies += ray.get(worker.get_latencies.remote())
        return latencies


# Async actor to concurrently serve Batcher requests
#fashion_mnist = keras.datasets.fashion_mnist
#(train_images, train_labels), (test_images, test_labels) = fashion_mnist.load_data()
# print(test_images.shape)
@ray.remote
class Client:
    # One batcher per client
    def __init__(self, batcher, pass_by_value, worker_resource, request_rate, batch_size):
        self.batcher = batcher
        self.pass_by_value = pass_by_value
        self.worker_resource = worker_resource
        self.img = load_img('/home/ubuntu/ray/benchmarks/img.jpg', target_size = (224, 224))
        self.batch_interval = 1 / request_rate
        self.batch_size = batch_size

    def start(self):
        print("client alive")
        ray.get(self.batcher.start.remote())

    def run_concurrent(self, num_batches):
        results = []
        start = time.time()
        next_batch = start
        for i in range(num_batches):
            diff = next_batch - time.time()
            if diff > 0:
                time.sleep(diff)
            for j in range(self.batch_size):
                request_start = time.time()
                img_ref = preprocess.options(resources={self.worker_resource: 1}).remote(self.img)
                if self.pass_by_value:
                    ref = self.batcher.request.remote(img_ref, request_start)
                else:
                    ref = self.batcher.request.remote([img_ref], request_start)
                results.append(ref)
            # Backpressure.
            if i % 2 == 0 and len(results) >= self.batch_size * 4:
                done_ids, results = ray.wait(results, num_returns=len(results) // 2)
            next_batch += self.batch_interval
        print("Finished submitting", num_batches, "batches in", time.time() - start)
        ray.get(results)
        return

@ray.remote(num_cpus=0)
def kill(delay, pid):
    time.sleep(delay)
    print("KILLING", pid)
    os.kill(pid, signal.SIGKILL)
            

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Run the video benchmark.")

    parser.add_argument("--num-nodes", required=True, type=int)
    parser.add_argument("--num-batches-per-node", required=True, type=int)
    parser.add_argument("--batch-size", required=True, type=int)
    parser.add_argument("--num-clients-per-node", default=2, type=int)
    parser.add_argument("--num-batchers-per-node", default=1, type=int)
    parser.add_argument("--num-workers-per-node", default=8, type=int)
    parser.add_argument("--batcher-fp", default=0, type=float)
    parser.add_argument("--worker-fp", default=0, type=float)
    parser.add_argument("--pass-by-value", action="store_true")
    # How many batches per second each client should submit.
    parser.add_argument("--batch-rate", default=20, type=int)
    parser.add_argument("--output", default=None, type=str)
    parser.add_argument("--centralized", action="store_true")
    parser.add_argument("--kill-batcher", action="store_true")
    parser.add_argument("--kill-worker", action="store_true")
    args = parser.parse_args()

    ray.init(address="auto", log_to_driver=False)

    nodes = [node for node in ray.nodes() if node["Alive"]]
    while len(nodes) < args.num_nodes + 1:
        time.sleep(1)
        print("{} nodes found, waiting for nodes to join".format(len(nodes)))
        nodes = [node for node in ray.nodes() if node["Alive"]]

    import socket
    ip_addr = socket.gethostbyname(socket.gethostname())
    node_resource = "node:{}".format(ip_addr)

    for node in nodes:
        if node_resource in node["Resources"]:
            if "head" not in node["Resources"]:
                ray.experimental.set_resource("head", 100, node["NodeID"])

    for node in nodes:
        for resource in node["Resources"]:
            if resource.startswith("serve"):
                ray.experimental.set_resource(resource, 0, node["NodeID"])

    nodes = [node for node in ray.nodes() if node["Alive"]]
    print("All nodes joined")
    for node in nodes:
        print("{}:{}".format(node["NodeManagerAddress"], node["NodeManagerPort"]))

    head_node = [node for node in nodes if "head" in node["Resources"]]
    assert len(head_node) == 1
    head_ip = head_node[0]["NodeManagerAddress"]
    nodes.remove(head_node[0])
    # assert len(nodes) >= len(video_resources) + num_owner_nodes, ("Found {} nodes, need {}".format(len(nodes), len(video_resources) + num_owner_nodes))

    worker_resources = ["serve:{}".format(i) for i in range(args.num_nodes)]

    # Assign worker nodes
    assert len(nodes) >= len(worker_resources)
    for node, resource in zip(nodes, worker_resources):
        if "CPU" not in node["Resources"]:
            continue

        print("Assigning", resource, "to node", node["NodeID"], node["Resources"])
        ray.experimental.set_resource(resource, 200, node["NodeID"])

    if args.centralized:
        batcher_cls = CentralizedBatcher
    else:
        batcher_cls = ray.remote(max_restarts=-1, max_task_retries=-1)(Batcher)
    batchers = [batcher_cls.options(resources={worker_resources[i % len(worker_resources)]: 1}).remote(
        args.batch_size, args.batcher_fp, args.worker_fp, args.pass_by_value, (args.num_workers_per_node // args.num_batchers_per_node),
        worker_resources[i % len(worker_resources)]) for i  in range(args.num_batchers_per_node * args.num_nodes)]
    clients = [Client.options(resources={worker_resources[i % len(worker_resources)]: 1}).remote(
        batchers[i % len(batchers)], args.pass_by_value,
        worker_resources[i % len(worker_resources)], args.batch_rate,
        args.batch_size) for i in range(args.num_clients_per_node * args.num_nodes)]
    #[clients = Client.remote() for _ in range(numClients)]


    batcher_pid, worker_pid = ray.get(batchers[-1].pid.remote())
    print("BATCHER PID", batcher_pid, "on node", node["NodeManagerAddress"])
    print("WORKER PID", worker_pid, "on node", node["NodeManagerAddress"])
    print("RESOURCE", resource)

    # Start up clients
    for client in clients:
        ray.get(client.start.remote())

    print("Warmup for 10s")
    # 100 batches for warmup.
    ray.get([client.run_concurrent.remote(args.batch_rate * 10)])
    ray.get([b.get_latencies.remote() for b in batchers])
    print("Done with warmup")

    # Measure throughput
    #tstart = process_time()
    tstart = time.time()
    results = []
    num_batches = args.num_batches_per_node // args.num_clients_per_node
    for client in clients:
        results.append(client.run_concurrent.remote(num_batches))

    if args.kill_batcher or args.kill_worker:
        ray.get(kill.options(resources={resource: 1}).remote(
            num_batches // args.batch_rate // 2,
            batcher_pid if args.kill_batcher else worker_pid))

    ray.get(results)

    results = ray.get([b.get_latencies.remote() for b in batchers])
    if args.output is not None:
        for result in results:
            with open(args.output, 'a+') as f:
                for i, r in enumerate(result):
                    f.write("{} {}\n".format(i, r))

    results = [r for result in results for r in result]
    print("mean", np.mean(results))
    print("max", np.max(results))
    print("std", np.std(results))
    #tstop = process_time() 
    tstop = time.time()
    print("time: ", tstop-tstart)
    print("throughput: ", args.num_batches_per_node * args.num_nodes  * args.batch_size / (tstop-tstart))
