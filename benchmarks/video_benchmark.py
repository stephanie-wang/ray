import cv2
import os.path
import numpy as np
import time
import json
import threading
from ray import profiling

import ray
import ray.cluster_utils

YOLO_PATH = "/home/ubuntu/darknet"

NUM_FRAMES_PER_CHUNK = 300
MAX_FRAMES = 1200.0

CLEANUP = False

MSE_THRESHOLD = 100


def load_model():
    global net, ln
    weightsPath = os.path.join(YOLO_PATH, "yolov3-tiny.weights")
    configPath = os.path.join(YOLO_PATH, "cfg/yolov3-tiny.cfg")
    net = cv2.dnn.readNetFromDarknet(configPath, weightsPath)
    ln = net.getLayerNames()
    ln = [ln[i[0] - 1] for i in net.getUnconnectedOutLayers()]


@ray.remote(num_gpus=1)
def process_frame(frame):
    global net, ln
    if "net" not in globals():
        load_model()

    frame = frame / 255.
    net.setInput(frame)
    layerOutputs = net.forward(ln)

    classes = []
    for output in layerOutputs:
        for detection in output:
            scores = detection[5:]
            classId = np.argmax(scores)
            confidence = scores[classId]
            if confidence > 0:
                classes.append(classId)
    return classes


@ray.remote(resources={"preprocess": 1})
def load_frames(filename, start_frame, num_frames):
    filename = filename.decode("ascii")
    v = cv2.VideoCapture(filename)
    v.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
    frames = []
    for _ in range(num_frames):
        grabbed, frame = v.read()
        assert grabbed
        # Use uint8_t to reduce image size.
        frame = cv2.dnn.blobFromImage(
            frame, 1, (416, 416), swapRB=True, crop=False, ddepth=cv2.CV_8U)
        frames.append(frame)
    return frames


@ray.remote(resources={"preprocess": 1})
def compute_mse(frame1, frame2):
    return np.square(frame1 - frame2).mean()


@ray.remote(resources={"preprocess": 1})
def process_chunk_single_thread(filename, start_frame, num_frames):
    with profiling.profile("load_frames"):
        v = cv2.VideoCapture(filename)
        v.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
        frames = []
        for _ in range(num_frames):
            grabbed, frame = v.read()
            assert grabbed
            # Use uint8_t to reduce image size.
            frame = cv2.dnn.blobFromImage(
                frame,
                1, (416, 416),
                swapRB=True,
                crop=False,
                ddepth=cv2.CV_8U)
            frames.append(frame)

    last_frame_index = None
    results = []
    for i, frame in enumerate(frames):
        mse = MSE_THRESHOLD
        if last_frame_index is not None:
            with profiling.profile("mse"):
                mse = np.square(frame - frames[last_frame_index]).mean()
        if mse < MSE_THRESHOLD:
            results.append(results[last_frame_index])
        else:
            results.append(process_frame.remote(frame))
            last_frame_index = i
            if i != 0:
                print("switch frame", start_frame + i)
    return ray.get(results)


@ray.remote(resources={"preprocess": 1})
def process_chunk(filename, start_frame, num_frames):
    frames = load_frames.options(num_return_vals=num_frames).remote(
        filename, start_frame, num_frames)

    last_frame_index = None
    results = []
    for i, frame in enumerate(frames):
        mse = MSE_THRESHOLD
        if last_frame_index is not None:
            mse = ray.get(compute_mse.remote(frame, frames[last_frame_index]))
        if mse < MSE_THRESHOLD:
            results.append(results[last_frame_index])
        else:
            results.append(process_frame.remote(frame))
            last_frame_index = i
            if i != 0:
                print("switch frame", start_frame + i)
    return ray.get(results)


def process_video(video_pathname, num_total_frames):
    results = []
    futures = []
    start_frame = 0
    while start_frame < num_total_frames:
        if len(futures) >= 4:
            results += ray.get(futures.pop(0))
        print("Processing chunk at index", start_frame)
        num_frames = min(NUM_FRAMES_PER_CHUNK, num_total_frames - start_frame)
        futures.append(
            process_chunk.remote(video_pathname, start_frame, num_frames))
        start_frame += num_frames

    for f in futures:
        results += ray.get(f)


def main(num_nodes, video_path, local, test_failure, timeline):
    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 100000,
        "num_heartbeats_timeout": 10,
        "lineage_pinning_enabled": 1,
        "free_objects_period_milliseconds": -1,
        "object_manager_repeated_push_delay_ms": 1000,
        "task_retry_delay_ms": 100,
        "centralized_owner": 1,
    })
    if local:
        cluster = ray.cluster_utils.Cluster()
        cluster.add_node(
            num_cpus=0, _internal_config=internal_config, include_webui=False)
        for _ in range(2):
            cluster.add_node(
                object_store_memory=10**9,
                num_cpus=2,
                _internal_config=internal_config)
        for _ in range(1):
            cluster.add_node(
                object_store_memory=10**9,
                num_gpus=2,
                _internal_config=internal_config)
        cluster.wait_for_nodes()
        address = cluster.address
    else:
        address = "auto"

    ray.init(address=address, _internal_config=internal_config, redis_password='5241590000000000')

    nodes = ray.nodes()
    while len(nodes) < num_nodes + 1:
        time.sleep(1)
        print("{} nodes found, waiting for nodes to join".format(len(nodes)))
        nodes = ray.nodes()

    print("All nodes joined")
    for node in nodes:
        print("{}:{}".format(node["NodeManagerAddress"], node["NodeManagerPort"]))

    num_gpu_nodes = 2
    nodes = ray.nodes()[-num_nodes:]
    for node in nodes:
        if num_gpu_nodes > 0:
            ray.experimental.set_resource("GPU", 4, node["NodeID"])
            num_gpu_nodes -= 1
        elif "CPU" in node["Resources"]:
            ray.experimental.set_resource("preprocess", 100, node["NodeID"])
        #if "GPU" not in node["Resources"] and "CPU" in node["Resources"]:
        #    ray.experimental.set_resource("preprocess", 100, node["NodeID"])

    v = cv2.VideoCapture(video_path)
    num_total_frames = min(v.get(cv2.CAP_PROP_FRAME_COUNT), MAX_FRAMES)
    print("FRAMES", num_total_frames)

    start = time.time()

    if local and test_failure:
        t = threading.Thread(
            target=process_video, args=(video_path, num_total_frames))
        t.start()

        if test_failure:
            time.sleep(3)
            cluster.remove_node(preprocess_nodes[-1], allow_graceful=False)
            time.sleep(1)
            cluster.add_node(
                object_store_memory=10**9,
                num_cpus=2,
                resources={"preprocess": 100},
                _internal_config=internal_config)

        t.join()
    else:
        video_path = video_path.encode("ascii")
        process_video(video_path, num_total_frames)

    end = time.time()
    print("Finished in", end - start)
    print("Throughput:", num_total_frames / (end - start))

    if timeline:
        ray.timeline(filename=timeline)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run the video benchmark.")

    parser.add_argument("--num-nodes", required=True, type=int)
    parser.add_argument("--video-path", required=True, type=str)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--failure", action="store_true")
    parser.add_argument("--timeline", default=None, type=str)
    args = parser.parse_args()
    main(args.num_nodes, args.video_path, args.local, args.failure, args.timeline)
