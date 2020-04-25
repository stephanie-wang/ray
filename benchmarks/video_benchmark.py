import cv2
import os.path
import numpy as np
import time
import json
import threading
from ray import profiling

import ray
import ray.cluster_utils

YOLO_PATH = "/home/swang/darknet"

NUM_FRAMES_PER_CHUNK = 100
MAX_FRAMES = 500.0

MSE_THRESHOLD = 80


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
    time.sleep(0.02)
    #layerOutputs = net.forward(ln)

    classes = []
    #for output in layerOutputs:
    #    for detection in output:
    #        scores = detection[5:]
    #        classId = np.argmax(scores)
    #        confidence = scores[classId]
    #        if confidence > 0:
    #            classes.append(classId)
    return (classes, time.time())


@ray.remote(resources={"preprocess": 1})
def load_frames(filename, start_frame, num_frames):
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


@ray.remote(resources={"preprocess": 1}, num_cpus=0)
class Decoder:
    def __init__(self, filename, start_frame, start_timestamp):
        self.v = cv2.VideoCapture(filename)
        self.v.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
        self.start_timestamp = start_timestamp

    def decode(self):
        grabbed, frame = self.v.read()
        assert grabbed
        # Use uint8_t to reduce image size.
        frame = cv2.dnn.blobFromImage(
            frame, 1, (416, 416), swapRB=True, crop=False, ddepth=cv2.CV_8U)
        return frame


@ray.remote(resources={"preprocess": 1})
def compute_mse(frame1, frame2):
    return np.square(frame1 - frame2).mean()


@ray.remote(resources={"preprocess": 1})
def process_chunk(decoder, start_frame, num_frames, start_timestamp, fps):
    last_frame = None
    last_frame_index = 0

    frame_timestamps = [0] * num_frames
    result_timestamps = [0] * num_frames
    results = [None] * num_frames

    for i in range(num_frames):
        frame_timestamp = start_timestamp + (start_frame + i) / fps
        diff = frame_timestamp - time.time()
        if diff > 0:
            time.sleep(diff)
        frame_timestamps[i] = frame_timestamp

        frame = decoder.decode.remote()
        mse = MSE_THRESHOLD
        if last_frame is not None:
            mse = ray.get(compute_mse.remote(frame, last_frame))

        if mse < MSE_THRESHOLD:
            results[i] = results[last_frame_index]
            result_timestamps[i] = time.time()
        else:
            results[i] = process_frame.remote(frame)

            last_frame_index = i
            last_frame = frame
            if i != 0:
                print("switch frame", start_frame + i)

    results = ray.get(results)
    final = []
    for i, timestamp in enumerate(result_timestamps):
        timestamp = max(timestamp, results[i][1])
        latency = timestamp - frame_timestamps[i]
        final.append((results[i][0], latency))
    return final


@ray.remote(num_gpus=1)
def process_all_frames(filename, start_frame, num_frames, start_timestamp, fps):
    global net, ln
    if "net" not in globals():
        load_model()

    v = cv2.VideoCapture(filename)
    v.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
    start_timestamp = start_timestamp

    frame_timestamps = [0] * num_frames
    result_timestamps = [0] * num_frames
    results = [None] * num_frames

    for i in range(num_frames):
        frame_timestamp = start_timestamp + (start_frame + i) / fps
        diff = frame_timestamp - time.time()
        if diff > 0:
            time.sleep(diff)
        frame_timestamps[i] = frame_timestamp

        with ray.profiling.profile("load_frame"):
            grabbed, frame = v.read()
            assert grabbed
            # Use uint8_t to reduce image size.
            frame = cv2.dnn.blobFromImage(
                frame, 1, (416, 416), swapRB=True, crop=False, ddepth=cv2.CV_8U)

        with ray.profiling.profile("process_frame"):
            frame = frame / 255.
            net.setInput(frame)
            time.sleep(0.02)
            #layerOutputs = net.forward(ln)

            classes = []
            #for output in layerOutputs:
            #    for detection in output:
            #        scores = detection[5:]
            #        classId = np.argmax(scores)
            #        confidence = scores[classId]
            #        if confidence > 0:
            #            classes.append(classId)

            result_timestamps[i] = time.time()
            results[i] = classes

    final = []
    for i, timestamp in enumerate(result_timestamps):
        latency = timestamp - frame_timestamps[i]
        final.append((results[i], latency))
    return final


def process_video(video_pathname, num_total_frames, output_file, all_frames):
    futures = []
    start_frame = 0
    v = cv2.VideoCapture(video_pathname)
    fps = v.get(cv2.CAP_PROP_FPS)

    start_timestamp = time.time()
    decoder = Decoder.remote(video_pathname, start_frame, start_timestamp)

    while start_frame < num_total_frames:
        print("Processing chunk at index", start_frame)
        num_frames = min(NUM_FRAMES_PER_CHUNK, num_total_frames - start_frame)
        if all_frames:
            futures.append(
                process_all_frames.remote(video_pathname, start_frame, num_frames, start_timestamp, fps))
        else:
            futures.append(
                process_chunk.remote(decoder, start_frame, num_frames, start_timestamp, fps))

        start_frame += num_frames
        # Sleep until the next chunk of frames, if necessary.
        next_timestamp = start_timestamp + start_frame / fps
        diff = next_timestamp - time.time()
        if diff > 0:
            print("Sleeping", diff, "seconds")
            time.sleep(diff)

    results = [r for result in ray.get(futures) for r in result]
    results, latencies = zip(*results)
    if output_file:
        with open(output_file, 'w') as f:
            for l in latencies:
                f.write(str(l))
                f.write("\n")
    print("Mean latency:", np.mean(latencies))
    print("Max latency:", np.max(latencies))


def main(args):
    config = {
        "initial_reconstruction_timeout_milliseconds": 100000,
        "num_heartbeats_timeout": 10,
        "lineage_pinning_enabled": 1,
        "free_objects_period_milliseconds": -1,
        "object_manager_repeated_push_delay_ms": 1000,
        "task_retry_delay_ms": 100,
    }
    if args.centralized:
        config["centralized_owner"] = 1

    internal_config = json.dumps(config)
    if args.local:
        cluster = ray.cluster_utils.Cluster()
        cluster.add_node(
            num_cpus=0, _internal_config=internal_config, include_webui=False)
        for _ in range(args.num_nodes):
            cluster.add_node(
                object_store_memory=10**9,
                num_cpus=2,
                _internal_config=internal_config)
        cluster.wait_for_nodes()
        address = cluster.address
    else:
        address = "auto"

    ray.init(address=address, _internal_config=internal_config, redis_password='5241590000000000')

    nodes = ray.nodes()
    while len(nodes) < args.num_nodes + 1:
        time.sleep(1)
        print("{} nodes found, waiting for nodes to join".format(len(nodes)))
        nodes = ray.nodes()

    print("All nodes joined")
    for node in nodes:
        print("{}:{}".format(node["NodeManagerAddress"], node["NodeManagerPort"]))

    num_gpu_nodes = 2
    assert args.num_nodes > num_gpu_nodes
    nodes = ray.nodes()[-args.num_nodes:]
    for node in nodes:
        if num_gpu_nodes > 0:
            ray.experimental.set_resource("GPU", 4, node["NodeID"])
            num_gpu_nodes -= 1
        elif "CPU" in node["Resources"]:
            ray.experimental.set_resource("preprocess", 100, node["NodeID"])
        #if "GPU" not in node["Resources"] and "CPU" in node["Resources"]:
        #    ray.experimental.set_resource("preprocess", 100, node["NodeID"])

    v = cv2.VideoCapture(args.video_path)
    num_total_frames = min(v.get(cv2.CAP_PROP_FRAME_COUNT), MAX_FRAMES)
    print("FRAMES", num_total_frames)

    start = time.time()

    if args.local and args.failure:
        t = threading.Thread(
            target=process_video, args=(args.video_path, num_total_frames, args.output, args.all_frames))
        t.start()

        if args.failure:
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
        process_video(args.video_path, num_total_frames, args.output, args.all_frames)

    end = time.time()
    print("Finished in", end - start)
    print("Throughput:", num_total_frames / (end - start))

    if args.timeline:
        ray.timeline(filename=args.timeline)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run the video benchmark.")

    parser.add_argument("--num-nodes", required=True, type=int)
    parser.add_argument("--video-path", required=True, type=str)
    parser.add_argument("--output", type=str)
    parser.add_argument("--centralized", action="store_true")
    parser.add_argument("--all-frames", action="store_true")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--failure", action="store_true")
    parser.add_argument("--timeline", default=None, type=str)
    args = parser.parse_args()
    main(args)
