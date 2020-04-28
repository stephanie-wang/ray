import asyncio
import cv2
import os.path
import numpy as np
import time
import json
import threading
from ray import profiling
from ray.test_utils import SignalActor

import ray
import ray.cluster_utils

YOLO_PATH = "/home/swang/darknet"

MAX_FRAMES = 600.0


@ray.remote(max_reconstructions=1)
class Decoder:
    def __init__(self, filename, start_frame, start_timestamp):
        self.v = cv2.VideoCapture(filename)
        self.v.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
        self.start_timestamp = start_timestamp

    def decode(self, frame):
        if frame != self.v.get(cv2.CAP_PROP_POS_FRAMES):
            self.v.set(cv2.CAP_PROP_POS_FRAMES, frame)
        grabbed, frame = self.v.read()
        assert grabbed
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) 
        return frame

@ray.remote(num_return_vals=2)
def flow(prev_frame, frame, p0):
    with ray.profiling.profile("flow"):
        if p0 is None:
            p0 = cv2.goodFeaturesToTrack(prev_frame,
                                         maxCorners=200,
                                         qualityLevel=0.01,
                                         minDistance=30,
                                         blockSize=3)

        # Calculate optical flow (i.e. track feature points)
        p1, status, err = cv2.calcOpticalFlowPyrLK(prev_frame, frame, p0, None) 

        # Sanity check
        assert p1.shape == p0.shape 

        # Filter only valid points
        good_new = p1[status==1]
        good_old = p0[status==1]

        #Find transformation matrix
        m, _ = cv2.estimateAffinePartial2D(good_old, good_new)
         
        # Extract translation
        dx = m[0,2]
        dy = m[1,2]

        # Extract rotation angle
        da = np.arctan2(m[1,0], m[0,0])
         
        # Store transformation
        transform = [dx,dy,da]
        # Update features to track. 
        p0 = good_new.reshape(-1, 1, 2)

        return transform, p0


@ray.remote
def cumsum(prev, next):
    with ray.profiling.profile("cumsum"):
        return [i + j for i, j in zip(prev, next)]


@ray.remote
def smooth(transform, point, *window):
    with ray.profiling.profile("smooth"):
        mean = np.mean(window, axis=0)
        smoothed = mean - point + transform
        return smoothed


def fixBorder(frame):
  s = frame.shape
  # Scale the image 4% without moving the center
  T = cv2.getRotationMatrix2D((s[1]/2, s[0]/2), 0, 1.04)
  frame = cv2.warpAffine(frame, T, (s[1], s[0]))
  return frame


@ray.remote(num_cpus=0)
class Viewer:
    def __init__(self, video_pathname):
        self.video_pathname = video_pathname
        self.v = cv2.VideoCapture(video_pathname)

    def send(self, transform):
        success, frame = self.v.read() 
        assert success

        # Extract transformations from the new transformation array
        dx, dy, da = transform

        # Reconstruct transformation matrix accordingly to new values
        m = np.zeros((2,3), np.float32)
        m[0,0] = np.cos(da)
        m[0,1] = -np.sin(da)
        m[1,0] = np.sin(da)
        m[1,1] = np.cos(da)
        m[0,2] = dx
        m[1,2] = dy

        # Apply affine wrapping to the given frame
        w = int(self.v.get(cv2.CAP_PROP_FRAME_WIDTH)) 
        h = int(self.v.get(cv2.CAP_PROP_FRAME_HEIGHT))
        frame_stabilized = cv2.warpAffine(frame, m, (w,h))

        # Fix border artifacts
        frame_stabilized = fixBorder(frame_stabilized) 

        # Write the frame to the file
        frame_out = cv2.hconcat([frame, frame_stabilized])

        ## If the image is too big, resize it.
        if(frame_out.shape[1] > 1920): 
            frame_out = cv2.resize(frame_out, (frame_out.shape[1]//2, frame_out.shape[0]//2));
        
        cv2.imshow("Before and After", frame_out)
        cv2.waitKey(1)
        #out.write(frame_out)


@ray.remote(num_cpus=0)
class Sink:
    def __init__(self, signal, num_frames, viewer):
        self.signal = signal
        self.num_frames = num_frames
        self.latencies = []

        self.viewer = viewer

    def send(self, frame_index, transform, timestamp):
        with ray.profiling.profile("Sink.send"):
            assert frame_index == len(self.latencies), frame_index

            self.latencies.append(time.time() - timestamp)
            if len(self.latencies) % 100 == 0:
                print("Received", len(self.latencies))
            if len(self.latencies) == self.num_frames:
                print("DONE")
                self.signal.send.remote()
            if self.viewer is not None:
                self.viewer.send.remote(transform)

    def latencies(self):
        return self.latencies


@ray.remote(num_cpus=0)
def process_chunk(decoder, sink, start_frame, num_frames, start_timestamp, fps):
    radius = fps

    frame_timestamps = []
    trajectory = []
    transforms = []

    features = None
    next_to_send = 0

    frame_timestamp = start_timestamp + start_frame / fps
    diff = frame_timestamp - time.time()
    if diff > 0:
        time.sleep(diff)
    frame_timestamps.append(frame_timestamp)
    prev_frame = decoder.decode.remote(start_frame)

    for i in range(num_frames - 1):
        frame_timestamp = start_timestamp + (start_frame + i + 1) / fps
        diff = frame_timestamp - time.time()
        if diff > 0:
            time.sleep(diff)
        frame_timestamps.append(frame_timestamp)

        frame = decoder.decode.remote(start_frame + i + 1)
        transform, features = flow.remote(prev_frame, frame, features)
        if i % 200 == 0:
            features = None
        prev_frame = frame
        transforms.append(transform)
        if i > 0:
            trajectory.append(cumsum.remote(trajectory[-1], transform))
        else:
            for _ in range(radius + 1):
                trajectory.append(transform)

        if len(trajectory) == 2 * radius + 1:
            midpoint = radius
            final_transform = smooth.remote(transforms.pop(0), trajectory[midpoint], *trajectory)
            trajectory.pop(0)

            sink.send.remote(next_to_send, final_transform, frame_timestamps.pop(0))
            next_to_send += 1

    while next_to_send < num_frames - 1:
        trajectory.append(trajectory[-1])
        midpoint = radius
        final_transform = smooth.remote(transforms.pop(0), trajectory[midpoint], *trajectory)
        trajectory.pop(0)

        final = sink.send.remote(next_to_send, final_transform, frame_timestamps.pop(0))
        next_to_send += 1
    return final


def process_video(video_pathname, num_total_frames, output_file, view):
    futures = []
    start_frame = 0
    v = cv2.VideoCapture(video_pathname)
    fps = v.get(cv2.CAP_PROP_FPS)

    start_timestamp = time.time() + 1
    decoder = Decoder.remote(video_pathname, start_frame, start_timestamp)
    signal = SignalActor.remote()
    if view:
        viewer = Viewer.remote(video_pathname)
    else:
        viewer = None
    sink = Sink.remote(signal, num_total_frames - 1, viewer)

    diff = start_timestamp - time.time()
    if diff > 0:
        time.sleep(diff)
    final = process_chunk.remote(decoder, sink, start_frame, int(num_total_frames), start_timestamp, int(fps))
    ray.get(ray.get(final))

    ray.get(signal.wait.remote())
    latencies = ray.get(sink.latencies.remote())
    if output_file:
        with open(output_file, 'w') as f:
            for l in latencies:
                f.write(str(l))
                f.write("\n")
    else:
        for latency in latencies:
            print(latency)
    print("Mean latency:", np.mean(latencies))
    print("Max latency:", np.max(latencies))


def main(args):
    config = {
        "initial_reconstruction_timeout_milliseconds": 100,
        "num_heartbeats_timeout": 10,
        "lineage_pinning_enabled": 1,
        "free_objects_period_milliseconds": -1,
        "object_manager_repeated_push_delay_ms": 1000,
        "task_retry_delay_ms": 1000,
    }
    if args.centralized:
        config["centralized_owner"] = 1

    internal_config = json.dumps(config)
    if args.local:
        cluster = ray.cluster_utils.Cluster()
        cluster.add_node(
            num_cpus=0, _internal_config=internal_config, include_webui=False)
        num_nodes = args.num_nodes
        for _ in range(num_nodes):
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

    v = cv2.VideoCapture(args.video_path)
    num_total_frames = min(v.get(cv2.CAP_PROP_FRAME_COUNT), MAX_FRAMES)
    print("FRAMES", num_total_frames)

    start = time.time()

    if args.local and args.failure:
        t = threading.Thread(
            target=process_video, args=(args.video_path, num_total_frames, args.output, args.view))
        t.start()

        if args.failure:
            time.sleep(5)
            cluster.remove_node(cluster.list_all_nodes()[-1], allow_graceful=False)

            cluster.add_node(
                object_store_memory=10**9,
                num_cpus=2,
                _internal_config=internal_config)


        t.join()
    else:
        process_video(args.video_path, num_total_frames, args.output, args.view)

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
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--failure", action="store_true")
    parser.add_argument("--timeline", default=None, type=str)
    parser.add_argument("--view", action="store_true")
    args = parser.parse_args()
    main(args)
