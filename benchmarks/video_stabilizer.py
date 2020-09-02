import asyncio
import cv2
import os.path
import numpy as np
import time
import json
import threading
from collections import defaultdict
from ray import profiling
from ray.experimental.internal_kv import _internal_kv_put, \
    _internal_kv_get
import ray.cloudpickle as pickle

import ray
import ray.cluster_utils


NUM_WORKERS_PER_VIDEO = 1


@ray.remote(num_cpus=0, resources={"head": 1})
class SignalActor:
    def __init__(self, num_events):
        self.ready_event = asyncio.Event()
        self.num_events = num_events

    def send(self):
        assert self.num_events > 0
        self.num_events -= 1
        if self.num_events == 0:
            self.ready_event.set()

    async def wait(self, should_wait=True):
        if should_wait:
            await self.ready_event.wait()

    def ready(self):
        return

@ray.remote(num_cpus=0, resources={"head": 1})
class SignalActorV07:
    def __init__(self):
        self.num_signals = 0

    def send(self):
        self.num_signals += 1

    def wait(self):
        return self.num_signals

    def ready(self):
        return


class Decoder:
    def __init__(self, filename, start_frame):
        self.v = cv2.VideoCapture(filename)
        self.v.set(cv2.CAP_PROP_POS_FRAMES, start_frame)

    def decode(self, frame):
        if frame != self.v.get(cv2.CAP_PROP_POS_FRAMES):
            print("next frame", frame, ", at frame", self.v.get(cv2.CAP_PROP_POS_FRAMES))
            self.v.set(cv2.CAP_PROP_POS_FRAMES, frame)
        grabbed, frame = self.v.read()
        assert grabbed
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) 
        return frame

    def ready(self):
        return

class DecoderV07(ray.actor.Checkpointable):
    def __init__(self, filename, start_frame, radius, checkpoint_interval):
        self.v = cv2.VideoCapture(filename)
        self.v.set(cv2.CAP_PROP_POS_FRAMES, start_frame)

        self.filename = filename
        self.radius = radius
        self.checkpoint_interval = checkpoint_interval
        # Save the last `radius` many frames in memory so that we can reload
        # them after a failure.
        self.frame_buffer = []

        self.checkpoint_attrs = [
                "filename",
                "radius",
                "checkpoint_interval",
                "frame_buffer",
                ]

    def decode(self, frame):
        if frame != self.v.get(cv2.CAP_PROP_POS_FRAMES):
            print("next frame", frame, ", at frame", self.v.get(cv2.CAP_PROP_POS_FRAMES))
            self.v.set(cv2.CAP_PROP_POS_FRAMES, frame)
        grabbed, frame = self.v.read()
        assert grabbed
        # TODO: Save object IDs.
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) 
        self.frame_buffer.append(frame)
        if len(self.frame_buffer) > self.radius + 1:
            self.frame_buffer.pop(0)

        if self.checkpoint_interval > 0 and frame % self.checkpoint_interval == 0:
            self._should_checkpoint = True

        return frame

    def ready(self):
        return

    def should_checkpoint(self, checkpoint_context):
        should_checkpoint = self._should_checkpoint
        self._should_checkpoint = False
        return should_checkpoint

    def save_checkpoint(self, actor_id, checkpoint_id):
        with ray.profiling.profile("save_checkpoint"):
            checkpoint = {
                    attr: getattr(self, attr) for attr in self.checkpoint_attrs
                    }
            checkpoint = pickle.dumps(checkpoint)
            ray.experimental.internal_kv._internal_kv_put(checkpoint_id, checkpoint)

    def load_checkpoint(self, actor_id, available_checkpoints):
        if available_checkpoints:
            c = available_checkpoints[0]
            checkpoint = ray.experimental.internal_kv._internal_kv_get(c.checkpoint_id)
            checkpoint = pickle.loads(checkpoint)
            for attr, value in checkpoint.items():
                setattr(self, attr, value)
            self.v = cv2.VideoCapture(filename)
            self.v.set(cv2.CAP_PROP_POS_FRAMES, 0)
            return c


@ray.remote
def flow(prev_frame, frame, p0):
    with ray.profiling.profile("flow"):
        if p0 is None or p0.shape[0] < 100:
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
def cumsum(prev, next, checkpoint_key):
    with ray.profiling.profile("cumsum"):
        sum = [i + j for i, j in zip(prev, next)]
        if checkpoint_key is not None:
            ray.experimental.internal_kv._internal_kv_put(checkpoint_key, "{} {} {}".format(*sum))
        return sum



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


@ray.remote(num_cpus=0, resources={"head": 1})
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

    def ready(self):
        return


@ray.remote(num_cpus=0)
class Sink:
    def __init__(self, signal, viewer, checkpoint_interval):
        self.signal = signal
        self.num_frames_left = {}
        self.latencies = defaultdict(list)

        self.viewer = viewer
        self.last_view = None
        self.checkpoint_interval = checkpoint_interval

    def set_expected_frames(self, video_index, num_frames):
        self.num_frames_left[video_index] = num_frames

    def send(self, video_index, frame_index, transform, timestamp):
        with ray.profiling.profile("Sink.send"):
            if frame_index < len(self.latencies[video_index]):
                return
            assert frame_index == len(self.latencies[video_index]), frame_index

            self.latencies[video_index].append(time.time() - timestamp)

            self.num_frames_left[video_index] -= 1
            if self.num_frames_left[video_index] % 100 == 0:
                print("Expecting", self.num_frames_left[video_index], "more frames from video", video_index)

            if self.num_frames_left[video_index] == 0:
                print("DONE")
                if self.last_view is not None:
                    ray.get(self.last_view)
                self.signal.send.remote()

            if self.viewer is not None and video_index == 0:
                self.last_view = self.viewer.send.remote(transform)

            if self.checkpoint_interval != 0 and frame_index % self.checkpoint_interval == 0:
                print("Checkpointing video", video_index, "frame", frame_index)
                ray.experimental.internal_kv._internal_kv_put(video_index, frame_index, overwrite=True)

    def latencies(self):
        latencies = []
        for video in self.latencies.values():
            for i, l in enumerate(video):
                latencies.append((i, l))
        return latencies

    def ready(self):
        return


@ray.remote(num_cpus=0)
def process_chunk(video_index, video_pathname, sink, start_frame, num_frames, fps, resource, v07, start_timestamp, checkpoint_interval):
    decoder_cls_args = {
            "max_reconstructions": 100,
            } if v07 else {
            "max_restarts": -1,
            "max_task_retries": -1,
            }
    decoder_cls = ray.remote(**decoder_cls_args)(Decoder)
    decoder = decoder_cls.options(resources={resource: 1}).remote(video_pathname, 0)
    ray.get(decoder.ready.remote())

    # Check for a checkpoint.
    radius = fps
    checkpoint_frame = ray.experimental.internal_kv._internal_kv_get(video_index)
    trajectory = []
    if checkpoint_frame is not None:
        checkpoint_frame = int(checkpoint_frame)
        loaded = None
        while loaded is None and checkpoint_frame > 0:
            checkpoint_key = "{} {}".format(video_index, checkpoint_frame)
            print("Reloading checkpoint", checkpoint_key)
            checkpoint = ray.experimental.internal_kv._internal_kv_get(checkpoint_key)
            if checkpoint is not None:
                traj = [float(d) for d in checkpoint.decode('utf-8').split(' ')]
                print("Reloaded checkpoint", traj)
                trajectory.append(traj)
                break
            checkpoint_frame -= checkpoint_interval
            print("No checkpoint found, checking previous checkpoint at frame", video_index, checkpoint_frame)

        start_frame = int(checkpoint_frame)
        print("Restarting at frame", start_frame)
    next_to_send = start_frame
    start_frame -= radius
    if start_frame < 0:
        padding = start_frame * -1
        start_frame = 0
    else:
        padding = 0

    frame_timestamps = []
    transforms = []

    features = None

    frame_timestamp = start_timestamp + start_frame / fps
    diff = frame_timestamp - time.time()
    if diff > 0:
        time.sleep(diff)
    frame_timestamps.append(frame_timestamp)
    prev_frame = decoder.decode.remote(start_frame)

    for i in range(start_frame, num_frames - 1):
        frame_timestamp = start_timestamp + (start_frame + i + 1) / fps
        diff = frame_timestamp - time.time()
        if diff > 0:
            time.sleep(diff)
        frame_timestamps.append(frame_timestamp)

        frame = decoder.decode.remote(start_frame + i + 1)
        flow_options = {
                "num_return_vals": 2,
                } if v07 else {
                "num_returns": 2,
                }
        flow_options["resources"] = {resource: 1}

        transform, features = flow.options(**flow_options).remote(prev_frame, frame, features)
        if i and i % 200 == 0:
            features = None
        prev_frame = frame
        transforms.append(transform)
        if i > 0:
            if checkpoint_interval > 0 and (i + radius) % checkpoint_interval == 0:
                checkpoint_key = "{} {}".format(video_index, i + radius)
            else:
                checkpoint_key = None
            trajectory.append(cumsum.options(resources={resource: 1}).remote(trajectory[-1], transform, checkpoint_key))
        else:
            for _ in range(padding):
                trajectory.append(transform)
            trajectory.append(transform)

        if len(trajectory) == 2 * radius + 1:
            midpoint = radius
            final_transform = smooth.options(resources={resource: 1}).remote(transforms.pop(0), trajectory[midpoint], *trajectory)
            trajectory.pop(0)

            sink.send.remote(video_index, next_to_send, final_transform, frame_timestamps.pop(0))
            next_to_send += 1

    while next_to_send < num_frames - 1:
        trajectory.append(trajectory[-1])
        midpoint = radius
        final_transform = smooth.options(resources={resource: 1}).remote(transforms.pop(0), trajectory[midpoint], *trajectory)
        trajectory.pop(0)

        final = sink.send.remote(video_index, next_to_send, final_transform, frame_timestamps.pop(0))
        next_to_send += 1
    return ray.get(final)


def process_videos(video_pathnames, output_filename, view, resources,
        owner_resources, sink_resources, max_frames, num_sinks, v07,
        checkpoint_interval):
    if v07:
        signal = SignalActorV07.remote()
    else:
        signal = SignalActor.remote(len(video_pathnames))

    if view:
        viewer = Viewer.remote(video_pathnames[0])
        ray.get(viewer.ready.remote())
    else:
        viewer = None
    ray.get(signal.ready.remote())

    sinks = [Sink.options(resources={
        sink_resources[i % len(sink_resources)]: 1
        }).remote(signal, viewer, checkpoint_interval) for i in range(num_sinks)]
    ray.get([sink.ready.remote() for sink in sinks])

    for i, video_pathname in enumerate(video_pathnames):
        v = cv2.VideoCapture(video_pathname)
        num_total_frames = int(min(v.get(cv2.CAP_PROP_FRAME_COUNT), max_frames))
        print(video_pathname, "FRAMES", num_total_frames)
        ray.get(sinks[i % len(sinks)].set_expected_frames.remote(i, num_total_frames - 1))

    # Give the actors some time to start up.
    start_timestamp = time.time() + 5

    for i, video_pathname in enumerate(video_pathnames):
        v = cv2.VideoCapture(video_pathname)
        num_total_frames = int(min(v.get(cv2.CAP_PROP_FRAME_COUNT), max_frames))
        fps = v.get(cv2.CAP_PROP_FPS)
        owner_resource = owner_resources[i % len(owner_resources)]
        worker_resource = resources[i % len(resources)]
        print("Placing owner of video", i, "on node with resource", owner_resource)
        process_chunk.options(resources={owner_resource: 1}).remote(i, video_pathnames[i],
                sinks[i % len(sinks)], 0, num_total_frames,
                int(fps), worker_resource, v07, start_timestamp, checkpoint_interval)

    if v07:
        ready = 0
        while ready != len(video_pathnames):
            time.sleep(1)
            ready = ray.get(signal.wait.remote())
    else:
        ray.get(signal.wait.remote())

    latencies = []
    for sink in sinks:
        latencies += ray.get(sink.latencies.remote())
    if output_filename:
        with open(output_filename, 'w') as f:
            for t, l in latencies:
                f.write("{} {}\n".format(t, l))
    else:
        for latency in latencies:
            print(latency)
    latencies = [l for _, l in latencies]
    print("Mean latency:", np.mean(latencies))
    print("Max latency:", np.max(latencies))


def main(args):
    video_resources = ["video:{}".format(i) for i in range(len(args.video_path))]

    num_owner_nodes = len(args.video_path) // args.num_owners_per_node
    if len(args.video_path) % args.num_owners_per_node:
        num_owner_nodes += 1
    owner_resources = ["video_owner:{}".format(i) for i in range(num_owner_nodes)]

    num_sink_nodes = args.num_sinks // args.num_sinks_per_node
    if len(args.video_path) % args.num_sinks_per_node:
        num_sink_nodes += 1
    sink_resources = ["video_sink:{}".format(i) for i in range(num_sink_nodes)]

    num_required_nodes = len(args.video_path) + num_owner_nodes + num_sink_nodes
    assert args.num_nodes >= num_required_nodes, ("Requested {} nodes, need {}".format(args.num_nodes, num_required_nodes))

    if args.local:
        config = {
            "num_heartbeats_timeout": 10,
            "lineage_pinning_enabled": 1,
            "free_objects_period_milliseconds": -1,
            "object_manager_repeated_push_delay_ms": 1000,
            "task_retry_delay_ms": 100,
        }
        if args.centralized:
            config["centralized_owner"] = 1
        cluster = ray.cluster_utils.Cluster()
        cluster.add_node(
            num_cpus=0, _system_config=config, include_webui=False,
            resources={"head": 100})
        num_nodes = args.num_nodes
        for _ in range(num_nodes):
            cluster.add_node(
                object_store_memory=10**9,
                num_cpus=2,
                _internal_config=system_config)
        cluster.wait_for_nodes()
        address = cluster.address
    else:
        address = "auto"

    ray.init(address=address)

    nodes = [node for node in ray.nodes() if node["Alive"]]
    while len(nodes) < args.num_nodes + 1:
        time.sleep(1)
        print("{} nodes found, waiting for nodes to join".format(len(nodes)))
        nodes = [node for node in ray.nodes() if node["Alive"]]

    if not args.local:
        import socket
        ip_addr = socket.gethostbyname(socket.gethostname())
        node_resource = "node:{}".format(ip_addr)

        for node in nodes:
            if node_resource in node["Resources"]:
                if "head" not in node["Resources"]:
                    ray.experimental.set_resource("head", 100, node["NodeID"])

    for node in nodes:
        for resource in node["Resources"]:
            if resource.startswith("video"):
                ray.experimental.set_resource(resource, 0, node["NodeID"])

    nodes = [node for node in ray.nodes() if node["Alive"]]
    print("All nodes joined")
    for node in nodes:
        print("{}:{}".format(node["NodeManagerAddress"], node["NodeManagerPort"]))

    head_node = [node for node in nodes if "head" in node["Resources"]]
    assert len(head_node) == 1
    head_ip = head_node[0]["NodeManagerAddress"]
    nodes.remove(head_node[0])
    assert len(nodes) >= len(video_resources) + num_owner_nodes, ("Found {} nodes, need {}".format(len(nodes), len(video_resources) + num_owner_nodes))

    worker_ip = None
    worker_resource = None
    owner_ip = None
    owner_resource = None
    node_index = 0
    for node, resource in zip(nodes, sink_resources + owner_resources + video_resources):
        if "CPU" not in node["Resources"]:
            continue

        print("Assigning", resource, "to node", node["NodeID"], node["Resources"])
        ray.experimental.set_resource(resource, 100, node["NodeID"])

        if "owner" in resource:
            owner_resource = resource
            owner_ip = node["NodeManagerAddress"]
        elif "video:" in resource:
            worker_resource = resource
            worker_ip = node["NodeManagerAddress"]

    if args.failure or args.owner_failure:
        if args.local:
            t = threading.Thread(
                target=process_videos, args=(args.video_path, args.output,
                    args.view, video_resources, owner_resources, sink_resources,
                    args.max_frames, args.num_sinks, args.v07, args.checkpoint_interval))
            t.start()

            if args.failure:
                time.sleep(10)
                cluster.remove_node(cluster.list_all_nodes()[-1], allow_graceful=False)

                cluster.add_node(
                    object_store_memory=10**9,
                    num_cpus=2,
                    resources={"video:0": 100},
                    _internal_config=system_config)

            t.join()
        else:
            if args.failure:
                ip = worker_ip
                resource = worker_resource
            else:
                ip = owner_ip
                resource = owner_resource
            print("Killing", ip, "with resource", resource, "after 10s")
            def kill():
                cmd = 'ssh -i ~/ray_bootstrap_key.pem -o StrictHostKeyChecking=no {} "bash -s" -- < /home/ubuntu/ray/benchmarks/{} {}'.format(ip, "restart_0_7.sh" if args.v07 else "restart.sh", head_ip)
                print(cmd)
                time.sleep(10)
                os.system(cmd)
                recovered = False
                while not recovered:
                    time.sleep(1)
                    for node in ray.nodes():
                        if node["NodeManagerAddress"] == ip and "CPU" in node["Resources"] and resource not in node["Resources"]:
                            print(node)
                            ray.experimental.set_resource(resource, 100, node["NodeID"])
                            recovered = True
                            break
                print("Restarted node at IP", ip)
            t = threading.Thread(target=kill)
            t.start()
            process_videos(args.video_path, args.output, args.view,
                    video_resources, owner_resources, sink_resources, args.max_frames,
                    args.num_sinks, args.v07, args.checkpoint_interval)
            t.join()
    else:
        process_videos(args.video_path, args.output, args.view,
                video_resources, owner_resources, sink_resources, args.max_frames,
                args.num_sinks, args.v07, args.checkpoint_interval)

    if args.timeline:
        ray.timeline(filename=args.timeline)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run the video benchmark.")

    parser.add_argument("--num-nodes", required=True, type=int)
    parser.add_argument("--video-path", required=True, nargs='+', type=str)
    parser.add_argument("--output", type=str)
    parser.add_argument("--v07", action="store_true")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--failure", action="store_true")
    parser.add_argument("--owner-failure", action="store_true")
    parser.add_argument("--timeline", default=None, type=str)
    parser.add_argument("--view", action="store_true")
    parser.add_argument("--max-frames", default=600, type=int)
    parser.add_argument("--num-sinks", default=1, type=int)
    parser.add_argument("--num-sinks-per-node", default=1, type=int)
    parser.add_argument("--num-owners-per-node", default=1, type=int)
    parser.add_argument("--checkpoint-interval", default=0, type=int)
    args = parser.parse_args()
    main(args)
