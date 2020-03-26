import subprocess
import cv2
import os.path
import numpy as np
import time
import json
import threading

import ray
import ray.cluster_utils

OUTPUT_DIR = "/home/swang/data/images"
TEST_VIDEO = "/home/swang/data/test.mp4"
DURATION_CMD = "ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 {}"
DECODE_CMD = "ffmpeg -i {input} -vf fps={fps} -ss {start} -t {duration} {output_dir}/%03d.png"

NUM_FRAMES_PER_CHUNK = 10
FRAMES_PER_SECOND = 1
MAX_DURATION = 1200.0

CLEANUP = False

MSE_THRESHOLD = 100

def get_chunk_dir(chunk_index):
    chunk_dir = os.path.join(OUTPUT_DIR, "image-{}".format(chunk_index))
    return chunk_dir

def get_chunk_file(chunk_index, frame):
    chunk_dir = get_chunk_dir(chunk_index)
    filename = os.path.join(chunk_dir, "{:03d}.png".format(frame))
    return filename

@ray.remote(resources={"query": 1})
def process_frame(frame):
    time.sleep(0.1)
    return "a result"

@ray.remote(resources={"preprocess": 1})
def load_frame(filename):
    image = cv2.imread(filename)
    image = cv2.resize(image, (416, 416))
    return image

@ray.remote(resources={"preprocess": 1})
def compute_mse(frame1, frame2):
    return np.square(frame1 - frame2).mean()

@ray.remote(resources={"preprocess": 1})
def process_chunk(chunk_index, num_frames):
    frames = []
    for frame in range(num_frames):
        frame += 1
        filename = get_chunk_file(chunk_index, frame)
        frames.append(load_frame.remote(filename))

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
                print("SWITCHING", i)
    return ray.get(results)


def cleanup_chunk(chunk_index, num_frames):
    chunk_dir = get_chunk_dir(chunk_index)
    for frame in range(num_frames, 0, -1):
        filename = get_chunk_file(chunk_index, frame)
        os.remove(filename)
    os.rmdir(chunk_dir)


def decode_chunk(video_pathname, chunk_index, num_frames, start, duration):
    chunk_dir = get_chunk_dir(chunk_index)
    try:
        os.mkdir(chunk_dir)
    except FileExistsError:
        pass
    last_output = get_chunk_file(chunk_index, num_frames)
    if not os.path.exists(last_output):
        cmd = DECODE_CMD.format(
                input=video_pathname,
                start=start,
                fps=FRAMES_PER_SECOND,
                duration=duration,
                output_dir=chunk_dir).split()
        subprocess.check_output(cmd)
    else:
        print("Skipping chunk, {} already exists".format(last_output))

def process_video(video_pathname):
    chunk_duration = float(NUM_FRAMES_PER_CHUNK) / FRAMES_PER_SECOND
    start = 0
    chunk_index = 0
    results = []
    futures = []
    while start < MAX_DURATION:
        decode_chunk(TEST_VIDEO, chunk_index, NUM_FRAMES_PER_CHUNK, start, chunk_duration)
        if len(futures) >= 6:
            results += ray.get(futures.pop(0))
        futures.append(process_chunk.remote(chunk_index, NUM_FRAMES_PER_CHUNK))
        chunk_index += 1
        start += chunk_duration

    for f in futures:
        results += ray.get(f)

    if CLEANUP:
        for i in range(chunk_index):
            cleanup_chunk(i, NUM_FRAMES_PER_CHUNK)


def main():
    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 10,
        "lineage_pinning_enabled": 1,
    })
    cluster = ray.cluster_utils.Cluster()
    cluster.add_node(num_cpus=0, _internal_config=internal_config)
    preprocess_nodes = [cluster.add_node(num_cpus=2, resources={"preprocess": 100}, _internal_config=internal_config) for _ in range(2)]
    query_nodes = [cluster.add_node(num_cpus=2, resources={"query": 100}, _internal_config=internal_config) for _ in range(2)]
    cluster.wait_for_nodes()
    

    ray.init(address=cluster.address)
    start = time.time()

    #cmd = DURATION_CMD.format(TEST_VIDEO).split()
    #duration_s = subprocess.check_output(cmd)
    #duration_s = float(duration_s)

    def kill_node():
        cluster.remove_node(preprocess_nodes[-1], allow_graceful=False)

    t = threading.Timer(10.0, kill_node)
    t.start()


    process_video(TEST_VIDEO)

    end = time.time()
    num_frames = FRAMES_PER_SECOND * MAX_DURATION
    print("Finished in", end - start)
    print("Throughput:", num_frames / (end - start))

if __name__ == "__main__":
    main()
