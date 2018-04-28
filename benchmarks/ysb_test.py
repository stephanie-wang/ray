import ray
import time

from ysb_tasks import *


class Test(GroupBy):
    def __init__(self):
        super().__init__()
        self.timestamps = []
    def process_elements(self, elements):
        self.timestamps.append(time.time())
        super().process_elements(elements)
    def get_timestamps(self):
        return self.timestamps

def test(e):
    now = time.time()
    print("now", now)
    l = e._push(e.generate_elements2()[1])
    return now


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-parsers', type=int, required=True)
    parser.add_argument('--num-filters', type=int, required=True)
    parser.add_argument('--num-projectors', type=int, required=True)
    args = parser.parse_args()

    ray.worker._init(
            start_ray_local=True,
            num_local_schedulers=1,
            resources=[{'node1': 100}],
            use_raylet=True,
            huge_pages=True,
            plasma_directory="/mnt/hugepages/")

    ads = generate_ads()
    a = [ray.remote(resources={})(Test).remote() for _ in range(2)]
    e = EventGenerator("node1", ads, 100, 62500, args.num_parsers, args.num_filters, args.num_projectors, *a)
    start = []
    for _ in range(50):
        start.append(test(e))
        time.sleep(0.1)
    time.sleep(1)
    end = ray.get(a[0].get_timestamps.remote())
    latencies = []
    for i, end_timestamp in enumerate(end):
        latencies.append(end_timestamp - start[i])
    print(latencies)
    print("Average latency", sum(latencies[-20:]) / 20)
