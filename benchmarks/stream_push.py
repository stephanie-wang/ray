import ray
import time
import uuid

@ray.remote
class Mapper(object):
    def __init__(self, ad_to_campaign_map):
        self.ad_to_campaign_map = ad_to_campaign_map
        self.events = []

    def push(self, event):
        self.events.append(event)

    def last(self):
        if len(self.events) > 0:
            timestamp, _ = self.events[-1].split(" ")
            return len(self.events), float(timestamp)
        else:
            return len(self.events), 0

@ray.remote
class Generator(object):
    def __init__(self, ad_to_campaign_map, *receivers):
        self.receivers = receivers
        self.ad_to_campaign_map = ad_to_campaign_map
        self.ads = list(self.ad_to_campaign_map.values())

    def init(self):
        return

    def generate_events(self):
        i = 0
        while True:
            now = time.time()
            event = str(now) + " " + self.ads[i]
            [receiver.push.remote(event) for receiver in self.receivers]
            i += 1
            i %= len(self.ad_to_campaign_map)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--use-raylet', action='store_true')
    parser.add_argument('--num-mappers', type=int, required=True, default=1)
    parser.add_argument('--num-generators', type=int, default=1)

    args = parser.parse_args()

    ray.init(use_raylet=args.use_raylet)

    num_campaigns = 10
    num_ads_per_campaign = 10
    sleep_time = 10

    campaign_ids = [str(uuid.uuid4()) for _ in range(num_campaigns)]
    campaign_to_ad_map = {}
    ad_to_campaign_map = {}
    for campaign_id in campaign_ids:
        campaign_to_ad_map[campaign_id] = [str(uuid.uuid4()) for _ in range(num_ads_per_campaign)]
        for ad_id in campaign_to_ad_map[campaign_id]:
            ad_to_campaign_map[ad_id] = campaign_id
    ad_ids = ad_to_campaign_map.values()

    mappers = [Mapper.remote(ad_to_campaign_map) for _ in range(args.num_mappers)]
    ray.get([mapper.last.remote() for mapper in mappers])
    generators = [Generator.remote(ad_to_campaign_map, *mappers) for _ in range(args.num_generators)]
    ray.get([generator.init.remote() for generator in generators])
    time.sleep(1)

    [generator.generate_events.remote() for generator in generators]
    start = time.time()

    time.sleep(sleep_time)
    results = ray.get([mapper.last.remote() for mapper in mappers])
    for num_events, end in results:
        print("Achieved throughput for mapper was", num_events / (end - start))
