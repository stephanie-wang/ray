import numpy as np
import ray
import ray.cloudpickle as pickle
import time
import uuid
from random import randint

from conf import *

def generate_id():
    return str(uuid.uuid4()).encode('ascii')


def generate_ad_to_campaign_map():
    campaign_ids = [generate_id() for _ in range(NUM_CAMPAIGNS)]
    campaign_to_ads = {}
    ad_to_campaign = {}
    for campaign_id in campaign_ids:
        campaign_to_ads[campaign_id] = [generate_id() for _ in range(NUM_ADS_PER_CAMPAIGN)]
        for ad_id in campaign_to_ads[campaign_id]:
            ad_to_campaign[ad_id] = campaign_id

    return campaign_ids, ad_to_campaign

@ray.remote
def generate(generator_dependencies, timestamp, time_slice_num_events):
    template, user_ids, page_ids, ad_ids, event_types, indices = generator_dependencies
    template_cp = template.copy()
    template_cp[:, 12:48] = user_ids[indices % NUM_USER_IDS]
    template_cp[:, 61:97] = page_ids[indices % NUM_PAGE_IDS]
    template_cp[:, 108:144] = ad_ids[indices % len(ad_ids)]
    template_cp[:, 180:190] = event_types[indices % 3]
    if len(timestamp) < 16:
        timestamp += b'0' * (16 - len(timestamp))
    template_cp[:, 205:221] = np.tile(np.array(memoryview(timestamp), dtype=np.uint8), 
                                        (time_slice_num_events, 1))
    return template_cp


@ray.remote
def init_generator(ad_to_campaign, time_slice_num_events):

    ad_ids = np.array([np.array(memoryview(x)) for x in list(ad_to_campaign.keys())])
    user_ids = np.array([np.array(memoryview(generate_id()), np.uint8) for _ in range(NUM_USER_IDS)])
    page_ids = np.array([np.array(memoryview(generate_id()), np.uint8) for _ in range(NUM_PAGE_IDS)])
    event_types = np.array([np.array(memoryview(b'"view"    ')),
                            np.array(memoryview(b'"click"   ')),
                            np.array(memoryview(b'"purchase"'))])

    id_array = np.empty(shape=(time_slice_num_events, 36), dtype=np.uint8)
    type_array = np.empty(shape=(time_slice_num_events, 10), dtype=np.uint8)
    time_array = np.empty(shape=(time_slice_num_events, 16), dtype=np.uint8)
    part1 = np.array(time_slice_num_events * [np.array(memoryview(b'{"user_id":"'), np.uint8)])
    part2 = np.array(time_slice_num_events * [np.array(memoryview(b'","page_id":"'), np.uint8)])
    part3 = np.array(time_slice_num_events * [np.array(memoryview(b'","ad_id":"'), np.uint8)])
    part4 = np.array(time_slice_num_events * [np.array(memoryview(b'","ad_type":"banner78","event_type":'), np.uint8)])
    part5 = np.array(time_slice_num_events * [np.array(memoryview(b',"event_time":"'), np.uint8)])
    part6 = np.array(time_slice_num_events * [np.array(memoryview(b'","ip_address":"1.2.3.4"}'), np.uint8)])
    indices = np.arange(time_slice_num_events)
    template = np.hstack([part1,
                          id_array,
                          part2,
                          id_array,
                          part3,
                          id_array,
                          part4,
                          type_array,
                          part5,
                          time_array,
                          part6])
    return template, user_ids, page_ids, ad_ids, event_types, indices
    
    template_id = ray.put(template)
    indices_id = ray.put(indices)
    ad_ids_id = ray.put(ad_ids)
    user_ids_id = ray.put(user_ids)
    page_ids_id = ray.put(page_ids)
    event_types_id = ray.put(event_types)

    return template_id, user_ids_id, page_ids_id, ad_ids_id, event_types_id, indices_id  


# Global variable to pre-generate the ad map.
CAMPAIGN_IDS, AD_TO_CAMPAIGN_MAP = generate_ad_to_campaign_map()
