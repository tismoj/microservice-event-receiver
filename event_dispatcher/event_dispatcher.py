# noinspection PyCompatibility
from urllib.parse import quote_plus
from redis import Redis
from datetime import datetime
from common_lib import CommonLib, sorted_set_received_events, hash_received_events, sorted_set_event_listeners, \
    hash_event_listeners
import time
import pymongo
import re
import json


sorted_set_received_events_in_cache = "z_received_events_in_cache"
sorted_set_event_listeners_to_process = 'z_event_listeners_to_process'


class EventDispatcher:
    CONFIG = {
        'MONGODB_HOST': "mongo",
        "MONGODB_DB": "microservice",
        "MONGODB_USER": "root",
        "MONGODB_PW": "example",
        'REDIS_HOST': "redis",
        'REDIS_DB': 0,
        'WAIT_FOR_EVENT_INTERVAL': 0.1,
        'TESTING': False,
        'MOCKED': False,
    }
    redis = None
    db = None
    common_lib = None

    def establish_mongodb_connection(self):
        if self.db is None:
            uri = "mongodb://%s:%s@%s" % (
                quote_plus(self.CONFIG['MONGODB_USER']), quote_plus(self.CONFIG['MONGODB_PW']),
                quote_plus(self.CONFIG['MONGODB_HOST']))
            client = pymongo.MongoClient(uri)
            self.db = client[self.CONFIG['MONGODB_DB']]
        return self.db

    def establish_redis_connection(self):
        if self.redis is None:
            self.redis = Redis(host=self.CONFIG['REDIS_HOST'], db=self.CONFIG['REDIS_DB'], socket_connect_timeout=2, socket_timeout=2)
        return self.redis

    def get_common_lib_instance(self):
        if self.common_lib is None:
            self.common_lib = CommonLib()
        return self.common_lib

    def wait_for_event(self):
        redis = self.establish_redis_connection()
        db = self.establish_mongodb_connection()
        result = []
        while True:
            if redis.zcard(sorted_set_received_events) is not 0 and redis.zcard(sorted_set_received_events_in_cache) is 0:
                redis.zunionstore(
                    sorted_set_received_events_in_cache,
                    [sorted_set_received_events]
                )
            while redis.zcard(sorted_set_received_events_in_cache) is not 0:
                z_received_event = redis.zpopmin(
                        sorted_set_received_events_in_cache  # With pipeline buffering disabled, results can be collected immediately
                    )
                h_received_event = redis.hmget(hash_received_events, z_received_event[0][0])
                # TODO: REQUIRED: adding a semi-mandatory Hash CRC checking of the sent data
                # data_hash = None
                split_result = re.split(':', z_received_event[0][0].decode('UTF-8'), 3)
                if len(split_result) is 2:
                    event_id, timestamp = split_result
                else:
                    event_id, data_hash, timestamp = split_result
                data = json.loads(h_received_event[0].decode('UTF-8'))
                transaction_id = data['transaction_id']
                t_result = self.store_event_to_db(timestamp, event_id, transaction_id, data)
                if self.CONFIG['TESTING']:
                    result.append(t_result)
                data_to_be_checked = db.received_events.find_one({'_id': timestamp})['data']
                if data == data_to_be_checked:
                    with redis.pipeline(
                                transaction=True  # transaction indicates whether all commands should be executed atomically
                            ) as p:
                        p.zrem(sorted_set_received_events, z_received_event[0][0])
                        p.hdel(hash_received_events, z_received_event[0][0])
                        print("pipeline_results: " + str(p.execute()))
                else:
                    print("[WARNING] data from Redis and data from MongoDB are NOT the same")
                t_result = self.dispatch_event_to_listeners(event_id, timestamp, data)
                if self.CONFIG['TESTING']:
                    for item in t_result:
                        result.append(item)

            time.sleep(self.CONFIG['WAIT_FOR_EVENT_INTERVAL'])
            if self.CONFIG['TESTING']:
                break
        if self.CONFIG['TESTING']:
            return result

    def store_event_to_db(self, timestamp, event_id, transaction_id, data):
        db = self.establish_mongodb_connection()
        print("event_id: " + event_id)
        print("timestamp: " + timestamp)
        print("transaction_id: " + transaction_id)
        print("data: " + json.dumps(data))
        result = db.received_events.update({'_id': timestamp, 'event_id': event_id}, {
                '$setOnInsert': {'transaction_id': transaction_id},
                '$set': {'data': data}
            }, upsert=True)
        print(result)
        return result

    def dispatch_event_to_listeners(self, event_id, timestamp, event_data):
        result = []
        redis = self.establish_redis_connection()
        common_lib = self.get_common_lib_instance()
        redis.zunionstore(
            sorted_set_event_listeners_to_process + ':' + event_id,
            [sorted_set_event_listeners + ':' + event_id]
        )
        while redis.zcard(sorted_set_event_listeners_to_process + ':' + event_id) is not 0:
            z_event_listener = redis.zpopmin(
                sorted_set_event_listeners_to_process + ':' + event_id)  # With pipeline buffering disabled, results can be collected immediately
            if z_event_listener:
                print("Listener for event: " + event_id + ", data: " + str(z_event_listener))
                url = z_event_listener[0][0].decode('UTF-8')
                h_event_listener = redis.hmget(
                    hash_event_listeners + ":" + event_id, url
                )
                if h_event_listener:
                    print("Listener: " + url + ", data: " + str(h_event_listener))
                    listener_attributes = json.loads(h_event_listener[0])
                    if 'count' in listener_attributes:
                        if listener_attributes['count'] > 0:
                            listener_attributes['count'] -= 1
                        else:
                            continue
                    if 'datetime_start' in listener_attributes:
                        datetime_start = datetime.strptime(listener_attributes['datetime_start'], "%Y%m%d%H%M%S.%f")
                        if datetime_start > datetime.strptime(timestamp, "%Y%m%d%H%M%S.%f"):
                            continue
                    if 'datetime_end' in listener_attributes:
                        datetime_end = datetime.strptime(listener_attributes['datetime_end'], "%Y%m%d%H%M%S.%f")
                        if datetime_end < datetime.strptime(timestamp, "%Y%m%d%H%M%S.%f"):
                            continue
                    if 'required_data' in listener_attributes:
                        event = {
                                "_id": timestamp,
                                "transaction_id": event_data['transaction_id'],
                                "data": event_data
                            }
                        t_listener_attributes = listener_attributes.copy()
                        t_listener_attributes["url"] = url
                        found_all = common_lib.retrieve_from_db_required_data_from_any_recent_related_events(event, t_listener_attributes)
                        if not found_all:
                            continue
                    if self.CONFIG['TESTING']:
                        result.append((url, event_data, listener_attributes))
                        common_lib.reduce_count_if_attribute_is_present(event_id, url)
                        if not self.CONFIG['MOCKED']:
                            continue
                    resp = common_lib.send_event_to_listener(url, event_data)
                    if resp:
                        common_lib.reduce_count_if_attribute_is_present(event_id, url)
                    else:
                        print("[WARNING] Failed sending event to listener '" + url + "'")
                        response = {}
                        t_listener_attributes = listener_attributes.copy()
                        t_listener_attributes["url"] = url
                        t_listener_attributes["datetime_start"] = timestamp
                        common_lib.register_for_retro_event_retrieval(event_id, t_listener_attributes, response)
                        # TODO: Consider setting back-off algorithm listener attributes

        return result


if __name__ == '__main__':
    print("Starting the Event Dispatcher...")
    dispatcher = EventDispatcher()
    dispatcher.wait_for_event()
