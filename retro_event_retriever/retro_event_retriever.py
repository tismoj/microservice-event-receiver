# noinspection PyCompatibility
from urllib.parse import quote_plus
from redis import Redis
from common_lib import CommonLib, sorted_set_retro_event_listeners, hash_retro_event_listeners
import time
import pymongo
import json


sorted_set_retro_event_listeners_in_cache = 'z_retro_event_listeners_in_cache'


class RetroEventRetriever:
    CONFIG = {
        'MONGODB_HOST': "mongo",
        "MONGODB_DB": "microservice",
        "MONGODB_USER": "root",
        "MONGODB_PW": "example",
        'REDIS_HOST': "redis",
        'REDIS_DB': 0,
        'WAIT_FOR_LISTENER_INTERVAL': 0.1,
        'TESTING': False,
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

    def wait_for_retro_event_listener(self):
        result, retrieved_events = [], []
        redis = self.establish_redis_connection()
        common_lib = self.get_common_lib_instance()
        while True:
            if redis.zcard(sorted_set_retro_event_listeners) is not 0 \
                    and redis.zcard(sorted_set_retro_event_listeners_in_cache) is 0:
                redis.zunionstore(
                    sorted_set_retro_event_listeners_in_cache,
                    [sorted_set_retro_event_listeners]
                )
            while redis.zcard(sorted_set_retro_event_listeners_in_cache) is not 0:
                response, z_urls, h_urls = {}, {}, {}
                url, priority = redis.zpopmin(sorted_set_retro_event_listeners_in_cache)[0]
                url = url.decode('UTF-8')
                listener_attributes = json.loads(redis.hmget(hash_retro_event_listeners, url)[0].decode('UTF-8'))
                event_id = listener_attributes["event_id"]
                for event in self.retrieve_events_from_db(listener_attributes):
                    if 'count' in listener_attributes:
                        if listener_attributes['count'] > 0:
                            listener_attributes['count'] -= 1
                        else:
                            common_lib.prepare_to_register_for_event_listeners(listener_attributes, z_urls, h_urls)
                            common_lib.register_to_event_listeners(event_id, z_urls, h_urls, response)
                            if self.CONFIG['TESTING']:
                                result.append(response)
                            self.remove_retro_event_listener(url, result)
                            break
                    if self.CONFIG['TESTING']:
                        result.append((url, event, listener_attributes.copy()))
                        common_lib.reduce_count_if_attribute_is_present(event_id, url)
                        continue
                    resp = common_lib.send_event_to_listener(url, event['data'])
                    if resp:
                        common_lib.reduce_count_if_attribute_is_present(event_id, url)
                    else:
                        print("[WARNING] Failed sending event to listener '" + url + "'")
                        # TODO: REQUIRED: Adding a back-off algorithm
                        break
                # TODO: if found_all from retrieve_events_from_db() is False the listener should remain a retro listener or removed TBD
                common_lib.prepare_to_register_for_event_listeners(listener_attributes, z_urls, h_urls)
                common_lib.register_to_event_listeners(event_id, z_urls, h_urls, response)
                if self.CONFIG['TESTING']:
                    result.append(response)
                self.remove_retro_event_listener(url, result)

            time.sleep(self.CONFIG['WAIT_FOR_LISTENER_INTERVAL'])
            if self.CONFIG['TESTING']:
                break
        if self.CONFIG['TESTING']:
            return result

    def remove_retro_event_listener(self, url, result):
        redis = self.establish_redis_connection()
        with redis.pipeline(
                transaction=True  # transaction indicates whether all commands should be executed atomically
        ) as p:
            p.zrem(sorted_set_retro_event_listeners, url)
            p.hdel(hash_retro_event_listeners, url)
            pipeline_result = p.execute()
            if self.CONFIG['TESTING']:
                result.append(pipeline_result)
            print("pipeline_results: " + str(pipeline_result))

    def retrieve_events_from_db(self, listener_attributes):
        retrieved_events = []
        # datetime_start, datetime_end, transaction_id, required_data = None, None, None, None
        db = self.establish_mongodb_connection()
        common_lib = self.get_common_lib_instance()
        event_id = listener_attributes["event_id"]
        search_attributes = {"event_id": event_id}
        if "datetime_start" in listener_attributes:
            datetime_start = listener_attributes["datetime_start"]
            search_attributes["_id"] = {"$gte": datetime_start}
        if "datetime_end" in listener_attributes:
            datetime_end = listener_attributes["datetime_end"]
            if "_id" not in search_attributes:
                search_attributes["_id"] = {"$lte": datetime_end}
            else:
                search_attributes["_id"]["$lte"] = datetime_end
        if "transaction_id" in listener_attributes:
            transaction_id = listener_attributes["transaction_id"]
            search_attributes["transaction_id"] = transaction_id
        for event in db.received_events.find(search_attributes):  # Returns an iterator and should be iterated
            found_all = common_lib.retrieve_from_db_required_data_from_any_recent_related_events(event, listener_attributes)
            if not found_all:
                break
            retrieved_events.append(event)
        return retrieved_events


if __name__ == '__main__':
    print("Starting the Retro Event Retriever...")
    retriever = RetroEventRetriever()
    retriever.wait_for_retro_event_listener()
