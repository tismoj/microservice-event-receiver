# noinspection PyCompatibility
from urllib.parse import quote_plus
from datetime import datetime, timedelta
from redis import Redis, WatchError
import json
import pymongo
import requests
import socket
import time

sorted_set_received_events = "z_received_events"
hash_received_events = "h_received_events"
sorted_set_events_with_listeners = "z_events_with_listeners"
sorted_set_event_listeners = "z_event_listeners"
hash_event_listeners = "h_event_listeners"
sorted_set_retro_event_listeners = "z_retro_event_listeners"
hash_retro_event_listeners = "h_retro_event_listeners"


class CommonLib:
    CONFIG = {
        'MONGODB_HOST': "mongo",
        "MONGODB_DB": "microservice",
        "MONGODB_USER": "root",
        "MONGODB_PW": "example",
        'REDIS_HOST': "redis",
        'REDIS_DB': 0,
        'TESTING': False,
    }
    redis = None
    db = None

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

    def retrieve_from_db_required_data_from_any_recent_related_events(self, event, listener_attributes):
        found_all = True
        db = self.establish_mongodb_connection()
        if "required_data" in listener_attributes:
            for required_data in listener_attributes["required_data"]:
                if required_data not in event['data']:
                    search_attributes = {
                        "_id": {"$lt": event["_id"]}, "transaction_id": event["transaction_id"],
                        "data." + required_data: {"$exists": True}
                    }
                    event_containing_the_required_data = db.received_events.find_one(
                        search_attributes, sort=[("_id", pymongo.DESCENDING)]
                    )
                    if event_containing_the_required_data is not None:
                        event['data'][required_data] = event_containing_the_required_data['data'][required_data]
                    else:
                        found_all = False
                        print("[WARNING] Count not find required data '" + required_data + "' requested by listener '" +
                              listener_attributes['url'] + "'")
                        # TODO: REQUIRED: Report this as an event
                        break
        return found_all

    def prepare_to_register_for_event_listeners(self, data, z_urls, h_urls):
        url = data['url']
        priority = self.get_priority_if_present(data)
        z_urls[url] = priority
        h_urls[url] = {}
        if 'count' in data:
            h_urls[url]['count'] = int(data['count'])
        if 'expire_after' in data:
            expire_after = timedelta(seconds=int(data['expire_after']))
            h_urls[url]['datetime_end'] = (datetime.now() + expire_after).strftime("%Y%m%d%H%M%S.%f")
        if 'datetime_start' in data:
            h_urls[url]['datetime_start'] = str(data['datetime_start'])
        if 'datetime_end' in data:
            h_urls[url]['datetime_end'] = str(data['datetime_end'])
        if 'transaction_id' in data:
            h_urls[url]['transaction_id'] = str(data['transaction_id'])
        if 'listener_data' in data:
            h_urls[url]['listener_data'] = data['listener_data']
        if 'required_data' in data:
            h_urls[url]['required_data'] = data['required_data']

    @staticmethod
    def get_priority_if_present(data):
        if 'priority' in data:
            return float(data['priority'])
        else:
            return 100.00

    def register_to_event_listeners(self, event_id, z_urls, h_urls, response):
        new_urls_to_register = {}
        redis = self.establish_redis_connection()
        p = redis.pipeline(
            transaction=True  # transaction indicates whether all commands should be executed atomically
        )
        p.zadd(
            sorted_set_events_with_listeners, {event_id: 1},
            nx=False,
            # nx forces ZADD to only create new elements and not to update scores for elements that already exist.
            incr=True  # modifies ZADD to behave like ZINCRBY.
        )
        p.zadd(
            sorted_set_event_listeners + ":" + event_id, z_urls,
            nx=False
            # nx forces ZADD to only create new elements and not to update scores for elements that already exist.
        )
        for url, retrieved_data in h_urls.items():
            p.hmset(hash_event_listeners + ":" + event_id, {url: json.dumps(retrieved_data)})
        p.execute()
        for url, data in h_urls.items():
            new_urls_to_register[url] = json.loads(
                redis.hmget(hash_event_listeners + ":" + event_id, url)[0].decode('utf-8'))
        if 'registered_for_events' not in response:
            response["registered_for_events"] = {event_id: new_urls_to_register}
        else:
            response["registered_for_events"][event_id] = new_urls_to_register

    def register_for_retro_event_retrieval(self, event_id, listener_attributes, response):
        redis = self.establish_redis_connection()
        url = listener_attributes['url']
        priority = self.get_priority_if_present(listener_attributes)
        t_data = listener_attributes.copy()
        t_data.pop("url")
        listener_attributes['event_id'] = event_id
        p = redis.pipeline(
            transaction=True  # transaction indicates whether all commands should be executed atomically
        )
        p.zadd(
            sorted_set_retro_event_listeners, {url: priority},
            nx=False
            # nx forces ZADD to only create new elements and not to update scores for elements that already exist.
        )
        p.hmset(hash_retro_event_listeners, {url: json.dumps(listener_attributes)})
        p.execute()
        if "registered_for_retro_event_retrieval" not in response:
            response["registered_for_retro_event_retrieval"] = {event_id: {url: t_data}}
        elif event_id not in response["registered_for_retro_event_retrieval"]:
            response["registered_for_retro_event_retrieval"][event_id] = {url: t_data}
        else:
            response["registered_for_retro_event_retrieval"][event_id][url] = t_data

    @staticmethod
    def send_event_to_listener(url, data, retry=3, retry_interval=1.0):
        resp = None
        while retry > 0:
            try:
                payload = json.dumps(data)
                headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
                resp = requests.post(url, data=payload, headers=headers)
            except socket.error as e:
                print("[WARNING] socket.error (" + str(e) + ") thrown for url: " + url)
                time.sleep(retry_interval)
                retry -= 1
                continue
            retry = 0
        return resp

    def reduce_count_if_attribute_is_present(self, event_id, url):
        redis = self.establish_redis_connection()
        results = []
        while True:
            try:
                p = redis.pipeline(
                        transaction=True  # transaction indicates whether all commands should be executed atomically
                    )
                p.watch(
                    hash_event_listeners + ":" + event_id)  # Disables pipeline buffering and monitors the given name for changes up until multi() is called
                h_event_listener = p.hmget(
                    hash_event_listeners + ":" + event_id, url
                )  # With pipeline buffering disabled, results can be collected immediately
                if h_event_listener[0]:
                    print("Listener: " + url + ", data: " + str(h_event_listener))
                    listener_attributes = json.loads(h_event_listener[0])
                    if 'count' in listener_attributes:
                        if listener_attributes['count'] > 0:
                            listener_attributes['count'] -= 1
                            p.multi()  # Re-enables pipeline buffering, if the name indicated in watch() had not changed, otherwise will throw a WatchError exception
                            p.hmset(hash_event_listeners + ":" + event_id, {
                                        url: json.dumps(listener_attributes)
                                    })
                            results.append(p.execute())  # Collect the results of the remaining pipeline buffered calls
                break
            except WatchError:
                print("[WARNING] WatchError exception was thrown")
                continue
        return results
