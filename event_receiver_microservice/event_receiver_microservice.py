from nameko.rpc import rpc
from redis import Redis
from datetime import datetime
from common_lib import sorted_set_received_events, hash_received_events
import json


class EventReceiverMicroService:
    name = "event_receiver_microservice"

    # Connect to Redis
    redis = Redis(host="redis", db=0, socket_connect_timeout=2, socket_timeout=2)

    @rpc
    def receive_event(self, event_id, data):
        response = {}
        # print('In receive_event: Received event: ' + event_id + ', with JSON data: ' + str(data))
        datetime_formatted = datetime.now().strftime("%Y%m%d%H%M%S.%f")  # 20190531100739.0123456
        if 'transaction_id' in data:
            transaction_id = data['transaction_id']
        else:
            transaction_id = datetime_formatted
            data['transaction_id'] = transaction_id
        p = self.redis.pipeline(
                transaction=True  # transaction indicates whether all commands should be executed atomically
            )
        p.zadd(sorted_set_received_events, {
                event_id + ":" + datetime_formatted: transaction_id
            }, nx=True)  # nx forces ZADD to only create new elements and not to update scores for elements that already exist.
        p.hmset(hash_received_events, {
                event_id + ":" + datetime_formatted: json.dumps(data)
            })
        p.execute()
        # print('In receive_event: Received event: ' + event_id + ', with JSON data: '
        #       + self.redis.hget(hash_received_events, event_id + ":" + datetime_formatted).decode('utf-8'))
        # return "Received event: {}, with JSON data: {}".format(event_id + ":" + datetime_formatted, json.dumps(data))
        # return "Received event: {}, with JSON data: {}".format(event_id + ":" + datetime_formatted
        #         , self.redis.hget(hash_received_events, event_id + ":" + datetime_formatted).decode('utf-8'))
        response['event'] = event_id + ":" + datetime_formatted
        response['data'] = json.loads(self.redis.hmget(hash_received_events, event_id + ":" + datetime_formatted)[0].decode('utf-8'))
        return {'event_received': response}
