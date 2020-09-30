from nameko.rpc import rpc
from redis import Redis
from datetime import datetime
from common_lib import CommonLib


class EventListenerRegistrarMicroService:
    name = "event_listener_registrar_microservice"

    CONFIG = {
        'REDIS_HOST': "redis",
        'REDIS_DB': 0,
        'TESTING': False,
    }
    redis = None
    common_lib = None

    def establish_redis_connection(self):
        if self.redis is None:
            self.redis = Redis(host=self.CONFIG['REDIS_HOST'], db=self.CONFIG['REDIS_DB'], socket_connect_timeout=2, socket_timeout=2)
        return self.redis

    def get_common_lib_instance(self):
        if self.common_lib is None:
            self.common_lib = CommonLib()
        return self.common_lib

    @rpc
    def register_for_events(self, data):
        # TODO: REQUIRED: Should be able to accept multiple events also
        # print('In register_for_events: Listen for Event: ' + event_id + ', with JSON data: ' + str(data))
        response = {}
        common_lib = self.get_common_lib_instance()
        for event_to_register in data['events_to_register']:
            z_urls = {}
            h_urls = {}
            event_id = event_to_register['event']
            for listener_attributes in event_to_register['urls_to_register']:
                if 'transaction_id' in listener_attributes and 'datetime_start' not in listener_attributes:
                    common_lib.register_for_retro_event_retrieval(event_id, listener_attributes, response)
                    continue
                if ('datetime_start' in listener_attributes
                        and datetime.strptime(listener_attributes["datetime_start"], "%Y%m%d%H%M%S.%f") <= datetime.now()):
                    common_lib.register_for_retro_event_retrieval(event_id, listener_attributes, response)
                    continue
                common_lib.prepare_to_register_for_event_listeners(listener_attributes, z_urls, h_urls)
            if z_urls != {} and h_urls != {}:
                common_lib.register_to_event_listeners(event_id, z_urls, h_urls, response)
        return response
