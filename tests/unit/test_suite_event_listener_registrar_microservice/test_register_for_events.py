from datetime import datetime
from common_lib import sorted_set_events_with_listeners, sorted_set_event_listeners, hash_event_listeners, \
    sorted_set_retro_event_listeners, hash_retro_event_listeners
import event_listener_registrar_microservice
import conftest
import json


def test_register_for_events_with_multiple_events_and_urls_to_register(redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id_1 = 'sequence_video_clips-melt_ended_event_1'
    url_1 = "http://some_url_1.com/"
    event_id_2 = 'sequence_video_clips-melt_ended_event_2'
    url_2 = "http://some_url_2.com/"
    url_3 = "http://some_url_3.com/"
    event_id_4 = 'sequence_video_clips-melt_ended_event_4'
    url_4 = "http://some_url_4.com/"
    event_id_5 = 'sequence_video_clips-melt_ended_event_5'
    url_5 = "http://some_url_5.com/"
    url_6 = "http://some_url_6.com/"
    datetime_start = '20191231235959.987654'  # YYmmddHHMMSS.nnnnnn datetime format in string
    transaction_id = '20200518193802.123456'  # YYmmddHHMMSS.nnnnnn datetime format in string
    priority = 100

    registrar = event_listener_registrar_microservice.EventListenerRegistrarMicroService()
    registrar.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    registrar.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    registrar.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    registrar.establish_redis_connection()
    common_lib = registrar.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    result = registrar.register_for_events({
        "events_to_register": [{
            "event": event_id_1,
            "urls_to_register": [{"url": url_1, "listener_data": {"a": 1}}]
        }, {
            "event": event_id_2,
            "urls_to_register": [{"url": url_2, "listener_data": {"b": 2}}, {"url": url_3, "listener_data": {"c": "3"}}]
        }, {
            "event": event_id_4,
            "urls_to_register": [{"url": url_4, "datetime_start": datetime_start}]
        }, {
            "event": event_id_5,
            "urls_to_register": [{
                "url": url_5, "datetime_start": datetime_start, "transaction_id": transaction_id
            }, {
                "url": url_6, "transaction_id": transaction_id
            }]
        }]
    })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 2
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id_1) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id_2) == 2
    assert redis_client.hlen(hash_event_listeners + ':' + event_id_1) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id_2) == 2
    assert redis_client.zrange(
        sorted_set_event_listeners +
        ':' + event_id_1, 0, -1, withscores=True) == \
           [(
               url_1.encode('UTF-8'),
               priority
           )]
    data = json.loads(redis_client.hmget(
        hash_event_listeners +
        ':' + event_id_1, url_1)[0].decode('UTF-8'))
    assert data is not None
    assert data == {"listener_data": {"a": 1}}
    assert redis_client.zrange(
        sorted_set_event_listeners +
        ':' + event_id_2, 0, -1, withscores=True) == \
           [(
               url_2.encode('UTF-8'),
               priority), (
               url_3.encode('UTF-8'),
               priority,
           )]
    data = json.loads(redis_client.hmget(
        hash_event_listeners +
        ':' + event_id_2, url_2)[0].decode('UTF-8'))
    assert data is not None
    assert data == {"listener_data": {"b": 2}}
    data = json.loads(redis_client.hmget(
        hash_event_listeners +
        ':' + event_id_2, url_3)[0].decode('UTF-8'))
    assert data is not None
    assert data == {"listener_data": {"c": "3"}}
    assert redis_client.zcard(sorted_set_retro_event_listeners) == 3
    assert redis_client.hlen(hash_retro_event_listeners) == 3
    assert redis_client.zrange(
        sorted_set_retro_event_listeners, 0, -1, withscores=True) == \
           [(
               url_4.encode('UTF-8'),
               priority
           ), (
               url_5.encode('UTF-8'),
               priority
           ), (
               url_6.encode('UTF-8'),
               priority
           )]
    data = json.loads(
        redis_client.hmget(hash_retro_event_listeners, url_4)[0].decode('UTF-8')
    )
    assert data is not None
    expected_data = {
        "datetime_start": datetime_start,
        "event_id": event_id_4,
        "url": url_4,
    }
    assert data == expected_data
    data = json.loads(
        redis_client.hmget(hash_retro_event_listeners, url_5)[0].decode('UTF-8')
    )
    assert data is not None
    expected_data = {
        "datetime_start": datetime_start,
        "transaction_id": transaction_id,
        "event_id": event_id_5,
        "url": url_5,
    }
    assert data == expected_data
    data = json.loads(
        redis_client.hmget(hash_retro_event_listeners, url_6)[0].decode('UTF-8')
    )
    assert data is not None
    expected_data = {
        "transaction_id": transaction_id,
        "event_id": event_id_5,
        "url": url_6,
    }
    assert data == expected_data
    assert result == {
        "registered_for_events": {
            event_id_1: {
                url_1: {"listener_data": {"a": 1}},
            },
            event_id_2: {
                url_2: {"listener_data": {"b": 2}},
                url_3: {"listener_data": {"c": "3"}},
            },
        },
        "registered_for_retro_event_retrieval": {
            event_id_4: {
                url_4: {"datetime_start": datetime_start},
            },
            event_id_5: {
                url_5: {"datetime_start": datetime_start, "transaction_id": transaction_id},
                url_6: {"transaction_id": transaction_id},
            },
        },
    }


def test_register_for_events_with_1_event_and_url_to_register_having_attribute_priority(redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event_2'
    url = "http://some_url_2.com/"
    priority = 99.99

    registrar = event_listener_registrar_microservice.EventListenerRegistrarMicroService()
    registrar.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    registrar.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    registrar.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    registrar.establish_redis_connection()
    common_lib = registrar.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    result = registrar.register_for_events({
        "events_to_register": [{
            "event": event_id,
            "urls_to_register": [{"url": url, "priority": priority}]
        }]
    })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
        sorted_set_event_listeners + ':' + event_id, 0, -1, withscores=True) == \
           [(
               url.encode('UTF-8'),
               priority
           )]
    data = json.loads(redis_client.hmget(
        hash_event_listeners + ':' + event_id, url)[0].decode('UTF-8'))
    assert data is not None
    assert data == {}
    assert result == {"registered_for_events": {
        event_id: {
            url: {},
        }
    }}


def test_register_for_events_with_1_event_and_url_to_register_having_attribute_count(redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event_3'
    url = "http://some_url_3.com/"
    priority = 100
    count = 1

    registrar = event_listener_registrar_microservice.EventListenerRegistrarMicroService()
    registrar.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    registrar.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    registrar.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    registrar.establish_redis_connection()
    common_lib = registrar.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    result = registrar.register_for_events({
        "events_to_register": [{
            "event": event_id,
            "urls_to_register": [{"url": url, "count": count}]
        }]
    })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
        sorted_set_event_listeners +
        ':' + event_id, 0, -1, withscores=True)[0] == (
               url.encode('UTF-8'),
               priority
           )
    data = json.loads(redis_client.hmget(
        hash_event_listeners +
        ':' + event_id, url)[0].decode('UTF-8'))
    assert data is not None
    expected_data = {"count": count}
    assert data == expected_data
    assert result == {"registered_for_events": {
        event_id: {
            url: expected_data,
        }
    }}


def test_register_for_events_with_1_event_and_url_to_register_having_attribute_expire_after_set_to_5_seconds(
        redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event_3'
    url = "http://some_url_3.com/"
    priority = 100
    expire_after = 1.0 * 5  # expire after 5 secs

    registrar = event_listener_registrar_microservice.EventListenerRegistrarMicroService()
    registrar.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    registrar.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    registrar.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    registrar.establish_redis_connection()
    common_lib = registrar.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    result = registrar.register_for_events({
        "events_to_register": [{
            "event": event_id,
            "urls_to_register": [{"url": url, "expire_after": 0, "priority": priority + 1}]
        }]
    })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
        sorted_set_event_listeners +
        ':' + event_id, 0, -1, withscores=True)[0] == (
               url.encode('UTF-8'),
               priority + 1
           )
    data = json.loads(redis_client.hmget(
        hash_event_listeners +
        ':' + event_id, url)[0].decode('UTF-8'))
    assert data is not None
    assert 'datetime_end' in data
    datetime_end = data['datetime_end']
    expected_data = {"datetime_end": datetime_end}
    assert data == expected_data
    assert result == {"registered_for_events": {
        event_id: {
            url: expected_data,
        }
    }}
    result = registrar.register_for_events({
        "events_to_register": [{
            "event": event_id,
            "urls_to_register": [{"url": url, "expire_after": expire_after}]
        }]
    })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
        sorted_set_event_listeners +
        ':' + event_id, 0, -1, withscores=True)[0] == (
               url.encode('UTF-8'),
               priority
           )
    data = json.loads(redis_client.hmget(
        hash_event_listeners +
        ':' + event_id, url)[0].decode('UTF-8'))
    assert data is not None
    assert 'datetime_end' in data
    datetime_end_after_5_sec = data['datetime_end']
    expected_data = {"datetime_end": datetime_end_after_5_sec}
    assert data == expected_data
    assert result == {"registered_for_events": {
        event_id: {
            url: expected_data,
        }
    }}
    assert (datetime.strptime(datetime_end_after_5_sec, "%Y%m%d%H%M%S.%f")
            - datetime.strptime(datetime_end, "%Y%m%d%H%M%S.%f")).total_seconds() >= 5.0
    assert (datetime.strptime(datetime_end_after_5_sec, "%Y%m%d%H%M%S.%f")
            - datetime.strptime(datetime_end, "%Y%m%d%H%M%S.%f")).total_seconds() < 6.0


def test_register_for_events_with_1_event_and_url_to_register_having_attribute_datetime_end(redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event_3'
    url = 'http://some_url_3.com/'
    priority = 100
    datetime_end = '20211231235959.987654'  # YYmmddHHMMSS.nnnnnn datetime format in string

    registrar = event_listener_registrar_microservice.EventListenerRegistrarMicroService()
    registrar.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    registrar.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    registrar.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    registrar.establish_redis_connection()
    common_lib = registrar.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    result = registrar.register_for_events({
        "events_to_register": [{
            "event": event_id,
            "urls_to_register": [{"url": url, "datetime_end": datetime_end}]
        }]
    })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
        sorted_set_event_listeners +
        ':' + event_id, 0, -1, withscores=True)[0] == (
               url.encode('UTF-8'),
               priority
           )
    data = json.loads(redis_client.hmget(
        hash_event_listeners +
        ':' + event_id, url)[0].decode('UTF-8'))
    assert data is not None
    expected_data = {"datetime_end": datetime_end}
    assert data == expected_data
    assert result == {"registered_for_events": {
        event_id: {
            url: expected_data,
        }
    }}


def test_register_for_events_with_1_event_and_url_to_register_having_attribute_datetime_start_set_into_the_future(
        redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event_3'
    url = 'http://some_url_3.com/'
    priority = 100
    datetime_start = '21200518193101.012345'  # YYmmddHHMMSS.nnnnnn datetime format in string

    registrar = event_listener_registrar_microservice.EventListenerRegistrarMicroService()
    registrar.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    registrar.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    registrar.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    registrar.establish_redis_connection()
    common_lib = registrar.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    result = registrar.register_for_events({
        "events_to_register": [{
            "event": event_id,
            "urls_to_register": [{"url": url, "datetime_start": datetime_start}]
        }]
    })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
        sorted_set_event_listeners +
        ':' + event_id, 0, -1, withscores=True)[0] == (
               url.encode('UTF-8'),
               priority
           )
    data = json.loads(redis_client.hmget(
        hash_event_listeners +
        ':' + event_id, url)[0].decode('UTF-8'))
    assert data is not None
    expected_data = {"datetime_start": datetime_start}
    assert data == expected_data
    assert result == {"registered_for_events": {
        event_id: {
            url: expected_data,
        }
    }}


def test_register_for_events_with_1_event_and_url_to_register_having_an_attribute_datetime_start_set_in_the_past(
        redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event_4'
    url = 'http://some_url_4.com/'
    priority = 100
    datetime_start = '20191231235959.987654'  # YYmmddHHMMSS.nnnnnn datetime format in string

    registrar = event_listener_registrar_microservice.EventListenerRegistrarMicroService()
    registrar.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    registrar.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    registrar.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    registrar.establish_redis_connection()
    common_lib = registrar.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    result = registrar.register_for_events({
        "events_to_register": [{
            "event": event_id,
            "urls_to_register": [{"url": url, "datetime_start": datetime_start}]
        }]
    })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 0
    assert redis_client.zcard(sorted_set_retro_event_listeners) == 1
    assert redis_client.hlen(hash_retro_event_listeners) == 1
    assert redis_client.zrange(
        sorted_set_retro_event_listeners, 0, -1, withscores=True)[0] == (
               url.encode('UTF-8'),
               priority
           )
    data = json.loads(
        redis_client.hmget(hash_retro_event_listeners, url)[0].decode('UTF-8')
    )
    assert data is not None
    expected_data = {
        "datetime_start": datetime_start,
        "event_id": event_id,
        "url": url,
    }
    expected_result_data = {"datetime_start": datetime_start}
    assert data == expected_data
    assert result == {"registered_for_retro_event_retrieval": {
        event_id: {
            url: expected_result_data,
        }
    }}


def test_register_for_events_with_1_event_and_url_to_register_having_attribute_transaction_id(redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event_5'
    url = 'http://some_url_5.com/'
    priority = 100
    transaction_id = '20200518193802.123456'  # YYmmddHHMMSS.nnnnnn datetime format in string

    registrar = event_listener_registrar_microservice.EventListenerRegistrarMicroService()
    registrar.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    registrar.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    registrar.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    registrar.establish_redis_connection()
    common_lib = registrar.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    result = registrar.register_for_events({
        "events_to_register": [{
            "event": event_id,
            "urls_to_register": [{"url": url, "transaction_id": transaction_id}]
        }]
    })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 0
    assert redis_client.zcard(sorted_set_retro_event_listeners) == 1
    assert redis_client.hlen(hash_retro_event_listeners) == 1
    assert redis_client.zrange(
        sorted_set_retro_event_listeners, 0, -1, withscores=True)[0] == (
               url.encode('UTF-8'),
               priority
           )
    data = json.loads(
        redis_client.hmget(hash_retro_event_listeners, url)[0].decode('UTF-8')
    )
    assert data is not None
    expected_data = {
        "transaction_id": transaction_id,
        "event_id": event_id,
        "url": url,
    }
    expected_result_data = {"transaction_id": transaction_id}
    assert data == expected_data
    assert result == {"registered_for_retro_event_retrieval": {
        event_id: {
            url: expected_result_data,
        }
    }}


def test_register_for_events_with_1_event_and_url_to_register_having_attributes_transaction_id_and_datetime_start_set_into_the_future(
        redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event_6'
    url = 'http://some_url_6.com/'
    priority = 100
    datetime_start = '21200518193101.012345'  # YYmmddHHMMSS.nnnnnn datetime format in string
    transaction_id = '20200518193802.123456'  # YYmmddHHMMSS.nnnnnn datetime format in string

    registrar = event_listener_registrar_microservice.EventListenerRegistrarMicroService()
    registrar.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    registrar.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    registrar.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    registrar.establish_redis_connection()
    common_lib = registrar.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    result = registrar.register_for_events({
        "events_to_register": [{
            "event": event_id,
            "urls_to_register": [{
                "url": url,
                "datetime_start": datetime_start,
                "transaction_id": transaction_id,
            }]
        }]
    })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
        sorted_set_event_listeners +
        ':' + event_id, 0, -1, withscores=True)[0] == (
               url.encode('UTF-8'),
               priority
           )
    data = json.loads(redis_client.hmget(
        hash_event_listeners +
        ':' + event_id, url)[0].decode('UTF-8'))
    assert data is not None
    expected_data = {
        "datetime_start": datetime_start,
        "transaction_id": transaction_id,
    }
    assert data == expected_data
    assert result == {"registered_for_events": {
        event_id: {
            url: expected_data,
        }
    }}


def test_register_for_events_with_1_event_and_url_to_register_having_data_for_the_listener(redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event_7'
    url = 'http://some_url_7.com/'
    priority = 100.00
    listener_data = {
        "some_key": "some_value"}  # Dictionary data meant for the listener that is set only during registration

    registrar = event_listener_registrar_microservice.EventListenerRegistrarMicroService()
    registrar.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    registrar.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    registrar.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    registrar.establish_redis_connection()
    common_lib = registrar.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    result = registrar.register_for_events({
        "events_to_register": [{
            "event": event_id,
            "urls_to_register": [{"url": url, "listener_data": listener_data}]
        }]
    })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
        sorted_set_event_listeners +
        ':' + event_id, 0, -1, withscores=True)[0] == (
               url.encode('UTF-8'),
               priority
           )
    data = json.loads(redis_client.hmget(
        hash_event_listeners +
        ':' + event_id, url)[0].decode('UTF-8'))
    assert data is not None
    expected_data = {"listener_data": listener_data}
    assert data == expected_data
    assert result == {"registered_for_events": {
        event_id: {
            url: expected_data,
        }
    }}


def test_register_for_events_with_1_event_and_url_to_register_having_attribute_required_data(redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event_8'
    url = 'http://some_url_8.com/'
    priority = 100.00
    required_data = ["data_key_1",
                     "data_key_2"]  # Dictionary data meant for the listener that is set only during registration

    registrar = event_listener_registrar_microservice.EventListenerRegistrarMicroService()
    registrar.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    registrar.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    registrar.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    registrar.establish_redis_connection()
    common_lib = registrar.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    result = registrar.register_for_events({
        "events_to_register": [{
            "event": event_id,
            "urls_to_register": [{"url": url, "required_data": required_data}]
        }]
    })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
        sorted_set_event_listeners +
        ':' + event_id, 0, -1, withscores=True)[0] == (
               url.encode('UTF-8'),
               priority
           )
    data = json.loads(redis_client.hmget(
        hash_event_listeners +
        ':' + event_id, url)[0].decode('UTF-8'))
    assert data is not None
    expected_data = {"required_data": required_data}
    assert data == expected_data
    assert result == {"registered_for_events": {
        event_id: {
            url: expected_data,
        }
    }}
