from retro_event_retriever import RetroEventRetriever, sorted_set_retro_event_listeners_in_cache
from common_lib import sorted_set_events_with_listeners, sorted_set_event_listeners, hash_event_listeners, \
    sorted_set_retro_event_listeners, hash_retro_event_listeners
import conftest
import json

num_of_added_results_at_the_end_of_the_call = 2
num_of_added_results_due_to_count_reached_0 = 2


def test_wait_for_retro_event_listener_with_1_listener_having_attribute_datetime_start_that_should_retrieve_3_events(
        redis_retro_event_listener_entry_with_a_datetime_start, standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_1.com/"
    priority = 100
    transaction_id = "20200503174008.553417"
    datetime_start = transaction_id
    expected_listener_attributes = {
        "datetime_start": datetime_start,
    }
    expected_retro_listener_attributes = {
        "event_id": event_id,
        "url": url,
        "datetime_start": datetime_start,
    }

    num_of_events_expected = 3
    expected_events = [{
        "_id": "20200503174008.553421",
        "event_id": event_id,
        "transaction_id": transaction_id,
        "data": {
            "file_size": 67,
            "devices": [
                "OnatMacBookPro"
            ],
            "OnatMacBookPro:file_ids": [
                "1:c2875f140136afd4534fcd978c82af0b897fe96e"
            ]
        },
    }, {
        "_id": "20200503223628.840254",
        "event_id": event_id,
        "transaction_id": "20200503223628.840250",
        "data": {
            "devices": [
                "OnatMacBookPro"
            ],
            "OnatMacBookPro:file_ids": [
                "1:c2875f140136afd4534fcd978c82af0b897fe96e"
            ]
        },
    }, {
        "_id": "20200503223628.840266",
        "event_id": event_id,
        "transaction_id": transaction_id,
        "data": {
            "return_code": 0,
            "file_path_name": "/video_clips/output-20200503-090308.mp4",
            "transaction_id": transaction_id,
        },
    }]

    retriever = RetroEventRetriever()
    retriever.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    retriever.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    retriever.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    redis_client = retriever.establish_redis_connection()
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    common_lib = retriever.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    assert redis_client.zcard(sorted_set_retro_event_listeners) is 1
    assert redis_client.hlen(hash_retro_event_listeners) is 1
    assert redis_client.zcard(sorted_set_retro_event_listeners_in_cache) is 0
    assert redis_client.zcard(sorted_set_events_with_listeners) == 0
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) is 0
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) is 0
    result = retriever.wait_for_retro_event_listener()
    assert redis_client.zcard(sorted_set_retro_event_listeners) is 0
    assert redis_client.hlen(hash_retro_event_listeners) is 0
    assert redis_client.zcard(sorted_set_retro_event_listeners_in_cache) is 0
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(sorted_set_event_listeners + ':' + event_id, 0, -1, withscores=True)[0] == (
        url.encode('UTF-8'),
        priority,
    )
    listener_attributes = json.loads(redis_client.hmget(hash_event_listeners + ':' + event_id, url)[0].decode('UTF-8'))
    assert listener_attributes is not None
    assert listener_attributes == expected_listener_attributes

    assert len(result) == num_of_events_expected + num_of_added_results_at_the_end_of_the_call
    assert result[0] == (url, expected_events[0], expected_retro_listener_attributes)
    assert result[1] == (url, expected_events[1], expected_retro_listener_attributes)
    assert result[2] == (url, expected_events[2], expected_retro_listener_attributes)
    assert result[3] == {"registered_for_events": {
        event_id: {
            url: expected_listener_attributes,
        }
    }}
    assert result[4] == [1, 1]


def test_wait_for_retro_event_listener_with_1_listener_having_attributes_datetime_start_and_a_count_of_2_that_should_retrieve_2_events(
        redis_retro_event_listener_entry_with_a_datetime_start_and_a_count_of_2, standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_1.com/"
    priority = 100
    transaction_id = "20200503174008.553417"
    datetime_start = transaction_id
    expected_listener_attributes = {
        "datetime_start": datetime_start,
        "count": 0,
    }
    expected_retro_listener_attributes = [{
        "event_id": event_id,
        "url": url,
        "datetime_start": datetime_start,
        "count": 1,
    }, {
        "event_id": event_id,
        "url": url,
        "datetime_start": datetime_start,
        "count": 0,
    }]

    num_of_events_expected = 2
    expected_events = [{
        "_id": "20200503174008.553421",
        "event_id": event_id,
        "transaction_id": transaction_id,
        "data": {
            "file_size": 67,
            "devices": [
                "OnatMacBookPro"
            ],
            "OnatMacBookPro:file_ids": [
                "1:c2875f140136afd4534fcd978c82af0b897fe96e"
            ]
        },
    }, {
        "_id": "20200503223628.840254",
        "event_id": event_id,
        "transaction_id": "20200503223628.840250",
        "data": {
            "devices": [
                "OnatMacBookPro"
            ],
            "OnatMacBookPro:file_ids": [
                "1:c2875f140136afd4534fcd978c82af0b897fe96e"
            ]
        },
    }, {
        "_id": "20200503223628.840266",
        "event_id": event_id,
        "transaction_id": transaction_id,
        "data": {
            "return_code": 0,
            "file_path_name": "/video_clips/output-20200503-090308.mp4",
            "transaction_id": transaction_id,
        },
    }]

    retriever = RetroEventRetriever()
    retriever.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    retriever.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    retriever.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    redis_client = retriever.establish_redis_connection()
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    common_lib = retriever.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    assert redis_client.zcard(sorted_set_retro_event_listeners) is 1
    assert redis_client.hlen(hash_retro_event_listeners) is 1
    assert redis_client.zcard(sorted_set_retro_event_listeners_in_cache) is 0
    assert redis_client.zcard(sorted_set_events_with_listeners) == 0
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) is 0
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) is 0
    result = retriever.wait_for_retro_event_listener()
    assert redis_client.zcard(sorted_set_retro_event_listeners) is 0
    assert redis_client.hlen(hash_retro_event_listeners) is 0
    assert redis_client.zcard(sorted_set_retro_event_listeners_in_cache) is 0
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(sorted_set_event_listeners + ':' + event_id, 0, -1, withscores=True)[0] == (
        url.encode('UTF-8'),
        priority,
    )
    listener_attributes = json.loads(redis_client.hmget(hash_event_listeners + ':' + event_id, url)[0].decode('UTF-8'))
    assert listener_attributes is not None
    assert listener_attributes == expected_listener_attributes

    assert len(
        result) == num_of_events_expected + num_of_added_results_due_to_count_reached_0 + num_of_added_results_at_the_end_of_the_call
    assert result[0] == (url, expected_events[0], expected_retro_listener_attributes[0])
    assert result[1] == (url, expected_events[1], expected_retro_listener_attributes[1])
    assert result[2] == {"registered_for_events": {
        event_id: {
            url: expected_listener_attributes,
        }
    }}
    assert result[3] == [1, 1]
    assert result[4] == {"registered_for_events": {
        event_id: {
            url: expected_listener_attributes,
        }
    }}
    assert result[5] == [0, 0]


def test_wait_for_retro_event_listener_with_1_listener_having_attributes_datetime_start_and_a_count_of_5_that_should_retrieve_3_events_with_a_remaining_count_of_2_when_registered_as_an_event_listener(
        redis_retro_event_listener_entry_with_a_datetime_start_and_a_count_of_5, standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_1.com/"
    priority = 100
    transaction_id = "20200503174008.553417"
    datetime_start = transaction_id
    expected_listener_attributes = {
        "datetime_start": datetime_start,
        "count": 2,
    }
    expected_retro_listener_attributes = {
                                             "event_id": event_id,
                                             "url": url,
                                             "datetime_start": datetime_start,
                                             "count": 4,
                                         }, {
                                             "event_id": event_id,
                                             "url": url,
                                             "datetime_start": datetime_start,
                                             "count": 3,
                                         }, {
                                             "event_id": event_id,
                                             "url": url,
                                             "datetime_start": datetime_start,
                                             "count": 2,
                                         }

    num_of_events_expected = 3
    expected_events = [{
        "_id": "20200503174008.553421",
        "event_id": event_id,
        "transaction_id": transaction_id,
        "data": {
            "file_size": 67,
            "devices": [
                "OnatMacBookPro"
            ],
            "OnatMacBookPro:file_ids": [
                "1:c2875f140136afd4534fcd978c82af0b897fe96e"
            ]
        },
    }, {
        "_id": "20200503223628.840254",
        "event_id": event_id,
        "transaction_id": "20200503223628.840250",
        "data": {
            "devices": [
                "OnatMacBookPro"
            ],
            "OnatMacBookPro:file_ids": [
                "1:c2875f140136afd4534fcd978c82af0b897fe96e"
            ]
        },
    }, {
        "_id": "20200503223628.840266",
        "event_id": event_id,
        "transaction_id": transaction_id,
        "data": {
            "return_code": 0,
            "file_path_name": "/video_clips/output-20200503-090308.mp4",
            "transaction_id": transaction_id,
        },
    }]

    retriever = RetroEventRetriever()
    retriever.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    retriever.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    retriever.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    redis_client = retriever.establish_redis_connection()
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    common_lib = retriever.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    assert redis_client.zcard(sorted_set_retro_event_listeners) is 1
    assert redis_client.hlen(hash_retro_event_listeners) is 1
    assert redis_client.zcard(sorted_set_retro_event_listeners_in_cache) is 0
    assert redis_client.zcard(sorted_set_events_with_listeners) == 0
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) is 0
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) is 0
    result = retriever.wait_for_retro_event_listener()
    assert redis_client.zcard(sorted_set_retro_event_listeners) is 0
    assert redis_client.hlen(hash_retro_event_listeners) is 0
    assert redis_client.zcard(sorted_set_retro_event_listeners_in_cache) is 0
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(sorted_set_event_listeners + ':' + event_id, 0, -1, withscores=True)[0] == (
        url.encode('UTF-8'),
        priority,
    )
    listener_attributes = json.loads(redis_client.hmget(hash_event_listeners + ':' + event_id, url)[0].decode('UTF-8'))
    assert listener_attributes is not None
    assert listener_attributes == expected_listener_attributes

    assert len(result) == num_of_events_expected + num_of_added_results_at_the_end_of_the_call
    assert result[0] == (url, expected_events[0], expected_retro_listener_attributes[0])
    assert result[1] == (url, expected_events[1], expected_retro_listener_attributes[1])
    assert result[2] == (url, expected_events[2], expected_retro_listener_attributes[2])
    assert result[3] == {"registered_for_events": {
        event_id: {
            url: expected_listener_attributes,
        }
    }}
    assert result[4] == [1, 1]
