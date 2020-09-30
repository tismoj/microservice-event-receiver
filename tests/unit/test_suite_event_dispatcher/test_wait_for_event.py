from common_lib import CommonLib
import event_dispatcher
import conftest


def test_wait_for_event_without_a_registered_event_listener_when_an_event_received(db_client, standard_redis_received_event_entry):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    dispatcher = event_dispatcher.EventDispatcher()
    dispatcher.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    dispatcher.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    dispatcher.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    redis_client = dispatcher.establish_redis_connection()
    dispatcher.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    dispatcher.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    db_client = dispatcher.establish_mongodb_connection()
    common_lib = dispatcher.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    result = dispatcher.wait_for_event()
    assert redis_client.zcard(event_dispatcher.sorted_set_received_events_in_cache) == 0
    assert redis_client.zcard(event_dispatcher.sorted_set_received_events) == 0
    assert redis_client.hlen(event_dispatcher.hash_received_events) == 0
    assert len(result) is 1
    assert 'upserted' in result[0]
    assert result[0] == {
            'n': 1, 'nModified': 0, 'upserted': '20200503223628.840265', 'ok': 1.0, 'updatedExisting': False
        }
    assert db_client.received_events.count_documents({}) == 1
    received_events = db_client.received_events.find_one({'_id': "20200503223628.840265"})
    assert received_events is not None
    assert received_events['_id'] == "20200503223628.840265"
    assert received_events['event_id'] == "sequence_video_clips-melt_ended_event"
    assert received_events['transaction_id'] == "20200503174008.553417"
    data = received_events['data']
    assert data is not None
    assert data['return_code'] is 0
    assert data['file_path_name'] == "/video_clips/output-20200503-090308.mp4"
    assert data['transaction_id'] == "20200503174008.553417"


def test_wait_for_event_with_a_registered_event_listener_when_an_event_received(db_client, standard_redis_received_event_entry, standard_redis_event_listener_entry):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    dispatcher = event_dispatcher.EventDispatcher()
    dispatcher.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    dispatcher.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    dispatcher.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    redis_client = dispatcher.establish_redis_connection()
    dispatcher.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    dispatcher.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    db_client = dispatcher.establish_mongodb_connection()
    common_lib = dispatcher.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    result = dispatcher.wait_for_event()
    assert redis_client.zcard(event_dispatcher.sorted_set_received_events_in_cache) == 0
    assert redis_client.zcard(event_dispatcher.sorted_set_received_events) == 0
    assert redis_client.hlen(event_dispatcher.hash_received_events) == 0
    assert redis_client.zcard(
            event_dispatcher.sorted_set_event_listeners_to_process +
            ':sequence_video_clips-melt_ended_event') == 0
    assert len(result) is 2
    assert 'upserted' in result[0]
    assert result[0] == {
            'n': 1, 'nModified': 0, 'upserted': '20200503223628.840265', 'ok': 1.0, 'updatedExisting': False
        }
    assert result[1] == ('http://some_url.com/', {
            "return_code": 0,
            "file_path_name": "/video_clips/output-20200503-090308.mp4",
            "transaction_id": "20200503174008.553417"
        }, {})
    assert db_client.received_events.count_documents({}) == 1
    received_events = db_client.received_events.find_one({'_id': "20200503223628.840265"})
    assert received_events is not None
    assert received_events['_id'] == "20200503223628.840265"
    assert received_events['event_id'] == "sequence_video_clips-melt_ended_event"
    assert received_events['transaction_id'] == "20200503174008.553417"
    data = received_events['data']
    assert data is not None
    assert data['return_code'] is 0
    assert data['file_path_name'] == "/video_clips/output-20200503-090308.mp4"
    assert data['transaction_id'] == "20200503174008.553417"
