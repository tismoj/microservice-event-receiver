from common_lib import CommonLib, sorted_set_events_with_listeners, sorted_set_event_listeners, hash_event_listeners
import conftest
import json


def test_register_to_event_listeners_with_1_url_to_register_having_attributes_transaction_id_and_datetime_start(
        redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event_1'
    url = 'http://some_url_1.com/'
    priority = 100
    datetime_start = '21200518193101.012345'  # YYmmddHHMMSS.nnnnnn datetime format in string
    transaction_id = '20200518193802.123456'  # YYmmddHHMMSS.nnnnnn datetime format in string
    z_url = {url: priority}
    h_url = {url: {
        "datetime_start": datetime_start,
        "transaction_id": transaction_id,
    }}
    response = {}

    common_lib = CommonLib()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.register_to_event_listeners(event_id, z_url, h_url, response)
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
    assert response == {"registered_for_events": {
        event_id: {
            url: expected_data,
        }
    }}
