from common_lib import CommonLib, sorted_set_retro_event_listeners, hash_retro_event_listeners
import conftest
import json


def test_register_for_retro_event_retrieval_with_1_url_to_register_having_attribute_transaction_id(redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event_1'
    url = 'http://some_url_1.com/'
    priority = 100
    transaction_id = '20200518193802.123456'  # YYmmddHHMMSS.nnnnnn datetime format in string
    data = {
        "url": url,
        "transaction_id": transaction_id,
    }
    response = {}
    expected_h_retro_event_listeners = {
        "transaction_id": transaction_id,
        "event_id": event_id,
        "url": url,
    }
    expected_result_data = {"transaction_id": transaction_id}
    expected_response = {"registered_for_retro_event_retrieval": {
        event_id: {
            url: expected_result_data,
        }
    }}

    common_lib = CommonLib()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.register_for_retro_event_retrieval(event_id, data, response)
    assert redis_client.zcard(sorted_set_retro_event_listeners) == 1
    assert redis_client.hlen(hash_retro_event_listeners) == 1
    assert redis_client.zrange(
        sorted_set_retro_event_listeners, 0, -1, withscores=True)[0] == (
               url.encode('UTF-8'),
               priority
           )
    retrieved_h_retro_event_listeners = json.loads(
        redis_client.hmget(hash_retro_event_listeners, url)[0].decode('UTF-8')
    )
    assert retrieved_h_retro_event_listeners is not None
    assert retrieved_h_retro_event_listeners == expected_h_retro_event_listeners
    assert response == expected_response
