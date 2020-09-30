from common_lib import CommonLib
import conftest


def test_reduce_count_if_attribute_is_present_without_any_registered_event_listeners(redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    common_lib = CommonLib()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    result = common_lib.reduce_count_if_attribute_is_present(
            "sequence_video_clips-melt_ended_event", "http://some_url.com/"
        )
    assert len(result) is 0


def test_reduce_count_if_attribute_is_present_with_a_registered_event_listener_without_an_attribute_count(standard_redis_event_listener_entry):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    common_lib = CommonLib()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    result = common_lib.reduce_count_if_attribute_is_present(
            "sequence_video_clips-melt_ended_event", "http://some_url.com/"
        )
    assert len(result) is 0


def test_reduce_count_if_attribute_is_present_with_a_registered_event_listener_having_a_count_of_2(redis_event_listener_entry_with_a_count_of_2):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    common_lib = CommonLib()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    result = common_lib.reduce_count_if_attribute_is_present(
            "sequence_video_clips-melt_ended_event", "http://some_url.com/"
        )
    assert len(result) is 1
    assert result == [[True]]
    result = common_lib.reduce_count_if_attribute_is_present(
            "sequence_video_clips-melt_ended_event", "http://some_url.com/"
        )
    assert len(result) is 1
    assert result == [[True]]
    result = common_lib.reduce_count_if_attribute_is_present(
            "sequence_video_clips-melt_ended_event", "http://some_url.com/"
        )
    assert len(result) is 0
