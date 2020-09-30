from common_lib import CommonLib
from datetime import datetime


def test_prepare_to_register_for_event_listeners_with_data_having_attribute_count():
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    url = 'http://some_url_1.com/'
    priority = 100
    count = 5
    z_urls = {}
    h_urls = {}
    data = {
            "url": url,
            "count": count,
        }

    common_lib = CommonLib()
    common_lib.prepare_to_register_for_event_listeners(data, z_urls, h_urls)
    assert z_urls[url] == priority
    assert h_urls[url] == {"count": count}


def test_prepare_to_register_for_event_listeners_with_data_having_attribute_expire_after_set_to_5_seconds():
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    url = 'http://some_url_2.com/'
    priority = 100
    expire_after = 1.0 * 5  # expire after 5 secs
    z_urls = {}
    h_urls = {}

    common_lib = CommonLib()
    common_lib.prepare_to_register_for_event_listeners({"url": url, "expire_after": 0}, z_urls, h_urls)
    current_time = h_urls[url]["datetime_end"]
    assert z_urls[url] == priority
    assert h_urls[url] == {"datetime_end": current_time}
    common_lib.prepare_to_register_for_event_listeners({"url": url, "expire_after": expire_after}, z_urls, h_urls)
    datetime_end_after_5_sec = h_urls[url]["datetime_end"]
    assert z_urls[url] == priority
    assert h_urls[url] == {"datetime_end": datetime_end_after_5_sec}
    assert (datetime.strptime(datetime_end_after_5_sec, "%Y%m%d%H%M%S.%f")
            - datetime.strptime(current_time, "%Y%m%d%H%M%S.%f")).total_seconds() >= 5.0
    assert (datetime.strptime(datetime_end_after_5_sec, "%Y%m%d%H%M%S.%f")
            - datetime.strptime(current_time, "%Y%m%d%H%M%S.%f")).total_seconds() < 6.0


def test_prepare_to_register_for_event_listeners_with_data_having_attributes_datetime_start_and_priority():
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    url = 'http://some_url_3.com/'
    priority = 20
    datetime_start = '21200518193101.012345'  # YYmmddHHMMSS.nnnnnn datetime format in string
    z_urls = {}
    h_urls = {}
    data = {
            "url": url,
            "datetime_start": datetime_start,
            "priority": priority,
        }

    common_lib = CommonLib()
    common_lib.prepare_to_register_for_event_listeners(data, z_urls, h_urls)
    assert z_urls[url] == priority
    assert h_urls[url] == {"datetime_start": datetime_start}


def test_prepare_to_register_for_event_listeners_with_data_having_attribute_datetime_end():
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    url = 'http://some_url_4.com/'
    priority = 100
    datetime_end = '21200518193101.012345'  # YYmmddHHMMSS.nnnnnn datetime format in string
    z_urls = {}
    h_urls = {}
    data = {
            "url": url,
            "datetime_end": datetime_end,
        }

    common_lib = CommonLib()
    common_lib.prepare_to_register_for_event_listeners(data, z_urls, h_urls)
    assert z_urls[url] == priority
    assert h_urls[url] == {"datetime_end": datetime_end}


def test_prepare_to_register_for_event_listeners_with_data_having_attribute_transaction_id():
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    url = 'http://some_url_5.com/'
    priority = 100
    transaction_id = '20200518193802.123456'  # YYmmddHHMMSS.nnnnnn datetime format in string
    z_urls = {}
    h_urls = {}
    data = {
            "url": url,
            "transaction_id": transaction_id,
        }

    common_lib = CommonLib()
    common_lib.prepare_to_register_for_event_listeners(data, z_urls, h_urls)
    assert z_urls[url] == priority
    assert h_urls[url] == {"transaction_id": transaction_id}


def test_prepare_to_register_for_event_listeners_with_data_having_attribute_listener_data():
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    url = 'http://some_url_6.com/'
    priority = 100
    listener_data = {"some_key": "some_value"}  # Dictionary data meant for the listener that is set only during registration
    z_urls = {}
    h_urls = {}
    data = {
            "url": url,
            "listener_data": listener_data,
        }

    common_lib = CommonLib()
    common_lib.prepare_to_register_for_event_listeners(data, z_urls, h_urls)
    assert z_urls[url] == priority
    assert h_urls[url] == {"listener_data": listener_data}


def test_prepare_to_register_for_event_listeners_with_data_having_attribute_required_data():
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    url = 'http://some_url_7.com/'
    priority = 100
    required_data = ["data_key_1", "data_key_2"]  # Dictionary data meant for the listener that is set only during registration
    z_urls = {}
    h_urls = {}
    data = {
            "url": url,
            "required_data": required_data,
        }

    common_lib = CommonLib()
    common_lib.prepare_to_register_for_event_listeners(data, z_urls, h_urls)
    assert z_urls[url] == priority
    assert h_urls[url] == {"required_data": required_data}
