from common_lib import CommonLib


def test_get_priority_if_present_with_data_having_attribute_priority():
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    priority = 20
    data = {"priority": priority}

    common_lib = CommonLib()
    assert common_lib.get_priority_if_present(data) == 20


def test_get_priority_if_present_with_data_having_no_attribute_priority():
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    data = {}

    common_lib = CommonLib()
    assert common_lib.get_priority_if_present(data) == 100
