from common_lib import sorted_set_retro_event_listeners, hash_retro_event_listeners
from event_dispatcher import EventDispatcher
import conftest
import json


def test_dispatch_event_to_listeners_without_any_registered_event_listeners(redis_client):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    dispatcher = EventDispatcher()
    dispatcher.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    dispatcher.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    dispatcher.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    dispatcher.establish_redis_connection()
    dispatcher.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    dispatcher.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    dispatcher.establish_mongodb_connection()
    common_lib = dispatcher.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    result = dispatcher.dispatch_event_to_listeners("sequence_video_clips-melt_ended_event", "20200503223628.840265", {
        "return_code": 0,
        "file_path_name": "/video_clips/output-20200503-090308.mp4",
        "transaction_id": "20200503174008.553417",
    })
    assert len(result) is 0


def test_dispatch_event_to_listeners_with_a_registered_event_listener(standard_redis_event_listener_entry):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    return_code = 0
    file_path_name = "/video_clips/output-20200503-090308.mp4"
    transaction_id = "20200503174008.553417"

    dispatcher = EventDispatcher()
    dispatcher.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    dispatcher.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    dispatcher.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    dispatcher.establish_redis_connection()
    dispatcher.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    dispatcher.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    dispatcher.establish_mongodb_connection()
    common_lib = dispatcher.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    result = dispatcher.dispatch_event_to_listeners("sequence_video_clips-melt_ended_event", "20200503223628.840265", {
        "return_code": return_code,
        "file_path_name": file_path_name,
        "transaction_id": transaction_id,
    })
    assert len(result) is 1
    assert result[0] == ('http://some_url.com/', {
        "return_code": return_code,
        "file_path_name": file_path_name,
        "transaction_id": transaction_id
    }, {})


def test_dispatch_event_to_listeners_with_a_registered_event_listener_having_a_count_of_2(
        redis_event_listener_entry_with_a_count_of_2):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = "sequence_video_clips-melt_ended_event"
    timestamp = "20200503223628.840265"
    url = 'http://some_url.com/'
    return_code = 0
    transaction_id = "20200503174008.553417"

    dispatcher = EventDispatcher()
    dispatcher.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    dispatcher.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    dispatcher.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    dispatcher.establish_redis_connection()
    dispatcher.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    dispatcher.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    dispatcher.establish_mongodb_connection()
    common_lib = dispatcher.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    result = dispatcher.dispatch_event_to_listeners(event_id, timestamp, {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308a.mp4",
        "transaction_id": transaction_id,
    })
    assert len(result) is 1
    assert result[0] == (url, {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308a.mp4",
        "transaction_id": transaction_id
    }, {
                             "count": 1,
                         })

    result = dispatcher.dispatch_event_to_listeners(event_id, timestamp, {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308b.mp4",
        "transaction_id": transaction_id,
    })
    assert len(result) is 1
    assert result[0] == (url, {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308b.mp4",
        "transaction_id": transaction_id
    }, {
                             "count": 0,
                         })

    result = dispatcher.dispatch_event_to_listeners(event_id, timestamp, {
        "return_code": 0,
        "file_path_name": "/video_clips/output-20200503-090308c.mp4",
        "transaction_id": transaction_id,
    })
    assert len(result) is 0


def test_dispatch_event_to_listeners_with_a_registered_event_listener_having_a_datetime_end_set_in_the_past(
        redis_event_listener_entry_with_a_datetime_end_set_in_the_past):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    dispatcher = EventDispatcher()
    dispatcher.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    dispatcher.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    dispatcher.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    dispatcher.establish_redis_connection()
    dispatcher.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    dispatcher.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    dispatcher.establish_mongodb_connection()
    common_lib = dispatcher.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    result = dispatcher.dispatch_event_to_listeners("sequence_video_clips-melt_ended_event", "20200503223628.840265", {
        "return_code": 0,
        "file_path_name": "/video_clips/output-20200503-090308a.mp4",
        "transaction_id": "20200503174008.553417",
    })
    assert len(result) is 0


def test_dispatch_event_to_listeners_with_a_registered_event_listener_having_a_datetime_end_set_into_the_future(
        redis_event_listener_entry_with_a_datetime_end_set_into_the_future):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    return_code = 0
    transaction_id = "20200503174008.553417"
    datetime_end = "21200503174008.553417"

    dispatcher = EventDispatcher()
    dispatcher.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    dispatcher.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    dispatcher.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    dispatcher.establish_redis_connection()
    dispatcher.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    dispatcher.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    dispatcher.establish_mongodb_connection()
    common_lib = dispatcher.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    result = dispatcher.dispatch_event_to_listeners("sequence_video_clips-melt_ended_event", "20200503223628.840265", {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308a.mp4",
        "transaction_id": transaction_id,
    })
    assert len(result) is 1
    assert result[0] == ('http://some_url.com/', {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308a.mp4",
        "transaction_id": transaction_id,
    }, {
                             "datetime_end": datetime_end,
                         })

    result = dispatcher.dispatch_event_to_listeners("sequence_video_clips-melt_ended_event", datetime_end, {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308b.mp4",
        "transaction_id": transaction_id,
    })
    assert len(result) is 1
    assert result[0] == ('http://some_url.com/', {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308b.mp4",
        "transaction_id": transaction_id,
    }, {
                             "datetime_end": datetime_end,
                         })


def test_dispatch_event_to_listeners_with_a_registered_event_listener_having_a_datetime_start_set_into_the_future(
        redis_event_listener_entry_with_a_datetime_start_set_into_the_future):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = "sequence_video_clips-melt_ended_event"
    return_code = 0
    transaction_id = "20200503174008.553417"
    datetime_start = "21200503174008.553417"

    dispatcher = EventDispatcher()
    dispatcher.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    dispatcher.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    dispatcher.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    dispatcher.establish_redis_connection()
    dispatcher.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    dispatcher.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    dispatcher.establish_mongodb_connection()
    common_lib = dispatcher.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    result = dispatcher.dispatch_event_to_listeners(event_id, "20200503223628.840265", {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308a.mp4",
        "transaction_id": transaction_id,
    })
    assert len(result) is 0

    result = dispatcher.dispatch_event_to_listeners(event_id, datetime_start, {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308b.mp4",
        "transaction_id": transaction_id,
    })
    assert len(result) is 1
    assert result[0] == ('http://some_url.com/', {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308b.mp4",
        "transaction_id": transaction_id,
    }, {
                             "datetime_start": datetime_start,
                         })


def test_dispatch_event_to_listeners_with_a_registered_event_listener_having_a_datetime_start_set_in_the_past(
        redis_event_listener_entry_with_a_datetime_start_set_in_the_past):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = "sequence_video_clips-melt_ended_event"
    return_code = 0
    transaction_id = "20200503174008.553417"
    url = 'http://some_url.com/'
    datetime_start = "20200503223628.840265"
    timestamp_earlier_than_datetime_start = transaction_id
    timestamp_much_later_than_datetime_start = "21200503174008.553417"

    dispatcher = EventDispatcher()
    dispatcher.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    dispatcher.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    dispatcher.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    dispatcher.establish_redis_connection()
    dispatcher.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    dispatcher.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    dispatcher.establish_mongodb_connection()
    common_lib = dispatcher.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    result = dispatcher.dispatch_event_to_listeners(event_id, timestamp_earlier_than_datetime_start, {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308a.mp4",
        "transaction_id": transaction_id,
    })
    assert len(result) is 0

    result = dispatcher.dispatch_event_to_listeners(event_id, datetime_start, {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308b.mp4",
        "transaction_id": transaction_id,
    })
    assert len(result) is 1
    assert result[0] == (url, {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308b.mp4",
        "transaction_id": transaction_id,
    }, {
                             "datetime_start": datetime_start,
                         })

    result = dispatcher.dispatch_event_to_listeners(event_id, timestamp_much_later_than_datetime_start, {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308c.mp4",
        "transaction_id": transaction_id,
    })
    assert len(result) is 1
    assert result[0] == (url, {
        "return_code": return_code,
        "file_path_name": "/video_clips/output-20200503-090308c.mp4",
        "transaction_id": transaction_id,
    }, {
                             "datetime_start": datetime_start,
                         })


def test_dispatch_event_to_listeners_with_a_registered_event_listener_having_several_required_data_that_exists_in_recent_related_events(
        redis_event_listener_entry_with_several_required_data_that_exists_in_recent_related_events,
        standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = "sequence_video_clips-melt_ended_event"
    return_code = 0
    file_path_name = "/video_clips/output-20200503-090308.mp4"
    transaction_id = "20200503174008.553417"
    url = 'http://some_url.com/'
    timestamp = "20200503223628.840266"
    required_data = ["clips", "video_clips", "converted_video_clips", "sequenced_video_clips", "file_size"]

    dispatcher = EventDispatcher()
    dispatcher.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    dispatcher.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    dispatcher.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    dispatcher.establish_redis_connection()
    dispatcher.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    dispatcher.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    dispatcher.establish_mongodb_connection()
    common_lib = dispatcher.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    result = dispatcher.dispatch_event_to_listeners(event_id, timestamp, {
        "return_code": return_code,
        "file_path_name": file_path_name,
        "transaction_id": transaction_id,
    })
    assert len(result) is 1
    assert result[0] == (url, {
        "return_code": return_code,
        "file_path_name": file_path_name,
        "transaction_id": transaction_id,
        'clips': [{
            'clip_url': 'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp',
            'clip_title_text': 'im not gay', 'clip_player_name': 'YTMagMa',
            'clip_player_url': 'https://www.twitch.tv/ytmagma',
            'clip_image_url': 'https://clips-media-assets2.twitch.tv/AT-cm%7C766375085-preview-260x147.jpg',
            'clip_duration': 12, 'clip_view_count': 3000, 'clip_hours_ago': '1 hour ago'
        }, {
            'clip_url': 'https://www.twitch.tv/benjyfishy/clip/KawaiiDreamyKangarooThisIsSparta',
            'clip_title_text': 'Benjy Music Timing', 'clip_player_name': 'benjyfishy',
            'clip_player_url': 'https://www.twitch.tv/benjyfishy',
            'clip_image_url': 'https://clips-media-assets2.twitch.tv/AT-cm%7C765635143-preview-260x147.jpg',
            'clip_duration': 7, 'clip_view_count': 2400, 'clip_hours_ago': '15 hours ago'
        }
        ],
        'video_clips': {
            'https://www.twitch.tv/benjyfishy/clip/KawaiiDreamyKangarooThisIsSparta': {
                'file_path_name': '/video_clips/Benjy Music Timing.mp4', 'return_code': 0, 'std_errors': [],
                'std_outputs': []
            },
            'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {
                'file_path_name': '/video_clips/im not gay.mp4', 'return_code': 0, 'std_errors': [],
                'std_outputs': []
            }
        },
        'converted_video_clips': {
            'https://www.twitch.tv/benjyfishy/clip/KawaiiDreamyKangarooThisIsSparta': {
                'file_path_name': '/video_clips/Benjy Music Timing.Vimeo_YouTube_HQ_1080p60.mp4',
                'return_code': 0, 'std_errors': [], 'std_outputs': []
            },
            'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {
                'file_path_name': '/video_clips/im not gay.Vimeo_YouTube_HQ_1080p60.mp4',
                'return_code': 0, 'std_errors': [], 'std_outputs': []
            }
        },
        'sequenced_video_clips': {
            'file_path_name': '/video_clips/output-20200628-084301.mp4', 'return_code': 0, 'std_errors': [],
            'std_outputs': [],
            'timecodes': {
                'https://www.twitch.tv/benjyfishy/clip/KawaiiDreamyKangarooThisIsSparta': 12,
                'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': 0
            }
        },
        "file_size": 68,
    }, {
                             "required_data": required_data,
                         })


def test_dispatch_event_to_listeners_with_a_registered_event_listener_having_several_required_data_but_1_that_does_not_exist_in_any_recent_related_events(
        redis_event_listener_entry_with_several_required_data_but_1_that_does_not_exist_in_any_recent_related_events,
        standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = "sequence_video_clips-melt_ended_event"
    return_code = 0
    file_path_name = "/video_clips/output-20200503-090308.mp4"
    transaction_id = "20200503174008.553417"
    timestamp = "20200503223628.840266"

    dispatcher = EventDispatcher()
    dispatcher.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    dispatcher.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    dispatcher.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    dispatcher.establish_redis_connection()
    dispatcher.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    dispatcher.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    dispatcher.establish_mongodb_connection()
    common_lib = dispatcher.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    result = dispatcher.dispatch_event_to_listeners(event_id, timestamp, {
        "return_code": return_code,
        "file_path_name": file_path_name,
        "transaction_id": transaction_id,
    })
    assert len(result) is 0


def test_dispatch_event_to_listeners_with_a_registered_event_listener_that_is_offline(
        mocker,
        standard_redis_event_listener_entry):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    timestamp = "20200503223628.840265"
    return_code = 0
    file_path_name = "/video_clips/output-20200503-090308.mp4"
    transaction_id = "20200503174008.553417"
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url.com/"
    priority = 100
    event_data = {
        "return_code": return_code,
        "file_path_name": file_path_name,
        "transaction_id": transaction_id,
    }
    expected_listener_attributes = {
        "url": url,
        "datetime_start": timestamp,
        "event_id": event_id,
    }

    dispatcher = EventDispatcher()
    dispatcher.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    dispatcher.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    dispatcher.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    redis_client = dispatcher.establish_redis_connection()
    dispatcher.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    dispatcher.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    dispatcher.establish_mongodb_connection()
    common_lib = dispatcher.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['REDIS_HOST'] = conftest.CONFIG['REDIS_HOST']
    common_lib.CONFIG['REDIS_DB'] = conftest.CONFIG['REDIS_DB']
    common_lib.establish_redis_connection()
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    send_event_to_listener = mocker.patch.object(common_lib, 'send_event_to_listener')
    send_event_to_listener.return_value = False
    dispatcher.CONFIG['MOCKED'] = True
    result = dispatcher.dispatch_event_to_listeners(event_id, timestamp, event_data)
    dispatcher.CONFIG['MOCKED'] = False
    assert len(result) is 1
    assert result[0] == (url, event_data, {})
    send_event_to_listener.assert_called_with(url, event_data)
    assert redis_client.zcard(sorted_set_retro_event_listeners) == 1
    assert redis_client.zrange(
        sorted_set_retro_event_listeners, 0, -1, withscores=True)[0] == (
               url.encode('UTF-8'),  # b'http://some_url.com/'
               priority
           )
    listener_attributes = json.loads(
        redis_client.hmget(
            hash_retro_event_listeners, url)[0].decode('UTF-8')
    )
    assert listener_attributes is not None
    assert listener_attributes == expected_listener_attributes
