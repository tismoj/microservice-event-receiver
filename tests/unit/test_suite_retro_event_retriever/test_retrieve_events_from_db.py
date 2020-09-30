from retro_event_retriever import RetroEventRetriever
import conftest


def test_retrieve_events_from_db_with_a_listener_attribute_of_datetime_start_that_should_retrieve_1_event(standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_1.com/"
    transaction_id = "20200503174008.553417"
    datetime_start = "20200503223628.840265"
    listener_attributes = {
            "event_id": event_id,
            "url": url,
            "datetime_start": datetime_start,
        }

    event_data = {
            "return_code": 0,
            "file_path_name": "/video_clips/output-20200503-090308.mp4",
            "transaction_id": transaction_id,
        }
    expected_events = [{
        "_id": "20200503223628.840266",
        "event_id": event_id,
        "transaction_id": transaction_id,
        "data": event_data,
    }]

    retriever = RetroEventRetriever()
    retriever.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    retrieved_events = retriever.retrieve_events_from_db(listener_attributes)

    print(retrieved_events)
    assert len(retrieved_events) == 1
    assert retrieved_events == expected_events


def test_retrieve_events_from_db_with_a_listener_attribute_of_datetime_start_that_should_retrieve_2_events(standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_1.com/"
    datetime_start = "20200503223628.840254"
    listener_attributes = {
            "event_id": event_id,
            "url": url,
            "datetime_start": datetime_start,
        }

    expected_events = [{
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
        "transaction_id": "20200503174008.553417",
        "data": {
            "return_code": 0,
            "file_path_name": "/video_clips/output-20200503-090308.mp4",
            "transaction_id": "20200503174008.553417",
        },
    }]

    retriever = RetroEventRetriever()
    retriever.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    retrieved_events = retriever.retrieve_events_from_db(listener_attributes)

    print(retrieved_events)
    assert len(retrieved_events) == 2
    assert retrieved_events == expected_events


def test_retrieve_events_from_db_with_a_listener_attribute_of_datetime_end_that_should_retrieve_2_events(standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_1.com/"
    datetime_end = "20200503223628.840254"
    listener_attributes = {
            "event_id": event_id,
            "url": url,
            "datetime_end": datetime_end,
        }

    expected_events = [{
        "_id": "20200503174008.553421",
        "event_id": event_id,
        "transaction_id": "20200503174008.553417",
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
    }]

    retriever = RetroEventRetriever()
    retriever.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    retrieved_events = retriever.retrieve_events_from_db(listener_attributes)

    print(retrieved_events)
    assert len(retrieved_events) == 2
    assert retrieved_events == expected_events


def test_retrieve_events_from_db_with_listener_attributes_of_datetime_start_and_datetime_end_that_should_retrieve_2_events(standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_1.com/"
    datetime_start = "20200503174008.553417"
    datetime_end = "20200503223628.840265"
    listener_attributes = {
            "event_id": event_id,
            "url": url,
            "datetime_start": datetime_start,
            "datetime_end": datetime_end,
        }

    expected_events = [{
        "_id": "20200503174008.553421",
        "event_id": event_id,
        "transaction_id": "20200503174008.553417",
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
    }]

    retriever = RetroEventRetriever()
    retriever.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    retrieved_events = retriever.retrieve_events_from_db(listener_attributes)

    print(retrieved_events)
    assert len(retrieved_events) == 2
    assert retrieved_events == expected_events


def test_retrieve_events_from_db_with_a_listener_attribute_of_transaction_id_that_should_retrieve_2_events(standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_1.com/"
    transaction_id = "20200503174008.553417"
    listener_attributes = {
            "event_id": event_id,
            "url": url,
            "transaction_id": transaction_id,
        }

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
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    retrieved_events = retriever.retrieve_events_from_db(listener_attributes)

    print(retrieved_events)
    assert len(retrieved_events) == 2
    assert retrieved_events == expected_events


def test_retrieve_events_from_db_with_listener_attributes_of_transaction_id_and_datetime_start_that_should_retrieve_1_event(standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_1.com/"
    transaction_id = "20200503174008.553417"
    datetime_start = "20200503223628.840266"
    listener_attributes = {
            "event_id": event_id,
            "url": url,
            "transaction_id": transaction_id,
            "datetime_start": datetime_start,
        }

    expected_events = [{
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
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    retrieved_events = retriever.retrieve_events_from_db(listener_attributes)

    print(retrieved_events)
    assert len(retrieved_events) == 1
    assert retrieved_events == expected_events


def test_retrieve_events_from_db_with_listener_attributes_of_transaction_id_and_datetime_end_that_should_retrieve_1_event(standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_1.com/"
    transaction_id = "20200503174008.553417"
    datetime_end = "20200503174008.553421"
    listener_attributes = {
            "event_id": event_id,
            "url": url,
            "transaction_id": transaction_id,
            "datetime_end": datetime_end,
        }

    expected_events = [{
        "_id": datetime_end,
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
    }]

    retriever = RetroEventRetriever()
    retriever.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    retrieved_events = retriever.retrieve_events_from_db(listener_attributes)

    print(retrieved_events)
    assert len(retrieved_events) == 1
    assert retrieved_events == expected_events


def test_retrieve_events_from_db_with_listener_attributes_of_transaction_id_datetime_start_and_datetime_end_that_should_retrieve_2_events(standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_1.com/"
    transaction_id = "20200503174008.553417"
    datetime_start = "20200503223628.840263"
    datetime_end = "20200503223628.840267"
    listener_attributes = {
            "event_id": event_id,
            "url": url,
            "transaction_id": transaction_id,
            "datetime_start": datetime_start,
            "datetime_end": datetime_end,
        }

    expected_events = [{
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
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    retrieved_events = retriever.retrieve_events_from_db(listener_attributes)

    print(retrieved_events)
    assert len(retrieved_events) == 1
    assert retrieved_events == expected_events


def test_retrieve_events_from_db_with_listener_attributes_of_transaction_id_and_required_data_that_should_retrieve_1_event_with_the_latest_data_from_any_previous_event_with_the_same_transaction_id(standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_2.com/"
    transaction_id = "20200503223628.840250"
    required_data = ["clips", "video_clips", "converted_video_clips", "sequenced_video_clips"]
    listener_attributes = {
            "event_id": event_id,
            "url": url,
            "transaction_id": transaction_id,
            "required_data": required_data,
        }

    expected_events = [{
            "_id": "20200503223628.840254",
            "event_id": "sequence_video_clips-melt_ended_event",
            "transaction_id": "20200503223628.840250",
            "data": {
                "devices": [
                    "OnatMacBookPro"
                ],
                "OnatMacBookPro:file_ids": [
                    "1:c2875f140136afd4534fcd978c82af0b897fe96e"
                ],
                'clips': [{
                        'clip_url': 'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp',
                        'clip_title_text': 'im not gay', 'clip_player_name': 'YTMagMa',
                        'clip_player_url': 'https://www.twitch.tv/ytmagma',
                        'clip_image_url': 'https://clips-media-assets2.twitch.tv/AT-cm%7C766375085-preview-260x147.jpg',
                        'clip_duration': 12, 'clip_view_count': 3000, 'clip_hours_ago': '1 hour ago'
                    },
                ],
                'video_clips': {
                    'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {
                        'file_path_name': '/video_clips/im not gay.mp4', 'return_code': 0, 'std_errors': [],
                        'std_outputs': []
                    }
                },
                'converted_video_clips': {
                    'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {
                        'file_path_name': '/video_clips/im not gay.Vimeo_YouTube_HQ_1080p60.mp4',
                        'return_code': 0, 'std_errors': [], 'std_outputs': []
                    }
                },
                'sequenced_video_clips': {
                    'file_path_name': '/video_clips/output-20200628-084301.mp4', 'return_code': 0, 'std_errors': [],
                    'std_outputs': [],
                    'timecodes': {
                        'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': 0
                    }
                }
            },
        }]

    retriever = RetroEventRetriever()
    retriever.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    common_lib = retriever.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    retrieved_events = retriever.retrieve_events_from_db(listener_attributes)

    print(retrieved_events)
    assert len(retrieved_events) == 1
    assert retrieved_events == expected_events


def test_retrieve_events_from_db_with_a_listener_attribute_of_required_data_looking_for_data_that_does_not_exist_should_retrieve_0_events(standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_2.com/"
    required_data = ["looking_for_some_non_existent_data"]
    listener_attributes = {
            "event_id": event_id,
            "url": url,
            "required_data": required_data,
        }

    expected_events = []

    retriever = RetroEventRetriever()
    retriever.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    common_lib = retriever.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    retrieved_events = retriever.retrieve_events_from_db(listener_attributes)

    print(retrieved_events)
    assert len(retrieved_events) == 0
    assert retrieved_events == expected_events


def test_retrieve_events_from_db_with_listener_attributes_of_datetime_end_and_required_data_that_should_retrieve_2_events_with_the_latest_data_from_any_previous_event_with_the_same_transaction_id(standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_2.com/"
    required_data = ["clips", "video_clips", "converted_video_clips", "sequenced_video_clips"]
    datetime_end = "20200503223628.840265"
    listener_attributes = {
            "event_id": event_id,
            "url": url,
            "required_data": required_data,
            "datetime_end": datetime_end,
        }

    expected_events = [{
            "_id": "20200503174008.553421",
            "event_id": "sequence_video_clips-melt_ended_event",
            "transaction_id": "20200503174008.553417",
            "data": {
                "file_size": 67,
                "devices": [
                    "OnatMacBookPro"
                ],
                "OnatMacBookPro:file_ids": [
                    "1:c2875f140136afd4534fcd978c82af0b897fe96e"
                ],
                'clips': [{
                        'clip_duration': 120, 'clip_hours_ago': '1 hour ago',
                        'clip_image_url': 'https://clips-media-assets2.twitch.tv/AT-cm%7C766375085-preview-260x147.jpg',
                        'clip_player_name': 'YTMagMa', 'clip_player_url': 'https://www.twitch.tv/ytmagma',
                        'clip_title_text': 'im not gay',
                        'clip_url': 'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp',
                        'clip_view_count': 3000
                    }, {
                        'clip_duration': 70, 'clip_hours_ago': '15 hours ago',
                        'clip_image_url': 'https://clips-media-assets2.twitch.tv/AT-cm%7C765635143-preview-260x147.jpg',
                        'clip_player_name': 'benjyfishy', 'clip_player_url': 'https://www.twitch.tv/benjyfishy',
                        'clip_title_text': 'Benjy Music Timing',
                        'clip_url': 'https://www.twitch.tv/benjyfishy/clip/KawaiiDreamyKangarooThisIsSparta',
                        'clip_view_count': 2400
                    }
                ],
                'video_clips': {
                    'https://www.twitch.tv/benjyfishy/clip/KawaiiDreamyKangarooThisIsSparta': {},
                    'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {}
                },
                'converted_video_clips': {
                    'https://www.twitch.tv/benjyfishy/clip/KawaiiDreamyKangarooThisIsSparta': {},
                    'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {}
                },
                'sequenced_video_clips': {
                    'file_path_name': '/video_clips/output-20200628-084301.mp4', 'return_code': 0, 'std_errors': [],
                    'std_outputs': [],
                    'timecodes': {}
                },
            }},
        {
            "_id": "20200503223628.840254",
            "event_id": "sequence_video_clips-melt_ended_event",
            "transaction_id": "20200503223628.840250",
            "data": {
                "devices": [
                    "OnatMacBookPro"
                ],
                "OnatMacBookPro:file_ids": [
                    "1:c2875f140136afd4534fcd978c82af0b897fe96e"
                ],
                'clips': [{
                        'clip_url': 'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp',
                        'clip_title_text': 'im not gay', 'clip_player_name': 'YTMagMa',
                        'clip_player_url': 'https://www.twitch.tv/ytmagma',
                        'clip_image_url': 'https://clips-media-assets2.twitch.tv/AT-cm%7C766375085-preview-260x147.jpg',
                        'clip_duration': 12, 'clip_view_count': 3000, 'clip_hours_ago': '1 hour ago'
                    },
                ],
                'video_clips': {
                    'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {
                        'file_path_name': '/video_clips/im not gay.mp4', 'return_code': 0, 'std_errors': [],
                        'std_outputs': []
                    }
                },
                'converted_video_clips': {
                    'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {
                        'file_path_name': '/video_clips/im not gay.Vimeo_YouTube_HQ_1080p60.mp4',
                        'return_code': 0, 'std_errors': [], 'std_outputs': []
                    }
                },
                'sequenced_video_clips': {
                    'file_path_name': '/video_clips/output-20200628-084301.mp4', 'return_code': 0, 'std_errors': [],
                    'std_outputs': [],
                    'timecodes': {
                        'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': 0
                    }
                }
            },
        }]

    retriever = RetroEventRetriever()
    retriever.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    common_lib = retriever.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    retrieved_events = retriever.retrieve_events_from_db(listener_attributes)

    print(retrieved_events)
    assert len(retrieved_events) == 2
    assert retrieved_events == expected_events


def test_retrieve_events_from_db_with_listener_attributes_of_datetime_start_transaction_id_and_required_data_that_should_retrieve_2_events_with_the_latest_data_from_any_previous_event_with_the_same_transaction_id(standard_db_event_entries):
    """
    GIVEN a StoreAFilesMd5MicroService instance
    WHEN the 'StoreAFilesMd5MicroService.get_device_id(db, device)' is invoked and given a new device as argument
    THEN that new device should be stored in the 'device_ids' db collection and the 'device_id_index' from 'globals' db
    collection should be incremented by 1
    """
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_2.com/"
    transaction_id = "20200503174008.553417"
    datetime_start = transaction_id
    required_data = ["clips", "video_clips", "converted_video_clips", "sequenced_video_clips", "file_size"]
    listener_attributes = {
            "event_id": event_id,
            "url": url,
            "required_data": required_data,
            "transaction_id": transaction_id,
            "datetime_start": datetime_start,
        }

    expected_events = [{
            "_id": "20200503174008.553421",
            "event_id": "sequence_video_clips-melt_ended_event",
            "transaction_id": "20200503174008.553417",
            "data": {
                "file_size": 67,
                "devices": [
                    "OnatMacBookPro"
                ],
                "OnatMacBookPro:file_ids": [
                    "1:c2875f140136afd4534fcd978c82af0b897fe96e"
                ],
                'clips': [{
                        'clip_duration': 120, 'clip_hours_ago': '1 hour ago',
                        'clip_image_url': 'https://clips-media-assets2.twitch.tv/AT-cm%7C766375085-preview-260x147.jpg',
                        'clip_player_name': 'YTMagMa', 'clip_player_url': 'https://www.twitch.tv/ytmagma',
                        'clip_title_text': 'im not gay',
                        'clip_url': 'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp',
                        'clip_view_count': 3000
                    }, {
                        'clip_duration': 70, 'clip_hours_ago': '15 hours ago',
                        'clip_image_url': 'https://clips-media-assets2.twitch.tv/AT-cm%7C765635143-preview-260x147.jpg',
                        'clip_player_name': 'benjyfishy', 'clip_player_url': 'https://www.twitch.tv/benjyfishy',
                        'clip_title_text': 'Benjy Music Timing',
                        'clip_url': 'https://www.twitch.tv/benjyfishy/clip/KawaiiDreamyKangarooThisIsSparta',
                        'clip_view_count': 2400
                    }
                ],
                'video_clips': {
                    'https://www.twitch.tv/benjyfishy/clip/KawaiiDreamyKangarooThisIsSparta': {},
                    'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {}
                },
                'converted_video_clips': {
                    'https://www.twitch.tv/benjyfishy/clip/KawaiiDreamyKangarooThisIsSparta': {},
                    'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {}
                },
                'sequenced_video_clips': {
                    'file_path_name': '/video_clips/output-20200628-084301.mp4', 'return_code': 0, 'std_errors': [],
                    'std_outputs': [],
                    'timecodes': {}
                },
            }},
        {
            "_id": "20200503223628.840266",
            "event_id": "sequence_video_clips-melt_ended_event",
            "transaction_id": "20200503174008.553417",
            "data": {
                "return_code": 0,
                "file_path_name": "/video_clips/output-20200503-090308.mp4",
                "transaction_id": "20200503174008.553417",
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
            }},
    ]

    retriever = RetroEventRetriever()
    retriever.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    retriever.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    retriever.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    retriever.establish_mongodb_connection()
    common_lib = retriever.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    retrieved_events = retriever.retrieve_events_from_db(listener_attributes)

    print(retrieved_events)
    assert len(retrieved_events) == 2
    assert retrieved_events == expected_events
