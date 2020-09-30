import event_dispatcher
import conftest


def test_store_event_to_db(db_client):
    timestamp = "20200503223628.840265"
    event_id = "sequence_video_clips-melt_ended_event"
    transaction_id = "20200503174008.553417"
    event_data = {
            "return_code": 0,
            "file_path_name": "/video_clips/output-20200503-090308.mp4",
            "transaction_id": transaction_id,
        }
    expected_event = {
            "_id": timestamp,
            "event_id": event_id,
            "transaction_id": transaction_id,
            "data": event_data,
        }

    dispatcher = event_dispatcher.EventDispatcher()
    dispatcher.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    dispatcher.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    dispatcher.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    db_client = dispatcher.establish_mongodb_connection()
    common_lib = dispatcher.get_common_lib_instance()
    common_lib.CONFIG['TESTING'] = conftest.CONFIG['TESTING']
    common_lib.CONFIG['MONGODB_HOST'] = conftest.CONFIG['MONGODB_HOST']
    common_lib.CONFIG['MONGODB_DB'] = conftest.CONFIG['MONGODB_DB']
    common_lib.establish_mongodb_connection()
    result = dispatcher.store_event_to_db(timestamp, event_id, transaction_id, event_data)
    assert result == {
            'n': 1, 'nModified': 0, 'upserted': timestamp, 'ok': 1.0, 'updatedExisting': False
        }
    assert db_client.received_events.count_documents({}) == 1
    retrieved_event = db_client.received_events.find_one({'_id': timestamp})
    assert retrieved_event is not None
    assert retrieved_event == expected_event
