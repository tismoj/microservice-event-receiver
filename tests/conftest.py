# noinspection PyCompatibility
from urllib.parse import quote_plus
from redis import Redis
from api import api
from common_lib import sorted_set_events_with_listeners, sorted_set_event_listeners, hash_event_listeners, \
    sorted_set_retro_event_listeners, hash_retro_event_listeners
import event_dispatcher
import retro_event_retriever
import pymongo
import pytest
import json

CONFIG = {
    'MONGODB_HOST': "localhost",
    "MONGODB_DB": "microservice_test",
    "MONGODB_USER": "root",
    "MONGODB_PW": "example",
    'REDIS_HOST': "localhost",
    'REDIS_DB': 1,
    'TESTING': True,
}


@pytest.fixture(scope='module')
def api_client():
    client = api.app.test_client()
    # Enable the TESTING flag to disable the error catching during request handling
    # so that you get better error reports when performing test requests against the application.
    api.app.config['TESTING'] = True
    api.CONFIG = {'AMQP_URI': "amqp://guest:guest@localhost"}

    # Establish an application context before running the tests.
    app_context = api.app.app_context()
    app_context.push()

    yield client

    app_context.pop()


@pytest.fixture(scope='function')
def db_client():
    uri = "mongodb://%s:%s@%s" % (
        quote_plus(CONFIG['MONGODB_USER']), quote_plus(CONFIG['MONGODB_PW']), quote_plus(CONFIG['MONGODB_HOST']))
    client = pymongo.MongoClient(uri)
    db = client[CONFIG['MONGODB_DB']]
    print()
    db.globals.drop()
    print('Dropped globals db collection')
    db.received_events.drop()
    print('Dropped received_events db collection')
    return db


@pytest.fixture(scope='function')
def redis_client():
    redis = Redis(host=CONFIG['REDIS_HOST'], db=CONFIG['REDIS_DB'], socket_connect_timeout=2, socket_timeout=2)
    print()
    redis.delete(event_dispatcher.sorted_set_received_events)
    print('Deleted ' + event_dispatcher.sorted_set_received_events)
    redis.delete(event_dispatcher.hash_received_events)
    print('Deleted ' + event_dispatcher.hash_received_events)
    redis.delete(event_dispatcher.sorted_set_received_events_in_cache)
    print('Deleted ' + event_dispatcher.sorted_set_received_events_in_cache)
    while redis.zcard(sorted_set_events_with_listeners):
        event_id = redis.zpopmin(sorted_set_events_with_listeners)[0][0].decode('UTF-8')
        redis.delete(sorted_set_event_listeners + ':' + event_id)
        print('Deleted ' + sorted_set_event_listeners + ':' + event_id)
        redis.delete(hash_event_listeners + ':' + event_id)
        print('Deleted ' + hash_event_listeners + ':' + event_id)
        redis.delete(event_dispatcher.sorted_set_event_listeners_to_process + ':' + event_id)
        print('Deleted ' + event_dispatcher.sorted_set_event_listeners_to_process + ':' + event_id)
    redis.delete(sorted_set_events_with_listeners)
    print('Deleted ' + sorted_set_events_with_listeners)
    redis.delete(sorted_set_retro_event_listeners)
    print('Deleted ' + sorted_set_retro_event_listeners)
    redis.delete(hash_retro_event_listeners)
    print('Deleted ' + hash_retro_event_listeners)
    redis.delete(retro_event_retriever.sorted_set_retro_event_listeners_in_cache)
    print('Deleted ' + retro_event_retriever.sorted_set_retro_event_listeners_in_cache)
    return redis


@pytest.fixture(scope='function')
def standard_redis_received_event_entry(redis_client):
    event_id_with_timestamp = "sequence_video_clips-melt_ended_event:20200503223628.840265"
    transaction_id = "20200503174008.553417"

    redis_client.zadd(event_dispatcher.sorted_set_received_events, {
            event_id_with_timestamp: float(transaction_id)
        })
    data = {
        event_id_with_timestamp:
            json.dumps({"return_code": 0, "file_path_name": "/video_clips/output-20200503-090308.mp4",
                        "transaction_id": transaction_id})
    }
    redis_client.hmset(event_dispatcher.hash_received_events, data)
    assert redis_client.zcard(event_dispatcher.sorted_set_received_events) == 1
    assert redis_client.hlen(event_dispatcher.hash_received_events) == 1
    assert redis_client.zcard(event_dispatcher.sorted_set_received_events_in_cache) == 0
    assert redis_client.zrange(event_dispatcher.sorted_set_received_events, 0, -1, withscores=True)[0] == (
            event_id_with_timestamp.encode('UTF-8'),  # b'sequence_video_clips-melt_ended_event:20200503223628.840265'
            20200503174008.555
        )
    retrieved_data = json.loads(redis_client.hmget(event_dispatcher.hash_received_events, event_id_with_timestamp)[0].decode('UTF-8'))
    assert retrieved_data == json.loads(data[event_id_with_timestamp])


@pytest.fixture(scope='function')
def standard_redis_event_listener_entry(redis_client):
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url.com/"
    priority = 100

    redis_client.zadd(
            sorted_set_events_with_listeners,
            {event_id: 1},
            nx=False,  # nx forces ZADD to only create new elements and not to update scores for elements that already exist.
            incr=True  # modifies ZADD to behave like ZINCRBY.
        )
    redis_client.zadd(
            sorted_set_event_listeners + ':' + event_id, {
                url: priority
            })
    redis_client.hmset(
            hash_event_listeners + ':' + event_id, {
                url: json.dumps({
                })
            })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
            sorted_set_event_listeners +
            ':' + event_id, 0, -1, withscores=True)[0] == (
            url.encode('UTF-8'),  # b'http://some_url.com/'
            priority
        )
    data = json.loads(redis_client.hmget(
            hash_event_listeners + ':' + event_id, url)[0].decode('UTF-8')
        )
    assert data is not None
    assert data == {}


@pytest.fixture(scope='function')
def redis_event_listener_entry_with_a_count_of_2(redis_client):
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url.com/"
    priority = 100
    count = 2

    redis_client.zadd(
            sorted_set_events_with_listeners,
            {event_id: 1},
            nx=False,  # nx forces ZADD to only create new elements and not to update scores for elements that already exist.
            incr=True  # modifies ZADD to behave like ZINCRBY.
        )
    redis_client.zadd(
            sorted_set_event_listeners + ':' + event_id, {
                url: priority
            })
    redis_client.hmset(
            hash_event_listeners + ':' + event_id, {
                url: json.dumps({
                    "count": count,
                })
            })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
            sorted_set_event_listeners +
            ':' + event_id, 0, -1, withscores=True)[0] == (
            url.encode('UTF-8'),  # b'http://some_url.com/'
            priority
        )
    data = json.loads(redis_client.hmget(
            hash_event_listeners + ':' + event_id, url)[0].decode('UTF-8')
        )
    assert data is not None
    assert data == {
            "count": count,
        }


@pytest.fixture(scope='function')
def redis_event_listener_entry_with_a_datetime_end_set_in_the_past(redis_client):
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url.com/"
    priority = 100
    datetime_end = "20200503174008.553417"

    redis_client.zadd(
            sorted_set_events_with_listeners,
            {event_id: 1},
            nx=False,  # nx forces ZADD to only create new elements and not to update scores for elements that already exist.
            incr=True  # modifies ZADD to behave like ZINCRBY.
        )
    redis_client.zadd(
            sorted_set_event_listeners + ':' + event_id, {
                url: priority
            })
    redis_client.hmset(
            hash_event_listeners + ':' + event_id, {
                url: json.dumps({
                    "datetime_end": datetime_end,
                })
            })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
            sorted_set_event_listeners +
            ':' + event_id, 0, -1, withscores=True)[0] == (
            url.encode('UTF-8'),  # b'http://some_url.com/'
            priority
        )
    data = json.loads(redis_client.hmget(
            hash_event_listeners + ':' + event_id, url)[0].decode('UTF-8')
        )
    assert data is not None
    assert data == {
            "datetime_end": datetime_end,
        }


@pytest.fixture(scope='function')
def redis_event_listener_entry_with_a_datetime_end_set_into_the_future(redis_client):
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url.com/"
    priority = 100
    datetime_end = "21200503174008.553417"

    redis_client.zadd(
            sorted_set_events_with_listeners,
            {event_id: 1},
            nx=False,  # nx forces ZADD to only create new elements and not to update scores for elements that already exist.
            incr=True  # modifies ZADD to behave like ZINCRBY.
        )
    redis_client.zadd(
            sorted_set_event_listeners + ':' + event_id, {
                url: priority
            })
    redis_client.hmset(
            hash_event_listeners + ':' + event_id, {
                url: json.dumps({
                    "datetime_end": datetime_end,
                })
            })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
            sorted_set_event_listeners +
            ':' + event_id, 0, -1, withscores=True)[0] == (
            url.encode('UTF-8'),  # b'http://some_url.com/'
            priority
        )
    data = json.loads(redis_client.hmget(
            hash_event_listeners + ':' + event_id, url)[0].decode('UTF-8')
        )
    assert data is not None
    assert data == {
            "datetime_end": datetime_end,
        }


@pytest.fixture(scope='function')
def redis_event_listener_entry_with_a_datetime_start_set_into_the_future(redis_client):
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url.com/"
    priority = 100
    datetime_start = "21200503174008.553417"

    redis_client.zadd(
            sorted_set_events_with_listeners,
            {event_id: 1},
            nx=False,  # nx forces ZADD to only create new elements and not to update scores for elements that already exist.
            incr=True  # modifies ZADD to behave like ZINCRBY.
        )
    redis_client.zadd(sorted_set_event_listeners + ':' + event_id, {
                url: priority
            })
    redis_client.hmset(hash_event_listeners + ':' + event_id, {
                url: json.dumps({
                    "datetime_start": datetime_start,
                })
            })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
            sorted_set_event_listeners +
            ':' + event_id, 0, -1, withscores=True)[0] == (
            url.encode('UTF-8'),  # b'http://some_url.com/'
            priority
        )
    data = json.loads(redis_client.hmget(
            hash_event_listeners + ':' + event_id, url)[0].decode('UTF-8')
        )
    assert data is not None
    assert data == {
            "datetime_start": datetime_start,
        }


@pytest.fixture(scope='function')
def redis_event_listener_entry_with_a_datetime_start_set_in_the_past(redis_client):
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url.com/"
    priority = 100
    datetime_start = "20200503223628.840265"

    redis_client.zadd(
            sorted_set_events_with_listeners,
            {event_id: 1},
            nx=False,  # nx forces ZADD to only create new elements and not to update scores for elements that already exist.
            incr=True  # modifies ZADD to behave like ZINCRBY.
        )
    redis_client.zadd(sorted_set_event_listeners + ':' + event_id, {
                url: priority
            })
    redis_client.hmset(hash_event_listeners + ':' + event_id, {
                url: json.dumps({
                    "datetime_start": datetime_start,
                })
            })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
            sorted_set_event_listeners +
            ':' + event_id, 0, -1, withscores=True)[0] == (
            url.encode('UTF-8'),  # b'http://some_url.com/'
            priority
        )
    data = json.loads(redis_client.hmget(
            hash_event_listeners + ':' + event_id, url)[0].decode('UTF-8')
        )
    assert data is not None
    assert data == {
            "datetime_start": datetime_start,
        }


@pytest.fixture(scope='function')
def redis_event_listener_entry_with_several_required_data_that_exists_in_recent_related_events(redis_client):
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url.com/"
    priority = 100
    required_data = ["clips", "video_clips", "converted_video_clips", "sequenced_video_clips", "file_size"]

    redis_client.zadd(
            sorted_set_events_with_listeners,
            {event_id: 1},
            nx=False,  # nx forces ZADD to only create new elements and not to update scores for elements that already exist.
            incr=True  # modifies ZADD to behave like ZINCRBY.
        )
    redis_client.zadd(sorted_set_event_listeners + ':' + event_id, {
                url: priority
            })
    redis_client.hmset(hash_event_listeners + ':' + event_id, {
                url: json.dumps({
                    "required_data": required_data,
                })
            })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
            sorted_set_event_listeners +
            ':' + event_id, 0, -1, withscores=True)[0] == (
            url.encode('UTF-8'),  # b'http://some_url.com/'
            priority
        )
    data = json.loads(redis_client.hmget(
            hash_event_listeners + ':' + event_id, url)[0].decode('UTF-8')
        )
    assert data is not None
    assert data == {
            "required_data": required_data,
        }


@pytest.fixture(scope='function')
def redis_event_listener_entry_with_several_required_data_but_1_that_does_not_exist_in_any_recent_related_events(redis_client):
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url.com/"
    priority = 100
    required_data = ["clips", "video_clips", "converted_video_clips", "sequenced_video_clips", "file_size", "some_non_existing_data"]

    redis_client.zadd(
            sorted_set_events_with_listeners,
            {event_id: 1},
            nx=False,  # nx forces ZADD to only create new elements and not to update scores for elements that already exist.
            incr=True  # modifies ZADD to behave like ZINCRBY.
        )
    redis_client.zadd(sorted_set_event_listeners + ':' + event_id, {
                url: priority
            })
    redis_client.hmset(hash_event_listeners + ':' + event_id, {
                url: json.dumps({
                    "required_data": required_data,
                })
            })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 1
    assert redis_client.zcard(sorted_set_event_listeners + ':' + event_id) == 1
    assert redis_client.hlen(hash_event_listeners + ':' + event_id) == 1
    assert redis_client.zrange(
            sorted_set_event_listeners +
            ':' + event_id, 0, -1, withscores=True)[0] == (
            url.encode('UTF-8'),  # b'http://some_url.com/'
            priority
        )
    data = json.loads(redis_client.hmget(
            hash_event_listeners + ':' + event_id, url)[0].decode('UTF-8')
        )
    assert data is not None
    assert data == {
            "required_data": required_data,
        }


@pytest.fixture(scope='function')
def redis_retro_event_listener_entry_with_a_datetime_start(redis_client):
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_1.com/"
    priority = 100
    datetime_start = "20200503174008.553417"

    redis_client.zadd(sorted_set_retro_event_listeners, {
                url: priority
            })
    redis_client.hmset(hash_retro_event_listeners, {
                url: json.dumps({
                    "event_id": event_id,
                    "url": url,
                    "datetime_start": datetime_start,
                })
            })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 0
    assert redis_client.zcard(sorted_set_retro_event_listeners) == 1
    assert redis_client.hlen(hash_retro_event_listeners) == 1
    assert redis_client.zrange(
            sorted_set_retro_event_listeners, 0, -1, withscores=True)[0] == (
            url.encode('UTF-8'),  # b'http://some_url.com/'
            priority
        )
    data = json.loads(redis_client.hmget(
            hash_retro_event_listeners, url)[0].decode('UTF-8')
        )
    assert data is not None
    assert data == {
            "event_id": event_id,
            "url": url,
            "datetime_start": datetime_start,
        }


@pytest.fixture(scope='function')
def redis_retro_event_listener_entry_with_a_datetime_start_and_a_count_of_2(redis_client):
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_1.com/"
    priority = 100
    datetime_start = "20200503174008.553417"
    count = 2

    redis_client.zadd(sorted_set_retro_event_listeners, {
                url: priority
            })
    redis_client.hmset(hash_retro_event_listeners, {
                url: json.dumps({
                    "event_id": event_id,
                    "url": url,
                    "datetime_start": datetime_start,
                    "count": count,
                })
            })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 0
    assert redis_client.zcard(sorted_set_retro_event_listeners) == 1
    assert redis_client.hlen(hash_retro_event_listeners) == 1
    assert redis_client.zrange(
            sorted_set_retro_event_listeners, 0, -1, withscores=True)[0] == (
            url.encode('UTF-8'),  # b'http://some_url.com/'
            priority
        )
    data = json.loads(redis_client.hmget(
            hash_retro_event_listeners, url)[0].decode('UTF-8')
        )
    assert data is not None
    assert data == {
            "event_id": event_id,
            "url": url,
            "datetime_start": datetime_start,
            "count": count,
        }


@pytest.fixture(scope='function')
def redis_retro_event_listener_entry_with_a_datetime_start_and_a_count_of_5(redis_client):
    event_id = 'sequence_video_clips-melt_ended_event'
    url = "http://some_url_1.com/"
    priority = 100
    datetime_start = "20200503174008.553417"
    count = 5

    redis_client.zadd(sorted_set_retro_event_listeners, {
                url: priority
            })
    redis_client.hmset(hash_retro_event_listeners, {
                url: json.dumps({
                    "event_id": event_id,
                    "url": url,
                    "datetime_start": datetime_start,
                    "count": count,
                    # "transaction_id": "20200503174008.553417",
                })
            })
    assert redis_client.zcard(sorted_set_events_with_listeners) == 0
    assert redis_client.zcard(sorted_set_retro_event_listeners) == 1
    assert redis_client.hlen(hash_retro_event_listeners) == 1
    assert redis_client.zrange(
            sorted_set_retro_event_listeners, 0, -1, withscores=True)[0] == (
            url.encode('UTF-8'),  # b'http://some_url.com/'
            priority
        )
    data = json.loads(redis_client.hmget(
            hash_retro_event_listeners, url)[0].decode('UTF-8')
        )
    assert data is not None
    assert data == {
            "event_id": event_id,
            "url": url,
            "datetime_start": datetime_start,
            "count": count,
        }
    # assert data['transaction_id'] == "20200503174008.553417"


@pytest.fixture(scope='function')
def standard_db_event_entries(db_client):
    events = [
        {
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
                ]
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
                ]
            }},
        {
            "_id": "20200503223628.840265",
            "event_id": "some_md5_event",
            "transaction_id": "20200503174008.553417",
            "data": {
                "md5": "052226681a4fd9066671a9903cf84be8",
                "file_size": 68,
                "devices": [
                    "OnatMacBookPro"
                ],
                "OnatMacBookPro:file_ids": [
                    "1:c2875f140136afd4534fcd978c82af0b897fe96e"
                ]
            }},
        {
            "_id": "20200503223628.840266",
            "event_id": "sequence_video_clips-melt_ended_event",
            "transaction_id": "20200503174008.553417",
            "data": {
                "return_code": 0,
                "file_path_name": "/video_clips/output-20200503-090308.mp4",
                "transaction_id": "20200503174008.553417",
            }},
        {
            "_id": "20200503174008.553417",
            "event_id": "get_new_clips_microservice-ended_event",
            "transaction_id": "20200503174008.553417",
            "data": {
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
            }},
        {
            "_id": "20200503223628.840250",
            "event_id": "get_new_clips_microservice-ended_event",
            "transaction_id": "20200503223628.840250",
            "data": {
                'clips': [{
                        'clip_url': 'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp',
                        'clip_title_text': 'im not gay', 'clip_player_name': 'YTMagMa',
                        'clip_player_url': 'https://www.twitch.tv/ytmagma',
                        'clip_image_url': 'https://clips-media-assets2.twitch.tv/AT-cm%7C766375085-preview-260x147.jpg',
                        'clip_duration': 12, 'clip_view_count': 3000, 'clip_hours_ago': '1 hour ago'
                    }
                ]
            }},
        {
            "_id": "20200503223628.840260",
            "event_id": "get_new_clips_microservice-ended_event",
            "transaction_id": "20200503174008.553417",
            "data": {
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
                ]
            }},
        {
            "_id": "20200503174008.553418",
            "event_id": "dl_video_clips_microservice-ended_event",
            "transaction_id": "20200503174008.553417",
            "data": {
                'video_clips': {
                    'https://www.twitch.tv/benjyfishy/clip/KawaiiDreamyKangarooThisIsSparta': {},
                    'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {}
                }
            }},
        {
            "_id": "20200503223628.840251",
            "event_id": "dl_video_clips_microservice-ended_event",
            "transaction_id": "20200503223628.840250",
            "data": {
                'video_clips': {
                    'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {
                        'file_path_name': '/video_clips/im not gay.mp4', 'return_code': 0, 'std_errors': [],
                        'std_outputs': []
                    }
                }
            }},
        {
            "_id": "20200503223628.840261",
            "event_id": "dl_video_clips_microservice-ended_event",
            "transaction_id": "20200503174008.553417",
            "data": {
                'video_clips': {
                    'https://www.twitch.tv/benjyfishy/clip/KawaiiDreamyKangarooThisIsSparta': {
                        'file_path_name': '/video_clips/Benjy Music Timing.mp4', 'return_code': 0, 'std_errors': [],
                        'std_outputs': []
                    },
                    'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {
                        'file_path_name': '/video_clips/im not gay.mp4', 'return_code': 0, 'std_errors': [],
                        'std_outputs': []
                    }
                }
            }},
        {
            "_id": "20200503174008.553419",
            "event_id": "convert_video_clips_microservice-ended_event",
            "transaction_id": "20200503174008.553417",
            "data": {
                'converted_video_clips': {
                    'https://www.twitch.tv/benjyfishy/clip/KawaiiDreamyKangarooThisIsSparta': {},
                    'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {}
                },
            }},
        {
            "_id": "20200503223628.840252",
            "event_id": "convert_video_clips_microservice-ended_event",
            "transaction_id": "20200503223628.840250",
            "data": {
                'converted_video_clips': {
                    'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': {
                        'file_path_name': '/video_clips/im not gay.Vimeo_YouTube_HQ_1080p60.mp4',
                        'return_code': 0, 'std_errors': [], 'std_outputs': []
                    }
                },
            }},
        {
            "_id": "20200503223628.840262",
            "event_id": "convert_video_clips_microservice-ended_event",
            "transaction_id": "20200503174008.553417",
            "data": {
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
            }},
        {
            "_id": "20200503174008.553420",
            "event_id": "sequence_video_clips_microservice-ended_event",
            "transaction_id": "20200503174008.553417",
            "data": {
                'sequenced_video_clips': {
                    'file_path_name': '/video_clips/output-20200628-084301.mp4', 'return_code': 0, 'std_errors': [],
                    'std_outputs': [],
                    'timecodes': {}
                },
            }},
        {
            "_id": "20200503223628.840253",
            "event_id": "sequence_video_clips_microservice-ended_event",
            "transaction_id": "20200503223628.840250",
            "data": {
                'sequenced_video_clips': {
                    'file_path_name': '/video_clips/output-20200628-084301.mp4', 'return_code': 0, 'std_errors': [],
                    'std_outputs': [],
                    'timecodes': {
                        'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': 0
                    }
                },
            }},
        {
            "_id": "20200503223628.840263",
            "event_id": "sequence_video_clips_microservice-ended_event",
            "transaction_id": "20200503174008.553417",
            "data": {
                'sequenced_video_clips': {
                    'file_path_name': '/video_clips/output-20200628-084301.mp4', 'return_code': 0, 'std_errors': [],
                    'std_outputs': [],
                    'timecodes': {
                        'https://www.twitch.tv/benjyfishy/clip/KawaiiDreamyKangarooThisIsSparta': 12,
                        'https://www.twitch.tv/ytmagma/clip/ShyFantasticPterodactylPogChamp': 0
                    }
                },
            }},
    ]

    dispatcher = event_dispatcher.EventDispatcher()
    dispatcher.CONFIG['MONGODB_HOST'] = CONFIG['MONGODB_HOST']
    dispatcher.CONFIG['MONGODB_DB'] = CONFIG['MONGODB_DB']
    dispatcher.CONFIG['TESTING'] = CONFIG['TESTING']
    db_client = dispatcher.establish_mongodb_connection()
    for event in events:
        result = dispatcher.store_event_to_db(event['_id'], event['event_id'], event['transaction_id'], event['data'])
        assert result == {
                'n': 1, 'nModified': 0, 'upserted': event['_id'], 'ok': 1.0, 'updatedExisting': False
            }
        retrieved_event = db_client.received_events.find_one({'_id': event['_id']})
        assert retrieved_event is not None
        assert retrieved_event == event
