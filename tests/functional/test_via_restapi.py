from api import api
from flask import request
import pytest
import flask
import re


@pytest.fixture
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


def test_hello(api_client):
    """
    GIVEN a Flask application
    WHEN the '/hello/' page is requested (GET) and given a name member in the JSON payload
    THEN that name's value should appear in the returned string
    """
    response = api_client.post('/hello/', json={
        'name': 'Flask'
    })
    assert response.status_code == 200
    assert b'Hello, Flask!' in response.data


# def test_distribute_event(api_client):
#     """
#     GIVEN a Flask application
#     WHEN the '/distribute_event/<event>' page is requested (POST) and given an event and some json content
#     THEN the event and the json content should appear in the returned formatted string
#     """
#     response = api_client.post('/distribute_event/some_event', json={
#         'username': 'flask', 'password': 'secret'
#     })
#     assert response.status_code == 200
#     assert b"Received event: some_event:" in response.data
#     assert b", with JSON data: {" in response.data
#     assert b"'username': 'flask'" in response.data
#     assert b"'password': 'secret'" in response.data
#
#     with api.app.test_request_context('/distribute_event/some_event', json={
#             'username': 'flask', 'password': 'secret'
#         }):
#         assert flask.request.path == '/distribute_event/some_event'
#         json_data = flask.request.get_json()
#         assert json_data['username'] == 'flask'
#         assert json_data['password'] == 'secret'
#
#     with api.app.test_client() as c:
#         rv = c.post('/distribute_event/some_event', json={
#             'username': 'flask', 'password': 'secret'
#         })
#         assert request.path == '/distribute_event/some_event'
#         assert rv.status_code == 200
#         assert b"Received event: some_event:" in rv.data
#         assert b", with JSON data: {" in rv.data
#         assert b"'username': 'flask'" in rv.data
#         assert b"'password': 'secret'" in rv.data


def test_receive_event(api_client):
    """
    GIVEN a Flask application
    WHEN the '/receive_event/<event>' page is requested (POST) and given an event and some json content
    THEN the event and the json content should appear in the returned formatted string
    """
    response = api_client.post('/receive_event/some_event', json={
        'username': 'flask', 'password': 'secret'
    })
    assert response.status_code == 200
    p = re.compile('{"event_received":{"data":{([^,]*),([^,]*),([^}]*)},"event":"some_event:(?P<datetime>[.0-9]+)"}}')
    m = p.match(response.data.decode('UTF-8'))
    print(response.data.decode('UTF-8'))
    assert m is not None
    assert '"username":"flask"' in m.groups()
    assert '"password":"secret"' in m.groups()
    assert '"transaction_id":"' + m.group('datetime') + '"' in m.groups()

    with api.app.test_request_context('/receive_event/some_event', json={
            'username': 'flask', 'password': 'secret'
        }):
        assert flask.request.path == '/receive_event/some_event'
        json_data = flask.request.get_json()
        assert json_data['username'] == 'flask'
        assert json_data['password'] == 'secret'

    # Another way of testing
    with api.app.test_client() as c:
        rv = c.post('/receive_event/some_event', json={
            'username': 'flask', 'password': 'secret'
        })
        assert request.path == '/receive_event/some_event'
        assert rv.status_code == 200
        p = re.compile(
                '{"event_received":{"data":{([^,]*),([^,]*),([^}]*)},"event":"some_event:(?P<datetime>[.0-9]+)"}}'
            )
        m = p.match(rv.data.decode('UTF-8'))
        print(rv.data.decode('UTF-8'))
        assert m is not None
        assert '"username":"flask"' in m.groups()
        assert '"password":"secret"' in m.groups()
        assert '"transaction_id":"' + m.group('datetime') + '"' in m.groups()


def test_receive_event_with_redirect_url(api_client):
    """
    GIVEN a Flask application
    WHEN the '/receive_event/<event>' page is requested (POST) and given an event and some json content, but this time
    with a key-value having a key of 'redirect_url'
    THEN it should redirect (302) the request to the url given in the paired value to the 'redirect_url' key
    """
    response = api_client.post('/receive_event/some_event', json={
        'username': 'flask', 'password': 'secret',
        'redirect_url': 'https://example.com'
    })
    assert response.status_code == 302
    assert b'<title>Redirecting...</title>' in response.data
    assert b'<h1>Redirecting...</h1>' in response.data
    assert b'redirected automatically to' in response.data
    assert b'target URL: <a href="https://example.com">https://example.com</a>.  If not click the link.' in response.data

    with api.app.test_request_context('/receive_event/some_event', json={
        'username': 'flask', 'password': 'secret',
        'redirect_url': 'https://example.com'
    }):
        assert flask.request.path == '/receive_event/some_event'
        json_data = flask.request.get_json()
        assert json_data['username'] == 'flask'
        assert json_data['password'] == 'secret'
        assert json_data['redirect_url'] == 'https://example.com'

    with api.app.test_client() as c:
        rv = c.post('/receive_event/some_event', json={
            'username': 'flask', 'password': 'secret',
            'redirect_url': 'https://example.com'
        })
        assert request.path == '/receive_event/some_event'
        assert rv.status_code == 302
        assert b'<title>Redirecting...</title>' in rv.data
        assert b'<h1>Redirecting...</h1>' in rv.data
        assert b'redirected automatically to ' in rv.data
        assert b'target URL: <a href="https://example.com">https://example.com</a>.  If not click the link.' in rv.data
