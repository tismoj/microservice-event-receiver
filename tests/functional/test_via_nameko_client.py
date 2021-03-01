from nameko.standalone.rpc import ClusterRpcProxy
import re

CONFIG = {'AMQP_URI': "amqp://guest:guest@localhost"}


def test_hello_microservice():
    """
    GIVEN a Nameko client
    WHEN the 'hello_microservice.hello({"name": <name>})' is invoked and given a dictionary argument having a name member
    THEN that name's value should appear in the returned formatted string
    """
    with ClusterRpcProxy(CONFIG) as rpc:
        result = rpc.hello_microservice.hello({"name": "Nameko Client"})
        assert result == {"hello": {"response": "Hello, Nameko Client!"}}


def test_hello2_microservice():
    """
    GIVEN a Nameko client
    WHEN the 'hello2_microservice.hello2(name)' is invoked and given a name argument
    THEN that name argument should appear in the returned formatted string
    """
    with ClusterRpcProxy(CONFIG) as rpc:
        result = rpc.hello2_microservice.hello2("Nameko Client")
        assert result == "Hello, Nameko Client!"


def test_event_receiver_microservice():
    """
    GIVEN a Nameko client
    WHEN the 'event_receiver_microservice.receive_event(event, json)' is invoked and given an event and a json string
    argument
    THEN those 2 arguments should appear in the returned formatted string
    """
    with ClusterRpcProxy(CONFIG) as rpc:
        result = rpc.event_receiver_microservice.receive_event("some_event", {
            'key1':'value1'
        })
        p = re.compile('some_event:(?P<datetime>[.0-9]+)')
        m = p.match(result['event_received']['event'])
        assert m is not None
        assert result['event_received']['data']['key1'] == 'value1'
        assert result['event_received']['data']['transaction_id'] == m.group('datetime')


def test_event_receiver_microservice_with_redirect_url():
    """
    GIVEN a Nameko client
    WHEN the 'event_receiver_microservice.receive_event(event, json)' is invoked and given an event and a json string
    argument, but this time with a key-value having a key of 'redirect_url'
    THEN it should still behave the same where those 2 arguments should still appear in the returned formatted string
    which is exactly the same as before, the difference will happen only on the rest api side only
    """
    with ClusterRpcProxy(CONFIG) as rpc:
        result = rpc.event_receiver_microservice.receive_event("some_event", {
            'key1': 'value1',
            'redirect_url': 'https://example.com'
        })
        p = re.compile('some_event:(?P<datetime>[.0-9]+)')
        m = p.match(result['event_received']['event'])
        assert m is not None
        assert result['event_received']['data']['key1'] == 'value1'
        assert result['event_received']['data']['redirect_url'] == 'https://example.com'
        assert result['event_received']['data']['transaction_id'] == m.group('datetime')
