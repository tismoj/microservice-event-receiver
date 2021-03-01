from flask import Flask, request, redirect, jsonify
from flask_restful import Resource, Api
from nameko.standalone.rpc import ClusterRpcProxy
import json

app = Flask(__name__)
api = Api(app)

CONFIG = {'AMQP_URI': "amqp://guest:guest@rabbitmq"}


class Hello(Resource):
    def post(self):
        data = request.get_json()
        print("In Hello: Received Event with JSON data: " + str(data))
        with ClusterRpcProxy(CONFIG) as rpc:
            if 'name' not in data:
                return rpc.event_receiver_microservice.receive_event('Hello_arg_missing-name', data)
            response = rpc.hello_microservice.hello(data)
            print("Microservice returned with a response: " + json.dumps(response))
            return jsonify(response)


api.add_resource(Hello, '/', '/hello/')


class Hello2(Resource):
    def get(self, name):
        print("Received request for Hello2 from " + name)
        with ClusterRpcProxy(CONFIG) as rpc:
            response = rpc.hello2_microservice.hello2(name)
            print("Microservice returned with a response: " + response)
            return response


api.add_resource(Hello2, '/', '/hello2/<string:name>')


class RegisterForEvents(Resource):
    def post(self):
        data = request.get_json()
        print("In RegisterForEvents: Listen for Events with JSON data: " + str(data))
        with ClusterRpcProxy(CONFIG) as rpc:
            if 'events_to_register' not in data:
                return rpc.event_receiver_microservice.receive_event('RegisterForEvents_arg_missing-events_to_register', data)
            response = rpc.event_listener_registrar_microservice.register_for_events(data)
            print("Micro-service returned with a response: " + json.dumps(response))
            return response


api.add_resource(RegisterForEvents, '/', '/register_for_events/')


class ReceiveEvent(Resource):
    def post(self, event_id):
        data = request.get_json()
        print("In ReceiveEvent: Received event: " + event_id + ', with JSON data: ' + str(data))
        with ClusterRpcProxy(CONFIG) as rpc:
            response = rpc.event_receiver_microservice.receive_event(event_id, data)
            print("Micro-service returned with a response: " + json.dumps(response))
            if 'redirect_url' in data and data['redirect_url'] != '':
                return redirect(data['redirect_url'])
            else:
                return jsonify(response)


api.add_resource(ReceiveEvent, '/', '/receive_event/<string:event_id>')


if __name__ == '__main__':
    print("Starting the job listing API ...")
    #app.run(host='0.0.0.0', port=8080, debug=True)
    app.run(host='0.0.0.0', port=8080)
