# microservice-event-receiver

This project is about supporting microservices by providing the most basic event distribution and monitoring system designed around the concept of microservices for High Availability and multi-node microservice state synchronization.

In plain terms, let's say in your project you decided to design it into microservices upfront so that in the future if you need to scale it up or down, you just simply add or remove replica nodes and you're done. But in reality, most microservice on their own will not just simply scale when you just add new replica nodes to it.

The problem is with the persistent data, or database, or the current state of the microservice, depending on how you designed your microservices. For example, most designs would have let's say microservice_A receive a request, process it, update or read either it's state variables, its database, or a file, then returns a response, now even if you replicate microservice_A into several replica nodes to improve high availability, they all would most likely connect to the same database or file, to make sure the data is consistent to all replica nodes, except in the case of a microservice having state variables, that data is very hard to keep consistent to all its replica nodes, not to mention issues with race conditions. Now imagine if all your replica nodes connect to this one database and it goes down, no matter how many replica nodes you have they will all go down.

This project hopes to alleviate that scenario by providing an easy to integrate way of keeping each and every replica node synchronized.

The concept is simple normally microservices receive requests either from other microservices or input from the user.

Instead simply just funnel all the vital state-changing communications between microservices (let's call them events) to go through the event_receiver_microservice. And then have each microservice that is expecting to receive a particular event simply register for that event in the event_listener_registrar_microservice, and all events received by event_receiver_microservice will be distributed to each microservice that had registered for that event. Now if you had registered all replica nodes to the same event then all replica nodes will receive that event, and in turn, those replica nodes can then update their individual state variable, their individual database, or their individual file and all data should remain consistent.

Now in case, you added some additional replica nodes because you needed to scale up, the replica can opt to register for the same event with a DateTime range if you want it to receive all relevant past events to have it go through all of the events simulating how the other already existing replicas had received those events in the past. Where if done correctly that new replica should be able to update its individual state variables, its individual database, or its individual file to be consistent with the other replicas.

Since all events and their data received are recorded, you have a very good way of tracking down state or scenario related bugs, and even have an option to rollback all replicas and their dependent nodes to a known working state, if needed.

I started this project around April of 2019 before I initially committed it to Github, previously it was exclusively subversioned using mercurial, so in the commit messages you would notice the original mercurial commit messages copy-pasted. It started as a microservice template that I use for my other microservice projects but I decided to separate it as its own prior to the Github commit, to make it easier to update across all the projects that use it. And I just modify it as the needs from the different projects arises. So if you know of any other similar opensource project out there doing the same thing, I would like to know about it, so do please inform me.

## HOW TO USE:

### Install Procedure
- Clone from GitHub
```bash
$ git clone https://github.com/tismoj/microservice-event-receiver
```

### Startup Procedure
- Startup all microservices and support apps
```bash
$ docker-compose up -d --build
```

### Logs Monitoring
- To view logs
```bash
$ docker-compose logs -f
```

### Typical / Normal RESTapi Request via Curl
- To send a Request directly to the sample microservice hello_microservice
```bash
$ curl -X POST -H 'Content-Type: application/json' localhost:8880/hello/ -d '{"name": "tismoj"}'
{"hello":{"response":"Hello, tismoj!"}}
```

### Sending of a similar Request through the event_receiver, without any microservice, registered to receive it
- To send a similar Request through the event_receiver, but prior to registering hello_microservice to any events
```bash
$ curl -X POST -H 'Content-Type: application/json' localhost:8880/receive_event/request_from_human -d '{"name": "tismoj while unregistered"}'
{"event_received":{"data":{"name":"tismoj while unregistered","transaction_id":"20201228220527.235058"},"event":"request_from_human:20201228220527.235058"}}
```

### Sending of another similar Request through the event_receiver, with a microservice, registered to receive it
- To register hello_microservice to the event request_from_human
```bash
$ curl -X POST -H 'Content-Type: application/json' localhost:8880/register_for_events/ -d '{"events_to_register": [{"event": "request_from_human", "urls_to_register": [{"url": "http://api:8080/hello/"}]}]}'
{"registered_for_events": {"request_from_human": {"http://api:8080/hello/": {}}}}
```

- To send a similar Request through the event_receiver, but this time hello_microservice is registered to receive all events of the event request_from_human
```bash
$ curl -X POST -H 'Content-Type: application/json' localhost:8880/receive_event/request_from_human -d '{"name": "tismoj now registered"}'
{"event_received":{"data":{"name":"tismoj now registered","transaction_id":"20201228220741.306084"},"event":"request_from_human:20201228220741.306084"}}
```

- To check if the event is actually received by the hello_microservice, we could only check through the api_1 app, as I still couldn't figure out how to log prints from any microservice started via nameko
```bash
$ docker-compose logs api
...
api_1                                    | In RegisterForEvents: Listen for Events with JSON data: {'events_to_register': [{'event': 'request_from_human', 'urls_to_register': [{'url': 'http://api:8080/hello/'}]}]}
api_1                                    | Micro-service returned with a response: {"registered_for_events": {"request_from_human": {"http://api:8080/hello/": {}}}}
api_1                                    | 172.27.0.1 - - [28/Dec/2020 22:07:24] "POST /register_for_events/ HTTP/1.1" 200 -
api_1                                    | In ReceiveEvent: Received event: request_from_human, with JSON data: {'name': 'tismoj now registered'}
api_1                                    | Micro-service returned with a response: {"event_received": {"event": "request_from_human:20201228220741.306084", "data": {"name": "tismoj now registered", "transaction_id": "20201228220741.306084"}}}
api_1                                    | 172.27.0.1 - - [28/Dec/2020 22:07:41] "POST /receive_event/request_from_human HTTP/1.1" 200 -
api_1                                    | In Hello: Received Event with JSON data: {'name': 'tismoj now registered', 'transaction_id': '20201228220741.306084'}
api_1                                    | Microservice returned with a response: {"hello": {"response": "Hello, tismoj now registered!"}}
api_1                                    | 172.27.0.9 - - [28/Dec/2020 22:07:41] "POST /hello/ HTTP/1.1" 200 -
...
```
### Sending a new event without again any registered microservice to receive
- Send a request with a new event name request_from_animal
```bash
$ curl -X POST -H 'Content-Type: application/json' localhost:8880/receive_event/request_from_animal -d '{"name": "cat"}'
{"event_received":{"data":{"name":"cat","transaction_id":"20201228230007.695724"},"event":"request_from_animal:20201228230007.695724"}}
```

### Registering hello_microservice for the new Event, but triggering to send all prior events starting from a given "datetime_start"
- To also register hello_microservice to the event request_from_animal, with a datetime_start requirement
```bash
$ curl -X POST -H 'Content-Type: application/json' localhost:8880/register_for_events/ -d '{"events_to_register": [{"event": "request_from_animal", "urls_to_register": [{"url": "http://api:8080/hello/", "datetime_start": "20201228230007.0"}]}]}'
{"registered_for_retro_event_retrieval": {"request_from_animal": {"http://api:8080/hello/": {"datetime_start": "20201228230007.0"}}}}
```
```bash
$ docker-compose logs api
...
In RegisterForEvents: Listen for Events with JSON data: {'events_to_register': [{'event': 'request_from_animal', 'urls_to_register': [{'url': 'http://api:8080/hello/', 'datetime_start': '20201228230007.0'}]}]}
Micro-service returned with a response: {"registered_for_retro_event_retrieval": {"request_from_animal": {"http://api:8080/hello/": {"datetime_start": "20201228230007.0"}}}}
172.27.0.1 - - [28/Dec/2020 23:21:12] "POST /register_for_events/ HTTP/1.1" 200 -
In Hello: Received Event with JSON data: {'name': 'cat', 'transaction_id': '20201228230007.695724'}
Microservice returned with a response: {"hello": {"response": "Hello, cat!"}}
172.27.0.10 - - [28/Dec/2020 23:21:12] "POST /hello/ HTTP/1.1" 200 -
...
```


- To send another request with the new event request_from_animal, but this time hello_microservice is registered to receive all events of the event request_from_animal as well
```bash
$ curl -X POST -H 'Content-Type: application/json' localhost:8880/receive_event/request_from_animal -d '{"name": "dog"}'{"event_received":{"data":{"name":"dog","transaction_id":"20201228232305.280595"},"event":"request_from_animal:20201228232305.280595"}}
```
```bash
$ docker-compose logs api
...
Micro-service returned with a response: {"event_received": {"event": "request_from_animal:20201228232305.280595", "data": {"name": "dog", "transaction_id": "20201228232305.280595"}}}
172.27.0.1 - - [28/Dec/2020 23:23:05] "POST /receive_event/request_from_animal HTTP/1.1" 200 -
In Hello: Received Event with JSON data: {'name': 'dog', 'transaction_id': '20201228232305.280595'}
Microservice returned with a response: {"hello": {"response": "Hello, dog!"}}
172.27.0.9 - - [28/Dec/2020 23:23:05] "POST /hello/ HTTP/1.1" 200 -
...
```

### Registering to retrieve past events given a "count"
- To register and retrieve past events, with a count requirement
```bash
$ curl -X POST -H 'Content-Type: application/json' localhost:8880/register_for_events/ -d '{"events_to_register": [{"event": "request_from_human", "urls_to_register": [{"url": "http://api:8080/hello/", "datetime_start": "20200101000000.0", "count": 1}]}]}'
{"registered_for_retro_event_retrieval": {"request_from_human": {"http://api:8080/hello/": {"datetime_start": "20200101000000.0", "count": 1}}}}
```
```bash
$ docker-compose logs api
...
In RegisterForEvents: Listen for Events with JSON data: {'events_to_register': [{'event': 'request_from_human', 'urls_to_register': [{'url': 'http://api:8080/hello/', 'datetime_start': '20200101000000.0', 'count': 1}]}]}
Micro-service returned with a response: {"registered_for_retro_event_retrieval": {"request_from_human": {"http://api:8080/hello/": {"datetime_start": "20200101000000.0", "count": 1}}}}
172.27.0.1 - - [28/Dec/2020 23:44:38] "POST /register_for_events/ HTTP/1.1" 200 -
In Hello: Received Event with JSON data: {'name': 'tismoj while unregistered', 'transaction_id': '20201228220527.235058'}
Microservice returned with a response: {"hello": {"response": "Hello, tismoj while unregistered!"}}
172.27.0.10 - - [28/Dec/2020 23:44:38] "POST /hello/ HTTP/1.1" 200 -
...
```

## TODO:
- Create additional sample microservices to properly show the other already implemented features (transaction_id, datetime_end, required_data, listener_data)
