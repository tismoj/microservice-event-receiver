# microservice-event-receiver

This project is about supporting microservices by providing the most basic event discimination system designed around the concept of microservices for High Availability and multi-node microservice state syncronization.

In plain terms, let's say in you're project you decided to design it into microservices upfront so that in the future if you need to scale it up or down, you just simply add or remove replica nodes and you're done. But in reality most microservice on its own will not just simply scale when you just add new replica nodes to it.

The problem is with the persistent data, or database or the current state of the microservice, depending on how you designed your microservices. For example most designs would have let's say microservice_A recieve a request, process it, update or read either it's state variables, its database, or a file, then returns a response, now even if you replicate microservice_A into several replica nodes to improve high availability, they all would most likely connect to the same database or file, to make sure the data is consistent to all replica nodes, except in the case of a microservice having state variables, that data is very hard to keep consistent to all its replica nodes, not to mention issues with race conditions. Now imagine if all your replica nodes connect to this one database and it goes down, no matter how many replica nodes you have they will all go down.

This project hopes to alleviate that scenario by providing an easy to integrate way of keeping each and every replica node syncronized.

The concept is simple normally microservices receive request either from other microservices or input from the user.

Instead simply just funnel all the vital state changing communications between microservices (let's call them events) to go through the event_receiver_microservice. And then have each microservice that is expecting to receive a particular event to simply register for that event in the event_listener_registrar_microservice, and all events received by event_receiver_microservice will be distributed to each microservice that had registered for that event. Now if you had registered all replica nodes to the same event then all replica nodes will recieve that event, and in turn those replica node can then update their individual state variable, their individual database, or their individual file and all data should remain consistent.

Now in case you added some additional replica nodes because you needed to scale up, the replica can opt to register for the same event with a datetime range if you want it to receive all relevent past events to have it go through all of the events simulating how the other already existing replicas had received those events in the past. Where if done correctly that new replica should be able to update its individual state variables, its individual database, or its individual file to be consistent with the other replicas.

Since all events and its data received are recorded, you have a very good way of tracking down state or scenario related bugs, and even have an option to rollback all replicas and their depended nodes to a known working state, if needed.
