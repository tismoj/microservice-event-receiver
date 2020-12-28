# microservice-event-receiver

This project is about supporting microservices by providing the most basic event discimination system designed around the concept of microservices for High Availability and multi-node microservice state syncronization.

In plain terms, let's say in you're project you decided to design it into microservices upfront so that in the future if you need to scale it up or down, you just simply add or remove replica nodes and you're done. But in reality most microservice on its own will not just simply scale when you just add new replica nodes to it.

The problem is with the persistent data, or database or the current state of the microservice, depending on how you designed your microservices. For example most designs would have let's say microservice_A recieve a request, process it, update or read either it's state variables, its database, or a file, then returns a response, now even if you replicate microservice_A into several replica nodes to improve high availability, they all would most likely connect to the same database or file, to make sure the data is consistent to all replica nodes, except in the case of a microservice having state variables, that data is very hard to keep consistent to all its replica nodes, not to mention issues with race conditions. Now imagine if all your replica nodes connect to this one database and it goes down, no matter how many replica nodes you have they will all go down.

This project hopes to alleviate that scenario by providing an easy to integrate way of keeping each and every replica node syncronized.

The concept is simple normally microservices receive request either from other microservices or input from the user.

Instead simply funnel all the vital state changing communications between microservices to go through the event_receiver
