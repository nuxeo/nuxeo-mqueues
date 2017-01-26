nuxeo-mqueues-core
===========================

## About

This module implements a generic message producers/consumers pattern using a set of unbounded persisted queues.

A message describes an action that will happen in the future when a consumer will process it.

This is a one-to-one relation between a producer and a consumer. 

This can be seen as an async API for a service, or a work queue.

## Warning

This module is under development and still experimental, interfaces and implementations may change until it is announced as a stable module.


## Features

Here is a list of features with rationals:

- A producer dispatch messages among a set of queues called a mqueues:
    - the queue decouple producer and consumer work
    - the message ordering and sharding is the producer responsability, this enable to have simple and generic consumer 

- For each queue there is a single consumer thread:
    - for a consumer the order of messages is deterministic
    - it makes it easy to ensure that a message is delivered once and only once
    - the consumer concurrency is equals to the number of queues,
      the producer should distribute evenly the message to get the optimal concurrency

- A consumer acts as a tailer:
    - consumer don't destroy messages while reading
    - messages can be processed again in the same order or from a random position

- The queues are presisted:
    - The messages appended by producers are presisted even if the JVM is stopped.
    - The retention policy is up to the mqueue implementation

- The queues are unbounded:
    - the producer will never be blocked when appending a message.
    - The mqueues implementation must be Off-Heap and have no limit in the number of messages.

- Consumer follows a batch policy:
    - consumer is pulled to accept message, start a batch, commit or rollback a batch

- Consumer's queue position is persisted:
    - After a batch commit the consumer position is persisted
    - A consumer can restart from the beginning or from the last successful batch

- Consumer follows a retry policy:
    - On consumer failure the consumer will replay the messages according to a retry policy.
    - when retrying the batch size is set to one


## Contracts

The module takes care of:

- driving the producers/consumers thread pools
- following the consumer batch policy
- following the consumer retry policy
- save the consumer position when a batch of message is processed
- start consumers where it was stopped/crasched
- exposing metrics for producers and consumers

The contract to implement a new producer/consumer is the following:

- A [Message](https://github.com/nuxeo/nuxeo-mqueues/blob/master/nuxeo-mqueues-core/src/main/java/org/nuxeo/ecm/platform/importer/mqueues/message/Message.java) must implement the Java Externalizable interface (Serializable is not enough efficient).
- A [Producer](https://github.com/nuxeo/nuxeo-mqueues/blob/master/nuxeo-mqueues-core/src/main/java/org/nuxeo/ecm/platform/importer/mqueues/producer/Producer.java) must implement a Java Iterator of message and knows how to shard its messages.
- A [Consumer](https://github.com/nuxeo/nuxeo-mqueues/blob/master/nuxeo-mqueues-core/src/main/java/org/nuxeo/ecm/platform/importer/mqueues/consumer/Consumer.java) receives message and can performs action on batch steps (begin, commit, rollback)

## Default MQueues implementation

The default queues implementation is using [Chronicle Queues](https://github.com/OpenHFT/Chronicle-Queue) which is an Off-Heap implementation.

A MQueues of size N will creates N+1 Chronicle Queues, one for each queue and a special offset queue to track the consumer offset position. 

The only limitation is the available disk storage, there is no retention policy so everything is kept for ever.

Chronicle Queue creates a single file per queue and per day, so it is possible to script some retention policy like keep message queues for X days. 


## Building

To build and run the tests, simply start the Maven build:

    mvn clean install

### Following Project QA Status
[![Build Status](https://qa.nuxeo.org/jenkins/buildStatus/icon?job=master/addon_nuxeo-mqueues-master)](https://qa.nuxeo.org/jenkins/job/master/job/addon_nuxeo-mqueues-master/)


## About Nuxeo
Nuxeo dramatically improves how content-based applications are built, managed and deployed, making customers more agile, innovative and successful. Nuxeo provides a next generation, enterprise ready platform for building traditional and cutting-edge content oriented applications. Combining a powerful application development environment with SaaS-based tools and a modular architecture, the Nuxeo Platform and Products provide clear business value to some of the most recognizable brands including Verizon, Electronic Arts, Sharp, FICO, the U.S. Navy, and Boeing. Nuxeo is headquartered in New York and Paris. More information is available at www.nuxeo.com.
