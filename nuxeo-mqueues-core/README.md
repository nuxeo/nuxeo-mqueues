nuxeo-mqueues-core
===========================

## About

This module implements a generic message producers/consumers pattern using a set of unbounded persisted queues (mqueues).

Here is a short list of features with rationals:

- A producer dispatch messages into multiple queues called a mqueues:
    - the queue decouple producer and consumer work
    - the message are sharded into different queues to create concurrency

- For each queue there is a single consumer thread:
    - this make consumer work easier by removing concurrency on the queue
    - the order of messages is deterministic

- A consumer acts as a tailer:
    - consumer don't destroy messages while reading
    - messages can be processed again in the same order

- The queues are presisted:
    - The messages appended by producers are presisted even if the JVM is stopped.

- The queues are unbounded:
    - The queue are Off-Heap and has not limit in number of messages,
        note that there are still limit depending on the mqueue implementation like available disk space.
    - the producer will never be blocked when appending a message.

- Consumer follows a batch policy:
    - consumer is notified of batch begin/commit/rollback

- Consumer's queue position is persisted:
    - After a batch commit the consumer position is persisted
    - A consumer can restart from the beginning or from the last successful batch

- Consumer follows a retry policy:
    - On consumer failure the consumer will replay the messages according to a retry policy.


The module takes care of:

- driving the producers/consumers thread pools
- following the consumer batch policy
- following the consumer retry policy
- save the consumer position when a batch of message is processed
- start consumers where it was stopped/crasched
- exposing metrics for producers and consumers

The contract to implement a new producer/consumer is the following:

- A message must implement the Java Externalizable interface (Serializable is not enough efficient).
- A producer must implement a Java Iterator of message and knows how to shard its messages.
- A consumer receives message and can performs action on batch steps (begin, commit, rollback)

The default queues implementation is using Chronicle Queues which is an Off-Heap implementation only limited by the available disk storage.


## Warning

This module is under development and still experimental, interfaces and implementations may change until it is announced as a stable module.


## Building

To build and run the tests, simply start the Maven build:

    mvn clean install

### Following Project QA Status
[![Build Status](https://qa.nuxeo.org/jenkins/buildStatus/icon?job=addons_nuxeo-mqueues-master)](https://qa.nuxeo.org/jenkins/job/addons_nuxeo-mqueues-master/)


## About Nuxeo
Nuxeo dramatically improves how content-based applications are built, managed and deployed, making customers more agile, innovative and successful. Nuxeo provides a next generation, enterprise ready platform for building traditional and cutting-edge content oriented applications. Combining a powerful application development environment with SaaS-based tools and a modular architecture, the Nuxeo Platform and Products provide clear business value to some of the most recognizable brands including Verizon, Electronic Arts, Sharp, FICO, the U.S. Navy, and Boeing. Nuxeo is headquartered in New York and Paris. More information is available at www.nuxeo.com.
