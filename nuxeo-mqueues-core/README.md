nuxeo-mqueues-core
===========================

## About

This module implements a generic message queue (MQueues) used to implement different producers/consumers patterns.

## Warning

This module is under development and still experimental, interfaces and implementations may change until it is announced as a stable module.

## MQueues features

A MQueues (Multiple Queues) is a bounded array of queues, each queue is referenced by an index.

A producer is responsible for choosing which message to assign to which queue:

* Using a round robin algorithm a producer can balance messages between queues
* Using a shard key (or any custom logic) producers can group messages by queue, the "grouping" semantic is shared with the consumers.

Each queue is an ordered immutable sequence of messages that are appended:

* The producer will never be blocked when appending a message.
* The messages are persisted outside of the JVM
* The retention policy is up to the MQueues implementation

Consumers read messages using a tailer, the current offset (read position) for a queue can be persisted (commit):

* Consumer don't destroy messages while reading from a queue.
* Consumer can start reading messages from the last committed message, the beginning or end of a queue.

Consumers can choose a namespace to persist its offset:

* Multiple consumers can tail a queue at different speed

This is enough to implement the two main patterns of producer/consumer:

1. Queuing (aka work queue): a message is delivered to one and only one consumer
  * producers dispatch messages in queues
  * there is a single consumer per queue (nb of queues = nb of concurrent consumers)
2. Pub/Sub (aka event bus): a message is delivered to multiple consumers interested in a topic
  * each queue in a mqueues can be seen as a topic
  * consumer subscribe a topic by getting a tailer to a queue
  * consumer persists its offset in a private name space


## Default MQueues implementation

The default queues implementation is using [Chronicle Queues](https://github.com/OpenHFT/Chronicle-Queue) which is an Off-Heap implementation.

A MQueues of size N will creates N Chronicle Queues, one for each queue.
There is an additional Chronicle Queue created for each consumer offset namespace.

The only limitation is the available disk storage, there is no retention policy so everything is kept for ever.

That being said Chronicle Queue creates a single file per queue and per day, so it is possible to script some retention policy like keep message queues for the last D days.

## Patterns


### Pattern 1: Queuing with a limited amount of messages

Typical usage can be a mass import process where producers extract documents and consumer import documents:

* it decouples producers and consumers: import process can be run multiple time in a deterministic way for debugging and tuning.
* it brings concurrency in import: producer need to dispatch messages with a correct semantic and evenly.

For efficiency consumer process message per batch. For reliability consumer follow a retry policy.

This is a one time process:

* Producers end on error or when all message are sent.
* Consumers stop in error (according to the retry policy) or when all messages are processed.

The proposed solution takes care of:

* Driving producers/consumers thread pools
* Following the consumer the customizable batch policy
* Following the consumer the customizable retry policy
* Saving the consumer's offset when a batch of messages is successfully processed
* Starting consumers from the last successfully processed message
* Exposing metrics for producers and consumers

To use this pattern one must implement a [ProducerIterator](https://github.com/nuxeo/nuxeo-mqueues/blob/master/nuxeo-mqueues-core/src/main/java/org/nuxeo/ecm/platform/importer/mqueues/producer/ProducerIterator.java) and a [Consumer](https://github.com/nuxeo/nuxeo-mqueues/blob/master/nuxeo-mqueues-core/src/main/java/org/nuxeo/ecm/platform/importer/mqueues/consumer/Consumer.java) with factories.
Both the producer and consumer implementation are driven (pulled) by the module.

See [TestQueuingPattern](https://github.com/nuxeo/nuxeo-mqueues/blob/master/nuxeo-mqueues-core/src/test/java/org/nuxeo/ecm/platform/importer/mqueues/tests/TestQueuingPattern.java) for basic examples.

### Pattern 2: TODO Queuing

Almost the same as pattern as above but producers and consumers are always up processing an infinite flow of messages.
There is no Producer interface, a producer just use a [MQueues](https://github.com/nuxeo/nuxeo-mqueues/blob/master/nuxeo-mqueues-core/src/main/java/org/nuxeo/ecm/platform/importer/mqueues/mqueues/MQueues.java) to append messages.

The Consumer follow the same interface as in previous pattern but it is driven in a different way:

* it will wait for ever on message
* after a failre on the retry policy, the consumer will continue and take the next message

A producer can wait for a message to be consumed.

### Pattern 3: TODO Publish subscribe (Event bus)

No producer interface.

Multiple Consumer with different offset namespace.


## Building

To build and run the tests, simply start the Maven build:

    mvn clean install

### Following Project QA Status
[![Build Status](https://qa.nuxeo.org/jenkins/buildStatus/icon?job=master/addon_nuxeo-mqueues-master)](https://qa.nuxeo.org/jenkins/job/master/job/addon_nuxeo-mqueues-master/)


## About Nuxeo
Nuxeo dramatically improves how content-based applications are built, managed and deployed, making customers more agile, innovative and successful. Nuxeo provides a next generation, enterprise ready platform for building traditional and cutting-edge content oriented applications. Combining a powerful application development environment with SaaS-based tools and a modular architecture, the Nuxeo Platform and Products provide clear business value to some of the most recognizable brands including Verizon, Electronic Arts, Sharp, FICO, the U.S. Navy, and Boeing. Nuxeo is headquartered in New York and Paris. More information is available at www.nuxeo.com.
