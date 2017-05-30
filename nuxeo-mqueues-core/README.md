nuxeo-mqueues-core
===========================

## About

 This module implements an asynchronous message passing system called MQueue. 
 
 A MQueue acts as a partitioned queue. MQueue stands for Multi Queue.
 
 MQueue is used in different producer consumer patterns described below.

 This module also contains a Computation pattern not tied to MQueue (even if one possible impl is done using MQueue).
 
 This module has no dependency on Nuxeo framework to ease integration with third party.

## Warning

  This module is under development and still experimental, interfaces and implementations may change until it is announced as a stable module.

## MQueue 

### Features

A MQueue is a partitioned queue, this can be seen as an array of queues. 
Each queue being unbounded and persisted.

A producer is responsible for choosing which message to assign to which queue:

* Using a round robin algorithm a producer can balance messages between queues.
* Using a shard key it can group message by queue following its own semantic.

There is no back pressure on producer because the queues are unbounded (persisted outside of JVM memory),
so the producer is never blocked when appending a message.

A consumer reads message using a tailer, the tailer don't destroy messages while reading from a queue.
Each queue of an MQueue is an ordered immutable sequence of messages that are appended. 

The maximum consumer concurrency is fixed by the size of the MQueues (the number of its partition) 

Tailer position (aka offset) can be persisted, this enable to have consumer that stop and resume processing without loosing messages.
Tailer can also read message from the beginning or end of a queue.

Tailer offsets are persisted in a namespace, this enable to create group of consumers that process concurrently 
the same mqueues at their own speed.

### MQueue Implementations

#### Chronicle Queue
 
  [Chronicle Queues](https://github.com/OpenHFT/Chronicle-Queue) is a high performance off-Heap queue implementation.

  A MQueue is composed of multiple Chronicle Queues (one for each queue).
  There is an additional Chronicle Queue created for each consumer offset namespace.

  This implementation is limited to a single node because the Chronicle Queue can not be distributed
  with the open source version.
  
  The queues are persisted on disk, at the moment there is no retention policy so everything is kept for ever until [NXP-22113](https://jira.nuxeo.com/browse/NXP-22113)
  
  
#### Kafka

  [Kafka](http://kafka.apache.org/) is a distributed streaming app framework.
  
  A MQueue is simply a Topic with partitions equals to the MQueue size.
  
  Tailer is assigned to a topic/partition manually. Offset are also managed (committed) manually. 
 
  As a result Kafka does not manage the consumer distribution in this implementation.
  All consumer of a namespace (a group) must run on the same node.
  
  Still it is possible to distribute producer and consumer group around nodes.  


### Producer/Consumer Patterns

Based on MQueue API we can implement multiple patterns of producer/consumer:

1. Queuing (aka work queue): a message is delivered to one and only one consumer
  * producers append messages to a MQueue
  * there is a single consumer per partitioned queue: the number of consumers is equal to the number of queues
  
2. Pub/Sub (aka event bus): an event is publish on a channel, multiple listeners can subscribe to a channel
  * a publisher append messages to a MQueue (channel)
  * subscribers read messages from a queue, each subscriber has its own tailer namespace

The first Queuing pattern has an API available to provide easy to implement producer and consumer,
it comes with a batching and retry policy (see below).

The pub/sub does not require extra API than the MQueue API. 

### Queuing with a limited amount of messages

Typical usage can be a mass import process where producers extract documents and consumer import documents:

* it decouples producers and consumers: import process can be run multiple time in a deterministic way for debugging and tuning.
* it brings concurrency in import when producer dispatch messages with a correct semantic and evenly.

For efficiency consumer process message per batch. For reliability consumer follow a retry policy.

This is a one time process:

* Producers end on error or when all message are sent.
* Consumers stop in error (according to the retry policy) or when all messages are processed.

The proposed solution takes care of:

* Driving producers/consumers thread pools
* Following a [consumer policy](https://github.com/nuxeo/nuxeo-mqueues/blob/master/nuxeo-mqueues-core/src/main/java/org/nuxeo/ecm/platform/importer/mqueues/pattern/consumer/ConsumerPolicy.java) that defines:
    - the batch policy: capacity and timeout
    - the retry policy: which exceptions to catch, number of retry, backoff and much more see [failsafe](https://github.com/jhalterman/failsafe) library for more info
    - when to stop and what to do in case of failure
* Saving the consumer's offset when a batch of messages is successfully processed
* Starting consumers from the last successfully processed message
* Exposing metrics for producers and consumers

To use this pattern one must implement a [ProducerIterator](https://github.com/nuxeo/nuxeo-mqueues/blob/master/nuxeo-mqueues-core/src/main/java/org/nuxeo/ecm/platform/importer/mqueues/producer/ProducerIterator.java) and a [Consumer](https://github.com/nuxeo/nuxeo-mqueues/blob/master/nuxeo-mqueues-core/src/main/java/org/nuxeo/ecm/platform/importer/mqueues/pattern/consumer/Consumer.java) with factories.
Both the producer and consumer implementation are driven (pulled) by the module.

See [TestBoundedQueuingPattern](https://github.com/nuxeo/nuxeo-mqueues/blob/master/nuxeo-mqueues-core/src/test/java/org/nuxeo/ecm/platform/importer/mqueues/tests/pattern/TestPatternBoundedQueuing.java) for basic examples.

### Queuing unlimited

Almost the same as pattern as above but producers and consumers are always up processing an infinite flow of messages.
There is no Producer interface, a producer just use a [MQueue](https://github.com/nuxeo/nuxeo-mqueues/blob/master/nuxeo-mqueues-core/src/main/java/org/nuxeo/ecm/platform/importer/mqueues/mqueues/MQueue.java) to append messages.

The Consumer is driven the same way but its policy is different:

* a consumer will wait forever on new message
* after a failure on the retry policy, the consumer will continue and take the next message
* consumer can be stopped properly using a poison pill message

A producer can wait for a message to be consumed, this can simulate an async call.

See [TestQueuingPattern](https://github.com/nuxeo/nuxeo-mqueues/blob/master/nuxeo-mqueues-core/src/test/java/org/nuxeo/ecm/platform/importer/mqueues/tests/pattern/TestPatternQueuing.java) for basic examples.


## Stream and Computations
 
This pattern is taken from [Google MillWheel](https://research.google.com/pubs/pub41378.html) and is implemented in [Concord.io](http://concord.io/docs/guides/architecture.html
) and not far from  [Kafka Stream Processor](https://github.com/apache/kafka/blob/trunk/streams/src/main/java/org/apache/kafka/streams/processor/Processor.java).

Instead of message we have record that hold some specific fields like the key and a watermark in addition to the payload.

The key is used to route the record. Records with the same key are always routed to the same computation instance.

The computation is defined almost like in [concord](http://concord.io/docs/guides/concepts.html).
 
The Topology represent a DAG of computations, that can be executed using a ComputationManager.
Computation read from 0 to n streams and write from 0 to n streams.

Here is an example of the DAG used in UT:

![dag](dag1.png)



### Computation implementation

A default implementation of Computation is provided based on MQueue, stream are simply a MQueue of Record.

Using MQueue brings MQueue limitation: a computation group must run on the same node.

Alternative implementations are in progress [NXP-22397](https://jira.nuxeo.com/browse/NXP-22397)

## Building

To build and run the tests, simply start the Maven build:

    mvn clean install

### Run Unit Tests with Kafka

 Test with kafka implementation rely on an assumption, if the Kafka cluster is not accessible tests are not launched.
 
 The easiest way to run a Kafka cluster is using [docker-compose](https://docs.docker.com/compose/install/):

    cd ./kafka-docker/
    docker-compose up -d
    # to stop
    docker-compose down

### Following Project QA Status
[![Build Status](https://qa.nuxeo.org/jenkins/buildStatus/icon?job=master/addon_nuxeo-mqueues-master)](https://qa.nuxeo.org/jenkins/job/master/job/addon_nuxeo-mqueues-master/)


## About Nuxeo
Nuxeo dramatically improves how content-based applications are built, managed and deployed, making customers more agile, innovative and successful. Nuxeo provides a next generation, enterprise ready platform for building traditional and cutting-edge content oriented applications. Combining a powerful application development environment with SaaS-based tools and a modular architecture, the Nuxeo Platform and Products provide clear business value to some of the most recognizable brands including Verizon, Electronic Arts, Sharp, FICO, the U.S. Navy, and Boeing. Nuxeo is headquartered in New York and Paris. More information is available at www.nuxeo.com.
