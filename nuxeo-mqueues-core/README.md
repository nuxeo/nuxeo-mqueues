nuxeo-mqueues-core
===========================

## About

 This module implements an asynchronous message passing system called MQueue.

 MQueue is used in different producer consumer patterns described below.

 This module has no dependency on Nuxeo framework to ease integration with third parties.

## Warning

  This module is under development and still experimental, interfaces and implementations may change until it is announced as a stable module.

## MQueue

### Features

 A MQueue can be seen as an array of queues, MQueue stands for Multi Queue.

 A MQueue acts as a partitioned queue, queues that are part of a MQueue are called __partitions__.

 To write to a MQueue a producer need to acquire an __appender__. The producer is responsible for choosing which message to assign to which partition:

 * Using a round robin algorithm a producer can balance messages between partitions.
 * Using a shard key it can group message by partition following its own semantic.

 There is no back pressure on producer because the partition are unbounded (persisted outside of JVM memory),
 so the producer is never blocked when appending a message.

 To read from a MQueue a consumer need to create a __tailer__. The tailer don't destroy messages while reading from a partition.
 Each partition of a MQueue is an ordered immutable sequence of messages.

 A tailer read message from one or more assigned partitions. It belongs to a consumer __group__.
 which is a name space to store its positions (__offsets__).
 By saving (__commit__) its offsets a consumer group can stop and resume processing without loosing messages.
 By default a Tailer will read from the last committed offset, but it can also read from the beginning or end of its assigned partitions.
 The maximum consumer group concurrency is fixed by the number of partitions of the MQueue (its size).

 Of course it is possible to create different group of consumers that process concurrently the same MQueue at their own speed.

 The assignment of partitions to a tailer can be static (manual assignment) or dynamic using the __subscribe__ API.
 A new tailer that subscribes or terminates will trigger a partition rebalancing between the tailers of the consumer group.

### MQueue Implementations

MQueue is an abstraction on top of two message queue implementations.

#### Chronicle Queue

  [Chronicle Queues](https://github.com/OpenHFT/Chronicle-Queue) is a high performance off-Heap queue implementation.

  Each partition of a MQueue is materialized with a Chronicle Queue.
  There is an additional Chronicle Queue created for each consumer group to persist consumer's offsets.

  This implementation is limited to a single node because the Chronicle Queue can not be distributed
  with the open source version.

  The dynamic assignment is not supported, therefore there is no rebalancing to handle.

  The queues are persisted on disk and a retention policy can be applied to keep only the last `n` cycles.
  (the default retention is to keep the message of the last 4 days).

  There is no replication therefore no fault tolerance. In other word the data directory must be
  backup and you should make sure you never run out of disk.

#### Kafka

  [Kafka](http://kafka.apache.org/) is a distributed streaming app framework. MQueue uses the latest Kafka version 0.11.0.0.

  A MQueue is simply a [topic](http://kafka.apache.org/intro#intro_topics), partitions have the same meaning.

  Appender and tailer use the [Producer](http://kafka.apache.org/documentation.html#producerapi) and [Consumer](http://kafka.apache.org/documentation.html#consumerapi) API of Kafka.

  Offsets are managed manually and persisted in the `__consumer_offsets` internal topic.

  The dynamic assignment is supported and needed to have distributed producers and consumers.

  Kafka brings distributed support and fault tolerance.

  When creating a KafkaMQManager you can use the consumer and producer options as describe in the [Kafka documentation for more information](https://kafka.apache.org/documentation#configuration).

  Here are some important options:


  | Consumer options | default | Description |
  | --- | ---: |  --- |
  | `enable.auto.commit` | `false` | MQueue manages the offset commit this is always set to `false`. |
  | `auto.offset.reset` | `earliest` | This option is always set to `earliest` |
  | `request.timeout.ms` | `30000` | Requests timeout between MQueue and Kafka brokers. |
  | `max.poll.interval.ms` | `300000` | Consumers that don't call poll during this delay are removed from the group. |
  | `session.timeout.ms` | `10000` | Consumers that don't send heartbeat during this delay are removed from the group. |
  | `heartbeat.interval.ms` | `3000` | Interval between heartbeats. |
  | `max.poll.records` | `500` | Can be adjusted to make sure the poll interval is respected. |
  | `group.initial.rebalance.delay.ms` | `3000` | Delay for the initial consumer rebalance. |
  | `subscribe.disable` | `false` | This is a MQueue only option to disable the subscribe mode, When this option is `true` MQueue will only support manual partition assignment. |

  | Producer options | default | Description |
  | --- | ---: |  --- |
  | `default.replication.factor` | `1` | This is a MQueue only option to set the topic replication factor when creating new topic. |



## Producer/Consumer Patterns

MQueue can be used as is and provides benefits of a solid asynchronous message passing system.
 For instance you can impl a work queue or pub/sub on it.

That being said this module comes with battery included for 2 interesting patterns:
- A simple producer/consumer pattern that handle retry and batching
- A computation stream pattern, where we can compose producer/consumer into complex topology

### Simple producer/consumer pattern

#### Queuing with a limited amount of messages

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

#### Queuing unlimited

Almost the same as pattern as above but producers and consumers are always up processing an infinite flow of messages.
There is no Producer interface, a producer just use a MQueue appender to send messages.

The Consumer is driven the same way but its policy is different:

* a consumer will wait forever on new message
* after a failure on the retry policy, the consumer will continue and take the next message
* consumer can be stopped properly using a poison pill message

A producer can wait for a message to be consumed, this can simulate an async call.

See [TestQueuingPattern](https://github.com/nuxeo/nuxeo-mqueues/blob/master/nuxeo-mqueues-core/src/test/java/org/nuxeo/ecm/platform/importer/mqueues/tests/pattern/TestPatternQueuing.java) for basic examples.


### Stream and Computations

This pattern is taken from [Google MillWheel](https://research.google.com/pubs/pub41378.html) and is implemented in [Concord.io](http://concord.io/docs/guides/architecture.html
) and not far from  [Kafka Stream Processor](https://github.com/apache/kafka/blob/trunk/streams/src/main/java/org/apache/kafka/streams/processor/Processor.java).

Instead of message we have record that hold some specific fields like the key and a watermark in addition to the payload.

The key is used to route the record. Records with the same key are always routed to the same computation instance.

The computation is defined almost like in [concord](http://concord.io/docs/guides/concepts.html).

The Topology represent a DAG of computations, that can be executed using a ComputationManager.
Computation read from 0 to n streams and write from 0 to n streams.

Here is an example of the DAG used in UT:

![dag](dag1.png)



#### Computation implementation

A default implementation of Computation is provided based on MQueue, a stream is simply a MQueue of Record.

## Building

To build and run the tests, simply start the Maven build:

    mvn clean install

### Run Unit Tests with Kafka

 Test with kafka implementation rely on an assumption, if the Kafka cluster is not accessible tests are not launched.

 The easiest way to run a Kafka cluster is using [docker-compose](https://docs.docker.com/compose/install/):

	git clone git@github.com:bdelbosc/kafka-docker.git
	cd ./kafka-docker/
    docker-compose up -d
    # to stop
    docker-compose down

### Following Project QA Status
[![Build Status](https://qa.nuxeo.org/jenkins/buildStatus/icon?job=master/addon_nuxeo-mqueues-master)](https://qa.nuxeo.org/jenkins/job/master/job/addon_nuxeo-mqueues-master/)


## About Nuxeo
Nuxeo dramatically improves how content-based applications are built, managed and deployed, making customers more agile, innovative and successful. Nuxeo provides a next generation, enterprise ready platform for building traditional and cutting-edge content oriented applications. Combining a powerful application development environment with SaaS-based tools and a modular architecture, the Nuxeo Platform and Products provide clear business value to some of the most recognizable brands including Verizon, Electronic Arts, Sharp, FICO, the U.S. Navy, and Boeing. Nuxeo is headquartered in New York and Paris. More information is available at www.nuxeo.com.
