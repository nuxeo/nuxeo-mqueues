nuxeo-mqueues-contribs
======================

## About

This module provides contributions to Nuxeo platform using MQueue:

- You can register Kafka access and properties via Nuxeo contribution.
- A Nuxeo service that takes care of initialize MQManager, creates MQueue, starts computations topologies.
- The producer/consumer pattern is adapted to do document mass import, it is exposed as automation operations.
- Computations are used to provide an alternative WorkManager implementation.
- Computations are also used to replace the Audit writer.

## Warning

This module is under development and still experimental, interfaces and implementations may change until it is announced as a stable module.

## Configurations

### Kafka

 You can register one or multiple Kafka configuration using this Nuxeo extention point:

```
<?xml version="1.0"?>
<component name="my.project.kafka.contrib">

  <extension target="org.nuxeo.ecm.platform.mqueues.kafka.service" point="kafkaConfig">

    <kafkaConfig name="default" zkServers="localhost:2181" topicPrefix="nuxeo-">
      <producerProperties>
        <property name="bootstrap.servers">localhost:9092</property>
      </producerProperties>
      <consumerProperties>
        <property name="bootstrap.servers">localhost:9092</property>
        <property name="request.timeout.ms">65000</property>
        <property name="max.poll.interval.ms">60000</property>
        <property name="session.timeout.ms">20000</property>
        <property name="heartbeat.interval.ms">1000</property>
        <property name="max.poll.records">50</property>
      </consumerProperties>
    </kafkaConfig>

  </extension>
</component>
```

The kafka configuration named `default` can be used in the MQueue configuration below.

Make sure you have read the [nuxeo-mqueues-core README](../nuxeo-mqueues-core/README.md) to setup properly Kafka.


### MQueues

You can define multiple MQueue configurations.

There are 2 types of configurations:

- Chronicle: limited for single node, all producers and consumers are on the same server.
- Kafka: required for distributed producers and consumers.

You can configure the MQ configuration with the following Nuxeo extention point:

```
<?xml version="1.0"?>
<component name="my.project.mqueue.contrib">

  <extension target="org.nuxeo.ecm.platform.mqueues.service" point="config">

    <!-- Chronicle impl, storage ${nuxeo.data.dir}/data/mqueue/default, 4 days of retention -->
    <config name="default" />

    <!-- Chronicle impl, storage ${nuxeo.data.dir}/data/mqueue/custom, 4 days of retention,
         create a MQueue named aqueue with 5 partitions if it does not exist. -->
    <config name="custom">
      <mqueue name="aqueue" size="5" />
    </config>

    <!-- Chronicle impl, storage in /tmp/imp, a week of retention -->
    <config name="import" type="chronicle">
      <option name="directory">imp</option>
      <option name="basePath">/tmp</option>
      <option name="retention">7d</option>
    </config>

    <!-- Kafka impl, referencing the default Kafka config -->
    <config name="work" type="kafka">
      <option name="config">default</option>
    </config>

  </extension>

  <extension target="org.nuxeo.ecm.platform.mqueues.service" point="topology">

    <!-- Start computations once Nuxeo is fully started, the class provides the topology and the EP the settings -->
    <topology name="myComputation" config="default" defaultConcurrency="4" defaultPartitions="4"
      class="org.nuxeo.ecm.platform.mqueues.tests.MyComputationTopology">
      <computation name="some-computation" concurrency="10" />
      <stream name="some-stream" partitions="10" />
    </topology>

  </extension>

</component>
```

By default the Chronicle Queue storage is located in the Nuxeo data directory: `${nuxeo.data.dir}/data/mqueue`.
This path can be changed using the `nuxeo.conf` option: `nuxeo.mqueue.chronicle.dir`.

The default retention for Chronicle implementation is four days. This can be changed using the `nuxeo.conf` option: `nuxeo.mqueue.chronicle.retention.duration`,
the value is expressed as a string like: `12h` or `7d`, respectively for 12 hours and 7 days.



### Get a MQManager from Nuxeo

The Nuxeo service MQService enables to get and share access to MQManager:

```
        MQService service = Framework.getService(MQService.class);
        // Get a MQManager with the 'default' configuration
        MQManager manager = service.getManager("default");
        // append
        try (MQAppender<Record> appender = manager.getAppender(mqName)) {
            appender.append(key, Record.of(key, value.getBytes()));
        }
        // or tail
        try (MQTailer<Record> tailer = manager.createTailer("myGroup", mqName)) {
            MQRecord<Record> mqRecord = tailer.read(Duration.ofSeconds(1));
            assertEquals(key, mqRecord.message().key);
        }
        // don't close the manager, this is done by the service
```


## Producer/Consumer pattern with automation operations

The MQueue here are used to perform mass import.

It decouples the Extraction/Transformation from the Load (using the [ETL](https://en.wikipedia.org/wiki/Extract-transform-load) terminology).

The extraction and transformation is done by a document message producer with custom logic,
this module comes with a random document and a random blob generator.

The load into Nuxeo is done with a generic consumer.

Automation operations are exposed to run producers and consumers.


### Two steps import: generate and import document with blobs

1. Run a random producers of document messages, these message represent Folder and File document a blob. The total number of document created is: `nbThreads * nbDocuments`.
  ```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runRandomDocumentProducers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"nbDocuments": 100, "nbThreads": 5}}'
```

| Params| Default | Description |
| --- | ---: | --- |
| `nbDocuments` |  | The number of documents to generate per producer thread |
| `nbThreads` | `8` | The number of concurrent producer to run |
| `avgBlobSizeKB` | `1` | The average blob size fo each file documents in KB. If set to `0` create File document without blob. |
| `lang` | `en_US` |The locale used for the generated content, can be `fr_FR` or `en_US` |
| `mqName` | `mq-doc` | The name of the MQueue. |
| `mqSize` | `$nbThreads` |The size of the MQueue which will fix the maximum number of consumer threads |
| `mqBlobInfo` |  | A MQueue containing blob information to use, see section below for use case |
| `mqConfig` | `import` | The MQ configuration name |

2. Run consumers of document messages creating Nuxeo documents, the concurrency will match the previous nbThreads producers parameters
  ```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runDocumentConsumers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"rootFolder": "/default-domain/workspaces"}}'
```

| Params| Default | Description |
| --- | ---: | --- |
| `rootFolder` |  | The path of the Nuxeo container to import documents, this document must exists |
| `repositoryName` |  | The repository name used to import documents |
| `nbThreads` | `mqSize` | The number of concurrent consumer, should not be greater than the mqSize |
| `batchSize` | `10` | The consumer commit documents every batch size |
| `batchThresholdS` | `20` | The consumer commit documents if the transaction is longer that this threshold |
| `retryMax` | `3` | Number of time a consumer retry to import in case of failure |
| `retryDelayS` | `2` | Delay between retries |
| `mqName` | `mq-doc` | The name of the MQueue to tail |
| `mqConfig` | `import` | The MQ configuration name |
| `useBulkMode` | `false` | Process asynchronous listeners in bulk mode |
| `blockIndexing` | `false` | Do not index created document with Elasticsearch |
| `blockAsyncListeners` | `false` | Do not process any asynchronous listeners |
| `blockPostCommitListeners` | `false` | Do not process any post commit listeners |
| `blockDefaultSyncListeners` | `false` | Disable some default synchronous listeners: dublincore, mimetype, notification, template, binarymetadata and uid |

### 4 steps import: generate and import blobs then generate and import documents

1. Run producers of random blob messages
  ```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runRandomBlobProducers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"nbBlobs": 100, "nbThreads": 5}}'
```

| Params| Default | Description |
| --- | ---: | --- |
| `nbBlobs` |  | The number of blobs to generate per producer thread |
| `nbThreads` | `8` | The number of concurrent producer to run |
| `avgBlobSizeKB` | `1` | The average blob size fo each file documents in KB |
| `lang` | `en_US` | The locale used for the generated content, can be "fr_FR" or "en_US" |
| `mqName` | `mq-blob` |  The name of the MQueue to store blobs. |
| `mqSize` | `$nbThreads`| The size of the MQueue which will fix the maximum number of consumer threads |
| `mqConfig` | `import` | The MQ configuration name |

2. Run consumers of blob messages importing into the Nuxeo binary store, saving blob information into a new MQueue.
  ```
mkdir /tmp/a
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runBlobConsumers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"blobProviderName": "default", "mqBlobInfo": "mq-blob-info"}}'
```

| Params| Default | Description |
| --- | ---: | --- |
| `blobProviderName` | `default` | The name of the binary store blob provider |
| `mqName` | `mq-blob` | The name of the MQueue that contains the blob |
| `mqBlobInfo` | `mq-blob-info` | The name of the MQueue to store blob information about imported blobs |
| `nbThreads` | `$mqSize` | The number of concurrent consumer, should not be greater than the mqSize |
| `retryMax` | `3` | Number of time a consumer retry to import in case of failure |
| `retryDelayS` | `2` | Delay between retries |
| `mqConfig` | `import` | The MQ configuration name |

3. Run producers of random Nuxeo document messages which use produced blobs created in step 2
  ```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runRandomDocumentProducers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"nbDocuments": 200, "nbThreads": 5, "mqBlobInfo": "mq-blob-info"}}'
```
Same params listed in the previous previous runRandomDocumentProducers call, here we set the `mqBlobInfo` parameter.

4. Run consumers of document messages
  ```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runDocumentConsumers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"rootFolder": "/default-domain/workspaces"}}'
```

Same params listed in the previous previous runDocumentConsumers call.

## WorkManagerComputation implementation

Instead of queueing work into memory or into Redis (which is also in memory),
you can queue job in a MQueue without worries about the memory limits.

To do so, add the following contribution to override the default WorkManagerImpl:

```
<?xml version="1.0"?>
<component name="my.project.work.service" version="1.0">

  <require>org.nuxeo.ecm.core.work.service</require>

  <service>
    <provide interface="org.nuxeo.ecm.core.work.api.WorkManager" />
  </service>

  <implementation class="org.nuxeo.ecm.platform.mqueues.workmanager.WorkManagerComputation" />

  <extension-point name="queues">
    <object class="org.nuxeo.ecm.core.work.api.WorkQueueDescriptor" />
  </extension-point>

</component>
```

The MQ configuration used by default is named `work`, this can be configured using
the `nuxeo.conf` option: `nuxeo.mqueue.work.config`.


The goal when using Kafka is to scale horizontally, so that adding a Nuxeo node supports more load.
To do so the number of partitions that fix the maximum concurrency must be greater than
the thread pool size of a single node. This strategy is called partition over provisioning.

By default there is an over provisioning factor of `3`. For instance for a work pool of size 4,
we have 12 partitions in the MQueue:
- With a single node we have 4 threads, each reading from 3 partitions.
- With 2 nodes we have 8 threads some reading from 2 or 1 partitions.
- With 3 nodes we reach the maximum concurrency of 12 threads, each thread reading from one partition.
- With more than 3 nodes some threads in the work pool will be unused, reducing the overall node load.

Note that work pool of size `1` are not over provisioned because we don't want any concurrency.

You can change the over provisioning factor using the `nuxeo.conf` option: `nuxeo.mqueue.work.over.provisioning`.
If the topics where already created you must delete them before or change the size of topic using Kafka tools.



### Limitations

The computation WorkManager API is more limited than the default implementation because it is not possible
to access submitted work. For instance it is not possible to list scheduled works or get the state of an existing work.
Invoking those methods will return no result.

This is mainly for performance reason, such kind of access find/list on a queue storage does not scale.
This should not be a problem because accessing completed work to get a result is not a way to go,
result can be persisted in a transient store or work chained using computations.

The work pool metrics are still available and reflect the total number of scheduled, running, completed or canceled works. Note that at the
moment the number of running works is just an estimation and don't reflect the exact number of concurrent running worker.

## Audit writer

The default audit writer has some drawback because it works as a post commit listener, it generates too much overhead.

The implementation with computation is a simple synchronous listener that collect events and save log entries as JSON into a stream.

A simple computation is processing the stream and performs the batching to submit log entries to the audit backend.

This improves the performance by not having to reconnect bundle event using new session and threads, but it also brings
reliability because events are persisted first into a MQueue. So even
if the audit backend is not reachable the application is not affected and we don't loose any log.

See the `test-audit-contrib.xml` file to activate this implementation.

## Building

To build and run the tests, simply start the Maven build:

    mvn clean install

### Following Project QA Status

[![Build Status](https://qa.nuxeo.org/jenkins/buildStatus/icon?job=master/addon_nuxeo-mqueues-master)](https://qa.nuxeo.org/jenkins/job/master/job/addon_nuxeo-mqueues-master/)


## About Nuxeo
Nuxeo dramatically improves how content-based applications are built, managed and deployed, making customers more agile, innovative and successful. Nuxeo provides a next generation, enterprise ready platform for building traditional and cutting-edge content oriented applications. Combining a powerful application development environment with SaaS-based tools and a modular architecture, the Nuxeo Platform and Products provide clear business value to some of the most recognizable brands including Verizon, Electronic Arts, Sharp, FICO, the U.S. Navy, and Boeing. Nuxeo is headquartered in New York and Paris. More information is available at www.nuxeo.com.
