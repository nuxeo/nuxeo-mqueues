nuxeo-mqueues-importer
======================

## About

This module provides integration of MQueue with Nuxeo:

- Define Kafka access via Nuxeo contribution.
- The producer/consumer pattern is adapted to do mass import document via automation.
- Computations are used to provide an alternative WorkManager implementation.

## Warning

This module is under development and still experimental, interfaces and implementations may change until it is announced as a stable module.

## Choosing the MQueue implementation

You can use Chronicle or Kafka MQueue implementation.

Chronicle implementation is limited for single node (all producers and consumers are on the same server), 
while distributed nodes requires the Kafka implementation.

The default MQueue implementation is using Chronicle Queue.

### The Chronicle implementation

A partition is materialized by a Chronicle Queue which is a `.cq4` file. 
A MQueue is a directory layout that contains multiple `.cq4` files. 

By default they are stored in Nuxeo: `${nuxeo.data.dir}/data/mqueue`.

This path can be changed using the `nuxeo.conf` option: `nuxeo.mqueue.chronicle.dir`.

For instance by default the automation import will use a MQueue named `mq-doc` with 5 partitions, the file layout is:

    nxserver/data/mqueue/
    └── import
        └── mq-doc
            ├── Q-00
            │   └── 20170616.cq4
            ├── Q-01
            │   └── 20170616.cq4
            ├── Q-02
            │   └── 20170616.cq4
            ├── Q-03
            │   └── 20170616.cq4
            └── Q-04
                └── 20170616.cq4

 

### Kafka implementation
 
 To use the Kafka implementation you need to register Kafka configuration.


      <?xml version="1.0"?>
      <component name="my.project.kafka.contrib">
      
        <extension target="org.nuxeo.ecm.mqueues.importer.kafka.service" point="kafkaConfig">
          
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

 Then you can refer to this configuration `default` to force MQueue to use the Kafka implementation.


## Producer/Consumer pattern with automation operations 

The MQueue here are used to perform mass import.

It decouples the Extraction/Transformation from the Load (using the [ETL](https://en.wikipedia.org/wiki/Extract-transform-load) terminology).

The extraction and transformation is done by a document message producer with custom logic, 
this module comes with a random document and a random blob generator.

The load into Nuxeo is done with a generic consumer.  

Automation operations are exposed to run producers and consumers.


### Two steps import: generate and import document with blobs

1. Run producers of document messages (file blob are part of the message)
  ```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runRandomDocumentProducers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"nbDocuments": 100, "nbThreads": 5}}'
```

| Params| Description |
| --- | --- |
| `nbDocuments` | The number of documents to generate per producer thread |
| `nbThreads` | The number of concurrent producer to run |
| `avgBlobSizeKB` | The average blob size fo each file documents in KB |
| `lang` | The locale used for the generated content, can be `fr_FR` or `en_US` |
| `mqName` |  The name of the MQueue|
| `mqSize` | The size of the MQueue which will fix the maximum number of consumer threads |
| `kafkaConfig` | Choose the Kakfka implementation, use the name of a registered Kafka configuration |

2. Run consumers of document messages creating Nuxeo documents, the concurrency will match the previous nbThreads producers parameters
  ```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runDocumentConsumers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"rootFolder": "/default-domain/workspaces"}}'
```

| Params| Description |
| --- | --- |
| `rootFolder` | The path of the Nuxeo container to import documents, this document must exists |
| `repositoryName` | The repository name used to import documents | 
| `nbThreads` | The number of concurrent consumer, should not be greater than the mqSize |
| `batchSize` | The consumer commit documents every batch size |
| `batchThresholdS` | The consumer commit documents if the transaction is longer that this threshold |
| `retryMax` | Number of time a consumer retry to import in case of failure |
| `retryDelayS` | Delay between retries |
| `mqName` | The name of the MQueue to tail |
| `kafkaConfig` | Choose the Kakfka implementation, use the name of a registered Kafka configuration |
| `useBulkMode` | Process asynchronous listeners in bulk mode |
| `blockIndexing` | Do not index created document with Elasticsearch|
| `blockAsyncListeners` | Do not process any asynchronous listeners|
| `blockPostCommitListeners` | Do not process any post commit listeners |
| `blockDefaultSyncListeners` | Disable some default synchronous listeners: dublincore, mimetype, notification, template, binarymetadata and uid |

### 4 steps import: generate and import blobs then generate and import documents

1. Run producers of blob messages
  ```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runRandomBlobProducers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"nbBlobs": 100, "nbThreads": 5}}'
```

| Params| Description |
| --- | --- |
| `nbBlobs` | The number of blobs to generate per producer thread |
| `nbThreads` | The number of concurrent producer to run |
| `avgBlobSizeKB` | The average blob size fo each file documents in KB |
| `lang` | The locale used for the generated content, can be "fr_FR" or "en_US" |
| `mqName` |  The name of the MQueue|
| `mqSize` | The size of the MQueue which will fix the maximum number of consumer threads |
| `kafkaConfig` | Choose the Kakfka implementation, use the name of a registered Kafka configuration |

2. Run consumers of blob messages importing into the Nuxeo binary store.
  ```
mkdir /tmp/a
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runBlobConsumers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"blobProviderName": "default", "blobInfoPath": "/tmp/a"}}'
```

| Params| Description |
| --- | --- |
| `blobProviderName` | The name of the binary store blob provider |
| `blobInfoPath` | The path to store blob information csv files, this will be used to link documents with blobs later |
| `nbThreads` | The number of concurrent consumer, should not be greater than the mqSize |
| `retryMax` | Number of time a consumer retry to import in case of failure |
| `retryDelayS` | Delay between retries |
| `mqName` | The name of the MQueue to tail |
| `kafkaConfig` | Choose the Kakfka implementation, use the name of a registered Kafka configuration |

3. Run producers of document messages which refer to produced blobs created in step 2
  ```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runRandomDocumentProducers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"nbDocuments": 200, "nbThreads": 5, "blobInfoPath": "/tmp/a"}}'
```
| Params| Description |
| --- | --- |
| `nbDocuments` | The number of documents to generate per producer thread |
| `nbThreads` | The number of concurrent producer to run |
| `blobInfoPath` | The blob information csv files path generated by runBlobConsumers |
| `lang` | The locale used for the generated content, can be "fr_FR" or "en_US" |
| `mqName` |  The name of the MQueue|
| `mqSize` | The size of the MQueue which will fix the maximum number of consumer threads |
| `kafkaConfig` | Choose the Kakfka implementation, use the name of a registered Kafka configuration |

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

    <?xml version="1.0"?>
    <component name="my.project.work.service" version="1.0">
    
      <require>org.nuxeo.ecm.core.work.service</require>
    
      <service>
        <provide interface="org.nuxeo.ecm.core.work.api.WorkManager" />
      </service>
    
      <implementation class="org.nuxeo.ecm.platform.importer.mqueues.workmanager.WorkManagerComputationChronicle" />
      <!-- implementation class="org.nuxeo.ecm.platform.importer.mqueues.workmanager.WorkManagerComputationKafka" /-->
    
      <extension-point name="queues">
        <object class="org.nuxeo.ecm.core.work.api.WorkQueueDescriptor" />
      </extension-point>
    
    </component>


When using the Kafka implementation you need to contribute a configuration (see above).

The Kafka default configuration used is named "default", you can choose another one using
using the `nuxeo.conf` option: `nuxeo.mqueue.work.kafka.config`.

## Building

To build and run the tests, simply start the Maven build:

    mvn clean install

### Following Project QA Status

[![Build Status](https://qa.nuxeo.org/jenkins/buildStatus/icon?job=master/addon_nuxeo-mqueues-master)](https://qa.nuxeo.org/jenkins/job/master/job/addon_nuxeo-mqueues-master/)


## About Nuxeo
Nuxeo dramatically improves how content-based applications are built, managed and deployed, making customers more agile, innovative and successful. Nuxeo provides a next generation, enterprise ready platform for building traditional and cutting-edge content oriented applications. Combining a powerful application development environment with SaaS-based tools and a modular architecture, the Nuxeo Platform and Products provide clear business value to some of the most recognizable brands including Verizon, Electronic Arts, Sharp, FICO, the U.S. Navy, and Boeing. Nuxeo is headquartered in New York and Paris. More information is available at www.nuxeo.com.
