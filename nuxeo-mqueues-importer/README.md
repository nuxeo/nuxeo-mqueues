nuxeo-mqueues-importer
======================

## About

This module contains Producer/Consumer pattern and Computation integration into Nuxeo.

Producer/consumer pattern are exposed as Nuxeo automation operation and enable to do 
 mass import.
 
A WorkManager implementation based on Computation is also available.

## Warning

This module is under developpent and still experimental, interfaces and implementations may change until it is announced as a stable module.

## Producer/Consumer pattern with automation operations 

The MQueue here are used to perform mass import.

It decouples the Extraction/Transformation from the Load (using the [ETL](https://en.wikipedia.org/wiki/Extract-transform-load) terminology).

The extraction and transformation is done by a document message producer with custom logic, 
this module comes with a random document and blob generator.

The load into Nuxeo is done by a generic document message consumer.  

Automation operations are exposed to run producers and consumers.


### Choose the MQueue implementation

You can use Chronicle or Kafka MQueue implementation.

The default is Chronicle implementation, the `.cq4` files are stored under `${nuxeo.data.dir}/data/mqueue`.
  You can override this default using the `nuxeo.conf` option: `nuxeo.mqueue.chronicle.dir`.

To use Kafka implementation you need to contribute a configuration for instance:

      <?xml version="1.0"?>
      <component name="my.project.kafka.contrib">
      
        <extension target="org.nuxeo.ecm.mqueues.importer.kafka.service" point="kafkaConfig">
          
          <kafkaConfig name="default" zkServers="localhost:2181" topicPrefix="my-app-">
            <producerProperties>
              <property name="bootstrap.servers">localhost:9092</property>
            </producerProperties>
            <consumerProperties>
              <property name="bootstrap.servers">localhost:9092</property>
            </consumerProperties>
          </kafkaConfig>
      
        </extension>
      </component>

Then you can refer to this configuration by passing the configuration name in your
automation call using`"kafkaConfig": "default"` parameter.

### Two steps import: generate and import document with blobs

1. Run producers of document messages (file blob are part of the message)
  ```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runRandomDocumentProducers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"nbDocuments": 100, "nbThreads": 5}}'

Params description:
    "nbDocuments": the number of documents to generate.
    "nbThreads": the number of threads for the producers and for the consumers
    "avgBlobSizeKB": the average blob size fo each file documents.
    "lang": the locale used for the generated document ("fr_FR" or "en_US")
    "mqName": the name of the MQueue
    "kafkaConfig": the name of the Kafka configuration to use
```
2. Run consumers of document messages creating Nuxeo documents, the concurrency will match the previous nbThreads producers parameters
  ```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runDocumentConsumers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"rootFolder": "/default-domain/workspaces"}}'

Params description:
    "rootFolder": the path of the Nuxeo container to import documents, this document must exists.
    "repositoryName": the repository name
    "batchSize": the consumer commit documents every batch size
    "batchThresholdS": the consumer commit documents if the transaction is longer that this threshold
    "retryMax": number of time a consumer retry to import in case of failure
    "retryDelayS": delay between retry
    "mqName": the name of the MQueue (use the same name as in previous runRandoDocumentProducers operation)
    "kafkaConfig": the name of the Kafka configuration to use
```

### 4 steps import: generate and import blobs then generate and import documents

1. Run producers of blob messages
  ```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runRandomBlobProducers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"nbBlobs": 100, "nbThreads": 5}}'

Params description:
    "nbBlobs": the number of blobs to generate
    "nbThreads": the number of threads for the producers and for the consumers
    "avgBlobSizeKB": the average blob size
    "lang": the locale used for the generated blob content ("fr_FR" or "en_US")
    "mqName": the name of the MQueue
    "kafkaConfig": the name of the Kafka configuration to use
```
2. Run consumers of blob messages importing into the Nuxeo binary store.
  ```
mkdir /tmp/a
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runBlobConsumers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"blobProviderName": "default", "blobInfoPath": "/tmp/a"}}'

Params description:
    "blobProviderName": the name of the binary store blob provider
    "blobInfoPath": the path to store blob information csv files, this will be used to link documents with blobs later
    "retryMax": number of time a consumer retry to import the blob
    "retryDelayS": delay between retry
    "mqName": the same name as in the previous operation
    "kafkaConfig": the name of the Kafka configuration to use
```
3. Run producers of document messages which refer to produced blobs created in step 2
  ```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runRandomDocumentProducers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"nbDocuments": 200, "nbThreads": 5, "blobInfoPath": "/tmp/a"}}'
  
Params description:
      "nbDocuments": the number of documents to generate.
      "nbThreads": the number of threads for the producers and for the consumers
      "blobInfoPath": the blob information csv files path generated by runBlobConsumers
      "lang": the locale used for the generated document ("fr_FR" or "en_US")
      "mqName": the name of the MQueue
      "kafkaConfig": the name of the Kafka configuration to use
```
4. Run consumers of document messages
  ```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runDocumentConsumers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"rootFolder": "/default-domain/workspaces"}}'
  
Same params listed in the previous previous runDocumentConsumers call.
```


## WorkManagerComputation implementation

Instead of queueing work into memory or into Redis (which is also in memory), 
you can queue job in a MQueue.

To do so add the following contribution to override the default WorkManagerImpl:

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
