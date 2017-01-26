nuxeo-mqueues-automation
=========================

## About

This module expose automation operations to run Nuxeo document and blob producers/consumers.


## Warning

This module is under developpent and still experimental, interfaces and implementations may change until it is announced as a stable module.


## Testing automation operations with curl

### Basic import

1. Run producers of document messages (file blob are part of the message)
```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runRandomDocumentProducers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"nbDocuments": 100, "nbThreads": 5}}'

Other params:
"avgBlobSizeKB": 1,
"lang": "fr_FR"
"queuePath": "/tmp/mq-doc"
```

2. Run consumers of document messages creating Nuxeo documents, the concurrency will match the previous nbThreads producers parameters
```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runDocumentConsumers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"rootFolder": "/default-domain/workspaces"}}'

Other options
"repositoryName": "",
"batchSize": 10,
"batchThresholdS": 20,
"retryMax": 3,
"retryDelayS": 2,
"queuePath": "/tmp/mq-doc"
```

### Import blobs then documents

1. Run producers of blob messages
```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runRandomBlobProducers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"nbBlobs": 100, "nbThreads": 5}}'

Other options:
"avgBlobSizeKB": 2,
"lang": "fr_FR",
"queuePath": "/tmp/mq-blob",
```

2. Run consumers of blob messages importing into the Nuxeo binary store.
```
mkdir /tmp/a
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runBlobConsumers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"blobProviderName": "default", "blobInfoPath": "/tmp/a"}}'

Other options:
"batchSize": 10,
"batchThresholdS": 20,
"retryMax": 3,
"retryDelayS": 2,
"queuePath": "/tmp/mq-blob"
```

3. Run producers of document messages which refer to produced blobs created in step 2
```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runRandomDocumentProducers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"nbDocuments": 200, "nbThreads": 5, "blobInfoPath": "/tmp/a"}}'
```

4. Run consumers of document messages
```
curl -X POST 'http://localhost:8080/nuxeo/site/automation/MQImporter.runDocumentConsumers' -u Administrator:Administrator -H 'content-type: application/json+nxrequest' \
  -d '{"params":{"rootFolder": "/default-domain/workspaces"}}'
```

## Building

To build and run the tests, simply start the Maven build:

    mvn clean install

### Following Project QA Status
[![Build Status](https://qa.nuxeo.org/jenkins/buildStatus/icon?job=addons_nuxeo-mqueues-master)](https://qa.nuxeo.org/jenkins/job/addons_nuxeo-mqueues-master/)


## About Nuxeo
Nuxeo dramatically improves how content-based applications are built, managed and deployed, making customers more agile, innovative and successful. Nuxeo provides a next generation, enterprise ready platform for building traditional and cutting-edge content oriented applications. Combining a powerful application development environment with SaaS-based tools and a modular architecture, the Nuxeo Platform and Products provide clear business value to some of the most recognizable brands including Verizon, Electronic Arts, Sharp, FICO, the U.S. Navy, and Boeing. Nuxeo is headquartered in New York and Paris. More information is available at www.nuxeo.com.
