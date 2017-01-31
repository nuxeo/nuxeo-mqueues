nuxeo-mqueues-importer
======================

## About

The mqueues here are used to perform mass import.

The mqueues decouple the Extraction/Transformation from the Load (using the [ETL](https://en.wikipedia.org/wiki/Extract-transform-load) terminology).

The extraction and transformation is done by a document message producer with custom logic.

The load into Nuxeo is done by a generic document message consumer.  

This module provides producers to generate random blobs and document messages.


## Warning

This module is under developpent and still experimental, interfaces and implementations may change until it is announced as a stable module.

## Building

To build and run the tests, simply start the Maven build:

    mvn clean install

### Following Project QA Status

[![Build Status](https://qa.nuxeo.org/jenkins/buildStatus/icon?job=master/addon_nuxeo-mqueues-master)](https://qa.nuxeo.org/jenkins/job/master/job/addon_nuxeo-mqueues-master/)


## About Nuxeo
Nuxeo dramatically improves how content-based applications are built, managed and deployed, making customers more agile, innovative and successful. Nuxeo provides a next generation, enterprise ready platform for building traditional and cutting-edge content oriented applications. Combining a powerful application development environment with SaaS-based tools and a modular architecture, the Nuxeo Platform and Products provide clear business value to some of the most recognizable brands including Verizon, Electronic Arts, Sharp, FICO, the U.S. Navy, and Boeing. Nuxeo is headquartered in New York and Paris. More information is available at www.nuxeo.com.
