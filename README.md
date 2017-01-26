nuxeo-mqueues
===========================

## About

This addon provides a generic message producers/consumers pattern using a set of unbounded persisted queues (nuxeo-mqueues-core).
It comes with specific implementation to produce/consume Nuxeo Document and Nuxeo Blob (nuxeo-mqueues-importer).
This implementation are exposed via automation operation (nuxeo-mqueues-automation).


## Warning

This module is under development and still experimental, interfaces and implementations may change until it is announced as a stable module.


## Building

To build and run the tests, simply start the Maven build:

    mvn clean install

### Following Project QA Status
[![Build Status](https://qa.nuxeo.org/jenkins/buildStatus/icon?job=addons_nuxeo-mqueues-master)](https://qa.nuxeo.org/jenkins/job/addons_nuxeo-mqueues-master/)


## About Nuxeo
Nuxeo dramatically improves how content-based applications are built, managed and deployed, making customers more agile, innovative and successful. Nuxeo provides a next generation, enterprise ready platform for building traditional and cutting-edge content oriented applications. Combining a powerful application development environment with SaaS-based tools and a modular architecture, the Nuxeo Platform and Products provide clear business value to some of the most recognizable brands including Verizon, Electronic Arts, Sharp, FICO, the U.S. Navy, and Boeing. Nuxeo is headquartered in New York and Paris. More information is available at www.nuxeo.com.
