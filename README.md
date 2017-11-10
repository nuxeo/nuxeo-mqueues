nuxeo-mqueues
===========================

## About

  This module implements an asynchronous message passing system called MQueue,
  it comes with different producer/consumer patterns, visit [nuxeo-mqueues-core](./nuxeo-mqueues-core/README.md) for more information.

  The integration with Nuxeo brings reliable mass importer and a scalable workmanager, visit [nuxeo-mqueues-contribs](./nuxeo-mqueues-contribs/README.md) for more information.

## Warning

This module is going to be migrated into nuxeo-runtime/nuxeo-stream and nuxeo-runtime/nuxeo-runtime-stream other contribution will be located appropriatly.
Once this is done this module will be deprecated.
See https://jira.nuxeo.com/browse/NXP-23252 for more information.

## Building

To build and run the tests, simply start the Maven build:

    mvn clean install

### Following Project QA Status

[![Build Status](https://qa.nuxeo.org/jenkins/buildStatus/icon?job=master/addon_nuxeo-mqueues-master)](https://qa.nuxeo.org/jenkins/job/master/job/addon_nuxeo-mqueues-master/)


## About Nuxeo
Nuxeo dramatically improves how content-based applications are built, managed and deployed, making customers more agile, innovative and successful. Nuxeo provides a next generation, enterprise ready platform for building traditional and cutting-edge content oriented applications. Combining a powerful application development environment with SaaS-based tools and a modular architecture, the Nuxeo Platform and Products provide clear business value to some of the most recognizable brands including Verizon, Electronic Arts, Sharp, FICO, the U.S. Navy, and Boeing. Nuxeo is headquartered in New York and Paris. More information is available at www.nuxeo.com.
