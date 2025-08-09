The ZeroMQ NodeStore
====================

This fork of Apache Jackrabbit Oak is for developing `oak-store-zeromq`,
the ZeroMQ NodeStore. The name stems from the fact that instances of
it communicate by sending and receiving messages via ZeroMQ (or, to be
more precise, jeromq, which is a pure Java implementation of the original).

See https://ahanikel.github.io/jackrabbit-oak/ for a description of its
workings.

Building
--------

Use Java 17 and maven 3.9.11. Other versions may work as well but this is
what worked for me. If you're on a Mac, install maven manually, not via
homebrew (java from homebrew is ok). My homebrew maven kept using the
latest installed java version instead of the one I've configured via
`JAVA_VERSION`.

Then do simply `mvn -DskipTests clean install`

Running
-------

First start the backend jobs: `./start-jobs`
The easiest way to run Sling with the ZeroMQ NodeStore is to modify
the `org-apache-sling-starter` module. Replace the oak version `1.82.0`
with `1.82.0-zeromq` in `pom.xml` and replace segment-tar with
oak-store-zeromq in `src/main/features/oak/persistence/oak_persistence_sns.json` so that it looks like this:

```
{
    "bundles":[
        {
            "id":"org.apache.jackrabbit:oak-store-zeromq:${oak.version}",
            "start-order":"15"
        }
    ],
    "configurations":{
        "org.apache.jackrabbit.oak.store.zeromq.SimpleNodeStore":{
            "name":"Default NodeStore"
        }
    }
}
```

Then create a `start-launcher` script with:

```
rm -rf launcher
mvn clean package
export backendReaderURL=tcp://localhost:8000
export backendWriterURL=tcp://localhost:8001
JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005" target/dependency/org.apache.sling.feature.launcher/bin/launcher -f targe\
t/slingfeature-tmp/feature-oak_tar.json
```

Then monitor `launcher/logs/error.log` to see what's going on.

Enjoy!

Here is the original README:

[![ASF Jira](https://img.shields.io/badge/ASF%20JIRA-OAK-orange)](https://issues.apache.org/jira/projects/OAK/summary)
[![Maven Central](https://img.shields.io/maven-central/v/org.apache.jackrabbit/oak-core.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/org.apache.jackrabbit/oak-core)
[![Build](https://github.com/apache/jackrabbit-oak/actions/workflows/build.yml/badge.svg)](https://github.com/apache/jackrabbit-oak/actions/workflows/build.yml)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=org.apache.jackrabbit%3Ajackrabbit-oak&metric=bugs)](https://sonarcloud.io/summary/new_code?id=org.apache.jackrabbit%3Ajackrabbit-oak)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=org.apache.jackrabbit%3Ajackrabbit-oak&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=org.apache.jackrabbit%3Ajackrabbit-oak)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=org.apache.jackrabbit%3Ajackrabbit-oak&metric=coverage)](https://sonarcloud.io/summary/new_code?id=org.apache.jackrabbit%3Ajackrabbit-oak)

Jackrabbit Oak - the next generation content repository
=======================================================

Jackrabbit Oak is a scalable, high-performance hierarchical content
repository designed for use as the foundation of modern world-class
web sites and other demanding content applications.

The Oak effort is a part of the Apache Jackrabbit project.
Apache Jackrabbit is a project of the Apache Software Foundation.

Getting Started 
---------------

To get started with Oak, build the latest sources with
Maven 3 and Java 11 (or higher) like this:

    mvn clean install

To enable all integration tests, including the JCR TCK, use:

    mvn clean install -PintegrationTesting

Before committing changes or submitting a patch, please make sure that
the above integration testing build passes without errors. If you like,
you can enable integration tests by default by setting the
`OAK_INTEGRATION_TESTING` environment variable.

MongoDB integration
-------------------

Parts of the Oak build expects a MongoDB instance to be available for
testing. By default a MongoDB instance running on localhost is expected,
and the relevant tests are simply skipped if such an instance is not found.
You can also configure the build to use custom MongoDB settings with the
following properties (shown with their default values):

    -Dmongo.host=127.0.0.1
    -Dmongo.port=27017
    -Dmongo.db=MongoMKDB
    -Dmongo.db2=MongoMKDB2

Note that the configured test databases will be *dropped* by the test cases.

Components
----------

The build consists of the following main components:

  - oak-parent        - parent POM
  - oak-doc           - Oak documentation
  - oak-commons       - shared utility code
  - [oak-core][1]     - Oak repository API and implementation
  - oak-jcr           - JCR binding for the Oak repository
  - oak-sling         - integration with Apache Sling
  - oak-http          - HTTP binding for Oak
  - oak-lucene        - Lucene-based query index
  - oak-run           - runnable jar packaging
  - oak-pojosr        - integration with PojoSR
  - oak-segment-tar   - TarMK API and implementation
  - oak-upgrade       - tooling for upgrading Jackrabbit repositories to Oak
  - oak-it            - integration tests
    - oak-it/osgi     - integration tests for OSGi
  - [oak-exercise][2] - Oak training material

  [1]: oak-api/README.md
  [2]: oak-exercise/README.md

Archive
-------

The following components have been moved to the Jackrabbit Attic:

  - oak-mk-api        - MicroKernel API (_deprecated_, see OAK-2701)
  - oak-mk            - MicroKernel implementation  (see OAK-2702)
  - oak-mk-remote     - MicroKernel remoting  (see OAK-2693)
  - oak-it/mk         - integration tests for MicroKernel

License
-------

(see [LICENSE.txt](LICENSE.txt) for full license details)

Collective work: Copyright 2014 The Apache Software Foundation.

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
