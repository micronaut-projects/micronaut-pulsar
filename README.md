# Micronaut Pulsar

[![Maven Central](https://img.shields.io/maven-central/v/io.micronaut.pulsar/micronaut-pulsar.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.micronaut.pulsar%22%20AND%20a:%22micronaut-pulsar%22)
[![Build Status](https://github.com/micronaut-projects/micronaut-pulsar/workflows/Java%20CI/badge.svg)](https://github.com/micronaut-projects/micronaut-pulsar/actions)
[![Revved up by Gradle Enterprise](https://img.shields.io/badge/Revved%20up%20by-Gradle%20Enterprise-06A0CE?logo=Gradle&labelColor=02303A)](https://ge.micronaut.io/scans)

This project includes integration between Apache Pulsar and Micronaut.
## Documentation

See the [Documentation](https://micronaut-projects.github.io/micronaut-pulsar/latest/guide/) for more information. 

See the [Snapshot Documentation](https://micronaut-projects.github.io/micronaut-pulsar/snapshot/guide/) for the current development docs.

## Examples

Examples can be found in the [examples](https://github.com/micronaut-projects/micronaut-pulsar/tree/master/examples) directory.

## Snapshots and Releases

Snaphots are automatically published to [JFrog OSS](https://oss.jfrog.org/artifactory/oss-snapshot-local/) using [Github Actions](https://github.com/micronaut-projects/micronaut-pulsar/actions).

See the documentation in the [Micronaut Docs](https://docs.micronaut.io/latest/guide/index.html#usingsnapshots) for how to configure your build to use snapshots.

Releases are published to JCenter and Maven Central via [Github Actions](https://github.com/micronaut-projects/micronaut-pulsar/actions).

Releases are completely automated. To perform a release use the following steps:

* [Publish the draft release](https://github.com/micronaut-projects/micronaut-pulsar/releases). There should be already a draft release created, edit and publish it. The Git Tag should start with `v`. For example `v1.0.0`.
* [Monitor the Workflow](https://github.com/micronaut-projects/micronaut-pulsar/actions?query=workflow%3ARelease) to check it passed successfully.
* Celebrate!
