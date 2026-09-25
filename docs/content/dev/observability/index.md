# Logging and Monitoring

This section explains how developers produce and test signals from CTA services. Collecting logs, deploying monitoring, and responding to alerts belong in [Operations](../../ops/monitoring/health-and-alerts.md).

- [Logging](logging.md): useful events, severity, structured fields, and context.
- [Metrics](metrics.md): instrument selection, attributes, and metric implementation.
- [Health Checks](health-checks.md): readiness and liveness behaviour.
- [Testing](testing.md): validating emitted signals in development and CI.

CTA's metric implementation uses OpenTelemetry; its background and SDK dependencies are described below.

## OpenTelemetry

# OpenTelemetry Introduction

!!! warning

    Note that OpenTelemetry support in CTA is in its early stages and considered experimental. Published metrics and their attributes are likely to change.

Before getting started with OpenTelemetry, we recommend you to read the official OpenTelemetry page: [What is OpenTelemetry](https://opentelemetry.io/docs/what-is-opentelemetry/). This page explains what OpenTelemetry is, what it is for and why it exists.

To use OpenTelemetry in C++, we use the [opentelemetry-cpp](https://github.com/open-telemetry/opentelemetry-cpp) SDK. This client is not provided as an RPM, so we package it ourselves using the [cta-dependencies GitLab repository](https://gitlab.cern.ch/cta/cta-dependencies/) and publish it in the `cta-dependencies` repository for the selected release and platform in the [CTA package repository](https://cta-public-repo.web.cern.ch/).

To read further about OpenTelemetry in CTA, see:

- [OpenTelemetry Metrics in CTA](metrics.md)
- [OpenTelemetry Testing in CI](testing.md)

At the moment we use OpenTelemetry only to produce Metrics. However, other telemetry data such as Tracing and Logs are also part of the OpenTelemetry spec. Support for these might be added in the future.

### References

While we try to give a brief explanation of these concepts when they are used, it is still useful to get a more complete definition and examples from the official documentation. Below are a number of useful links that can help in understanding the various concepts at play in OpenTelemetry. The short definitions below are (mostly) quoted from this documentation.

- [Signals](https://opentelemetry.io/docs/concepts/signals/): system outputs that describe the underlying activity of the operating system and applications running on a platform.
- [Metrics](https://opentelemetry.io/docs/concepts/signals/metrics/): a measurement of a service captured at runtime.
- [Instrumentation](https://opentelemetry.io/docs/concepts/instrumentation/): the process of adding instruments to a system that emit signals
    - [Code-based Instrumentation](https://opentelemetry.io/docs/concepts/instrumentation/code-based/)
    - [Instrumenting Libraries](https://opentelemetry.io/docs/concepts/instrumentation/libraries)
- [Resources](https://opentelemetry.io/docs/concepts/resources/): an entity that produces signals.
- [Semantic Conventions](https://opentelemetry.io/docs/concepts/semantic-conventions/): a set of conventions that specify common names for different kinds of operations and data.
