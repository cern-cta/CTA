# Metrics

Metrics aggregate runtime measurements such as completed transfers, bytes processed, queue size, or operation duration. CTA uses OpenTelemetry for metrics; the concepts and emitted metric contracts are independent of the implementation language. An instrument defines what is measured; attributes distinguish bounded categories of measurements. Resource attributes identify the service producing them.

## Choose an instrument

| Instrument | Use |
| --- | --- |
| Counter | Accumulate non-negative increments, such as completed operations or transferred bytes. |
| UpDownCounter | Track additive increases and decreases, such as work entering and leaving a queue. |
| Histogram | Record a distribution of observations, such as operation durations. |
| Gauge | Report a current value, such as an independently measured queue depth. |

Choose synchronous recording or an observable callback according to how the measurement is obtained. See [OpenTelemetry metrics](https://opentelemetry.io/docs/concepts/signals/metrics/) for the instrument model.

Before adding a metric, identify the question it answers. Individual file IDs, raw error messages, and other event-specific details usually belong in logs. Review [Metrics Conventions](semantic-conventions.md) before choosing names, units, and attributes.

## Add a metric

1. Define the question the metric answers, its instrument kind, name, description, unit, and bounded attributes. Follow [Metrics Conventions](semantic-conventions.md).
2. Specify exactly what triggers a recording and whether failures and retries are included. For durations, use a monotonic clock and convert to the declared unit.
3. Define and initialize the instrument through the component's instrumentation library. Follow the relevant language guide; [C++ Instrumentation](cpp.md#metrics) documents the current C++ implementation.
4. Record at the intended operation boundary, avoiding duplicate measurements and expensive work in frequently executed paths.
5. [Test the metric](testing.md), including its values, attributes, and exported representation.

Components in different languages should preserve the same metric names, units, attribute meanings, and recording semantics where they measure the same behaviour. SDK-specific registration and lifecycle details belong in the language guides.
