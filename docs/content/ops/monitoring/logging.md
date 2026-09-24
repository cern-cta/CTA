# Logging

Logging is a crucial part of CTA that allows operators to monitor and troubleshoot the various CTA components. CTA services can be configured to log to `stdout` or to a file.

For host log directories, packaged rotation policies, reopen signals, and the differences in containers, see [RPM Packages and Services](../deployment/rpm-packages-and-services.md#logging-and-rotation).

## Log Levels

CTA has the following log levels (in order of decreasing severity):

- `EMERG`
- `ALERT`
- `CRIT`
- `ERR`
- `WARNING`
- `NOTICE`
- `INFO`
- `DEBUG`

Each CTA service can be configured to filter out log messages based on the minimum desired log level.

## Log Formats

CTA supports two logging formats:

- key-value (default)
- JSON

It is highly recommended when deploying CTA to configure it to use JSON as this allows for easy parsing by monitoring tools.

## Log Schema

!!! info

    The logging schema is a work in progress. As of the latest CTA release (`5.11.18.0`), only the resource attributes are captured in the schema. In subsequent versions, events will be added. While the log schema version is < 1.0, changes may be frequent.

Since CTA version `5.11.18.0`, all CTA services come bundled with a `cta-logging.schema.json` file. This file follows the [jsonschema](https://json-schema.org/) specifications to define a schema for the log messages. The goal of this schema is to guide operators in what events to monitor and to give them guarantees on the output of CTA to ensure upgrades don't break existing monitoring.

The schema does not fully describe every log message in detail, it only describes the following:

- **Resource Attributes**: attributes present in every single log messages.
- **Events and their attributes**: certain events in CTA and what attributes are present when this event is logged.

Due to the way log context propagation is handled internally in CTA, it is difficult to specify the exact format of each and every log messages. As such, the properties defined in the log schema describe the **minimal** set of attributes you would find for a log message. Additional properties may exist, but if they are not in the schema, they should not be relied upon.
