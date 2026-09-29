# Logging

Logs describe individual events and their context. These message, severity, and attribute conventions apply across component languages. For the current C++ framework, see [C++ Instrumentation](cpp.md#logging).

Backend setup and output formats belong to service configuration; see [Operations Logging](../../../ops/run-and-maintain/monitoring/logging.md).

## Write useful messages

A message should tell a reader what happened and what operation or resource it concerns without requiring them to open the source code. Keep it concise, but retain enough detail to distinguish the event from other activity.

CTA's C++ logging framework records the call site in the `source_location` attribute. Prefixes such as `In <function name>:` repeat information already available there and become misleading when code is refactored. Describe the operation rather than its implementation location.

- Name the action and its subject: for example, “Mounted tape” rather than “Success”. Distinguish a requested or started operation from one that has completed.
- Keep the message stable across occurrences. Put identifiers, counts, durations, and variable error details in structured attributes.
- For a failure, identify the failed operation. Explain its consequence or the next action when known, such as retrying or abandoning the request. Do not claim a retry or recovery unless it will actually happen.
- Avoid vague messages such as “Something went wrong”, function-entry/exit chatter, and restating the same information in both the message and attributes.
- Log successful activity only when it helps explain progress or an outcome. Routine high-frequency successes may be better represented by metrics or a summary.

| Avoid | Prefer | Useful structured context |
| --- | --- | --- |
| `In mountTape(): success` | `Mounted tape` | Tape identifier and drive |
| `In processRequest(): failed` | `Failed to submit archive request` | Request identifier and error details |
| `Error` | `Failed to connect to catalogue; retrying` | Database endpoint, error details, and retry delay, without credentials |
| `Retrieved file 12345 in 2.3 seconds` | `Retrieved file from tape` | File identifier, tape identifier, bytes, and duration |
| `Retrying...` | `Retrying disk-buffer transfer` | Request identifier, attempt number, and reason |
| `Entering checkQueue()` | Usually omit; if diagnostically useful, use `Checking archive queue` at debug severity | Queue identity |

These examples illustrate message wording, not new attribute names. Reuse the established keys for the relevant component. Choose a specific action that matches the code; a generic replacement such as “Operation succeeded” is no improvement.

## Severity and frequency
- Use debug severity for detailed diagnostics, informational severity for useful normal activity, warnings for degraded or unexpected conditions, and errors for failed operations. Reserve critical and emergency severities for service-impacting conditions. Follow the language guide for the API-level mappings, including user errors.
- Avoid repetitive messages in frequently executed paths. Prefer summaries or metrics for repeated activity.
- Log a failure at the boundary where it can be explained or acted upon. Avoid logging and rethrowing the same error through every layer.

## Attributes and context

- Use `snake_case` for new logging attribute names. Reuse established field names and types across component languages; do not rename an existing attribute solely to normalize its spelling. Check the surrounding component's logs before introducing another name for the same concept.
- Include identifiers needed to connect related events, plus useful error context. Unlike metric attributes, log attributes can identify individual requests or files.
- Never log credentials, tokens, or sensitive payloads. Include only the diagnostic information needed for the event.
- Keep structured values typed where supported; do not convert numbers and booleans into formatted prose.
- Preserve the emitted field names and types when changing frameworks or implementation languages. Do not silently rename fields to match a new SDK’s conventions; consumers may depend on them.

## Logging schema and compatibility

`lib/runtime/cta-logging.schema.json` defines the shared log fields and selected named events with their attributes. It is shipped with CTA so operators can build monitoring against an explicit contract. It is not an exhaustive inventory of every contextual attribute: additional fields are allowed, and the required fields for each covered event define its minimum contract. See [Operations: Log Schema](../../../ops/run-and-maintain/monitoring/logging.md#log-schema).

The target naming convention is `snake_case`, but existing attributes may use other styles. Production parsers, dashboards, and alerts can depend on those names. A spelling cleanup is therefore a compatibility change, not merely a refactor. The schema records the contract; changing its version does not automatically migrate monitoring consumers.

When adding or changing attributes:

1. Check the schema, shared constants, and existing consumers before choosing a name. Reuse an existing attribute with the same meaning; use `snake_case` for a genuinely new one.
2. Define the field's type, meaning, and when it is present. For a field or event intended for monitoring, update the schema alongside the emitting code and constants. Do not make an event-specific attribute required on every log entry.
3. Preserve existing names, types, and meanings unless a migration has been agreed with operations. Renaming or removing an attribute, changing its type or unit, or changing an event identifier can break monitoring. Where needed, agree on a transition that supports both old and new consumers before removing the old representation. Check consumers before changing messages they may parse as well.
4. When changing the schema contract, update its `log_schema_version` constant and the emitted `LOG_SCHEMA_VERSION` in `version.cpp.in` together. Describe the compatibility impact and any operator action in the MR and changelog. The schema is currently pre-1.0; this does not remove the need to coordinate production changes.
5. Test representative emitted JSON against the updated schema and verify the affected monitoring with operations when its contract changes. Include checks for required attributes and their types, not just the message text.

## Test logging

Capture logs using the implementation's test facilities. Check meaningful message content, severity filtering, required attributes, and context cleanup. Avoid matching an entire rendered line containing timestamps or source locations. See [C++ logging tests](cpp.md#test-logging) for the current framework's helpers.
