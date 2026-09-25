# General Coding Conventions

Further details can be found in the relevant sections:

- [C++ Conventions](./cpp.md)
- [Bash Conventions](./bash.md)
- [Copyright Conventions](./copyright.md)

## Structure

- In object-oriented languages, one class SHOULD be defined per file.
    - Multiple classes MAY be defined in the same file if they are sufficiently small and tightly coupled, and if this improves readability and maintainability.
- Related classes and files SHOULD be grouped into directories for discoverability.
- Classes and functions SHOULD follow the single-responsibility principle.
- Functions SHOULD be short and focused.

## Behavior

- Functions SHOULD avoid side effects where practical.
- Global mutable state SHOULD be avoided.
    - Global objects that represent process-wide services (e.g., telemetry instruments, logging sinks, registries) MAY be used when justified and well-documented.
    - Any global object MUST have a clear ownership model, a defined initialization order, and documented thread-safety guarantees.
- Code SHOULD prioritize clarity and readability over cleverness or brevity.
- Exceptions SHOULD be handled locally or propagated meaningfully with additional context.
- Variable scope SHOULD be minimized.
- Excessive nesting SHOULD be avoided.

!!! tip

    - Prefer composition over inheritance for flexibility and reusability.
    - Avoid premature optimization; prioritize clarity and maintainability first.
    - Document the *why* of code decisions when they are not obvious.
    - Strive for self-documenting code; use comments to clarify intent or to explain non-trivial algorithms.
