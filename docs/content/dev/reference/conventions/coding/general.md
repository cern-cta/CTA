# General Coding Conventions

Apply these shared conventions alongside the relevant language guidance:

- [C++ Conventions](cpp.md)
- [Python Conventions](python.md)
- [Bash Conventions](bash.md)
- [Copyright](copyright.md)

## Structure and readability

- Code SHOULD prioritize clarity over cleverness or brevity. Use descriptive names and keep functions focused on a clear responsibility.
- Group related code into discoverable modules and directories. Prefer composition where inheritance adds no useful abstraction.
- Keep variable scope small and avoid unnecessary nesting.
- Follow the repository's formatter and linter configuration. Use [Pre-commit Hooks](../../tools-and-environment/pre-commit.md) rather than maintaining formatting rules manually.
- Document non-obvious decisions, assumptions, and interface contracts. Avoid comments that merely repeat the code.

## Behaviour and ownership

- Make side effects explicit, particularly I/O, changes to shared state, and resource ownership.
- Avoid global mutable state. Justified process-wide services MUST have clear ownership, initialization, and thread-safety expectations.
- Catch exceptions when the code can recover, translate them at an interface boundary, or add useful context. Otherwise, allow them to propagate. Avoid logging the same failure at every layer.
- Prefer clear implementations before optimizing. Base performance changes on measurements and preserve the relevant correctness guarantees.
