# Python Conventions

Follow [General Coding Conventions](general.md). These conventions apply to Python tools and libraries as well as CI and test code.

## Tooling and compatibility

- Use Ruff for formatting and linting, following `[tool.ruff]` in the root `pyproject.toml`. Run the configured [Pre-commit Hooks](../../tools-and-environment/pre-commit.md) rather than duplicating formatting rules manually.
- Keep code compatible with its supported Python interpreter. The repository Ruff configuration targets Python 3.15, with Python 3.9 overrides for system-integrated helpers and scripts executed in externally maintained images.
- Follow `[tool.pyright]` in the root `pyproject.toml` for type checking; CI runs Pyright. Add type annotations to function interfaces and meaningful data structures, and keep suppressions narrow and explained.

The root `.python-version` pins the Python release used by local uv commands, CI checks and documentation builds.
Package minimum versions and tool targets belong in `pyproject.toml`.

`cta-versionlock` and `cta-udev` must use system Python at `/usr/bin/python3` and remain compatible with Python 3.9 on EL9.
Build/bootstrap helpers and EOS remote scripts also retain their existing system interpreter.

## Code structure and behaviour

- Group related functions and classes into focused modules. Avoid imposing one class per file.
- Keep imports free of operational side effects. Put executable entry-point logic behind `if __name__ == "__main__":` and keep reusable logic callable independently.
- Use context managers to manage files, connections, and other resources that require cleanup.
- Catch specific exceptions when recovery or additional context is useful. Preserve the original cause when translating an exception, and avoid silently swallowing failures.
- Document non-obvious public interfaces and assumptions. Prefer clear names over docstrings that repeat the implementation.

## Tools and tests

- Use a standard argument parser for command-line tools, with useful `--help`, validated inputs, and meaningful exit statuses. Keep machine-readable output separate from diagnostics.
- Invoke subprocesses with argument lists and check their exit status where success is required. Avoid constructing shell commands from input; use a shell only when its behaviour is needed and handle quoting explicitly.
- Declare dependencies with the relevant tool or package rather than relying on incidental packages in a developer's environment.
- Test reusable logic separately from deployed-service workflows where practical. See [Unit Tests](../../testing/unit-tests.md) for Python tooling tests and [System Tests](../../testing/system-tests.md) for tests of deployed CTA instances.
