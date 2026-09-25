# Bash Conventions

- Scripts SHOULD accept input via command-line flags rather than environment variables.
    - If environment variables are supported, their presence MUST be explicitly validated.
- For each input flag, a script MUST provide a long-form version (e.g., `--image-tag`).
    - A short-form version (e.g., `-t`) MAY be provided when applicable.
- Each script that takes input MUST provide a `usage()` function or equivalent help output accessible via `--help`.
- Scripts MUST begin with a shebang.
    - The form `#!/bin/bash` is RECOMMENDED.
- Scripts MUST validate the presence and correctness of required input arguments.
- Scripts SHOULD follow the single-responsibility principle.
    - Scripts SHOULD be kept short and task-specific.
    - If a script is performing multiple tasks, it is RECOMMENDED to split it into smaller scripts and call them from a wrapper script.
- Scripts MUST NOT assume anything about their execution context.
    - This includes assumptions about the working directory (`pwd`), environment variables, or shell options.
- Scripts MUST return meaningful exit codes:
    - `0` on success.
    - Non-zero on failure.
- Scripts SHOULD handle errors predictably:
    - Exit with a non-zero status code.
    - Provide descriptive error messages.
    - Distinguish between stdout (normal output) and stderr (errors/logging).
- Scripts SHOULD use `set -euo pipefail` (or an equivalent explicit error-handling strategy).
- Scripts SHOULD clean up temporary files.
    - It is RECOMMENDED to enforce cleanup using `trap` to handle script exit.

!!! tip

    - Use tools like [ShellCheck](https://www.shellcheck.net/) to catch potential bugs and improve script quality.
    - Prefer referencing script-relative paths using `$(dirname "${BASH_SOURCE[0]}")` to avoid reliance on the current working directory.
