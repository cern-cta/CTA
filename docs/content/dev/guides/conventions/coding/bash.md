# Bash Conventions

Follow [General Coding Conventions](general.md). Prefer short, focused scripts; move complex control flow into a more suitable language when it becomes difficult to maintain in Bash.

## Inputs and execution context

- Executable scripts MUST begin with a shebang; `#!/bin/bash` is recommended. Files intended only to be sourced need not be executable.
- Prefer command-line flags for explicit user inputs. Provide long-form flags, with optional short forms where useful.
- Scripts accepting input MUST provide help through `--help`, which exits successfully, and validate required arguments before performing work.
- Document and validate required commands, environment variables, and working-directory assumptions. Optional environment variables may have documented defaults.
- Resolve repository-owned files relative to the script where practical, using `BASH_SOURCE[0]`, rather than relying on the caller's working directory.

## Commands and error handling

- Quote variable expansions unless splitting or globbing is intentional. Use arrays to preserve command arguments; avoid `eval` and assembling executable commands as strings.
- Return zero on success and non-zero on failure. Send diagnostics to stderr and keep stdout suitable for the command's intended output.
- Prefer `set -euo pipefail` or an explicit error-handling strategy. These options do not replace handling expected failures or checking commands whose status would otherwise be ignored.
- Use securely created temporary files and clean them up, normally with an exit trap. Cleanup should preserve the original failure status where appropriate.
- Explain non-obvious shell behaviour beside the command rather than relying on readers to infer it.

## Validation

- Check changed scripts with [ShellCheck](https://www.shellcheck.net/). The `lint-bash` CI job runs it; it is not currently part of the pre-commit configuration.
- Exercise relevant success and failure paths, including missing or invalid arguments and cleanup after a failure.
