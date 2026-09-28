# Pre-commit Hooks

Pre-commit runs local checks before a commit. For installation, see [Enable pre-commit](../../getting-started/prerequisites.md#enable-pre-commit). These checks complement GitLab CI; they do not replace it.

## Configured checks

The repository's `.pre-commit-config.yaml` defines the hooks, versions, and file selection.

| Hook | Purpose |
| --- | --- |
| `detect-secrets` | Detect potential secrets using `.secrets.baseline`. |
| `clang-format` | Format selected C/C++ and JSON files. |
| `ruff-check`, `ruff-format` | Check and format Python; the check hook also applies automatic fixes. |
| `yamllint` | Check YAML formatting and syntax. |
| `reuse-lint-file` | Check file copyright and licence metadata; see [Copyright](../conventions/coding/copyright.md). |
| `gitlab-ci` | Check merge-train guards, key ordering, and stage organization in CI configuration. |

## Run checks manually

Run from the repository root. If you used the dedicated environment from the setup guide, activate it first:

```bash
source ~/.local/share/cta-pre-commit/bin/activate
```

```bash
# Check staged files.
pre-commit run

# Run one hook on selected files.
pre-commit run ruff-check --files path/to/file.py

# Check all tracked files, for example after changing hook configuration.
pre-commit run --all-files
```

Formatters can modify files, including during an all-files run. Review their changes, stage the intended fixes, and retry. A hook reporting “no files to check” is normal when no selected files match it. See [pre-commit usage](https://pre-commit.com/#usage).

## Troubleshooting

| Problem | Action |
| --- | --- |
| Command or hook interpreter is missing | Restore the Python environment used during installation, then rerun `pre-commit install` in the checkout. |
| Hook reports an error | Follow its file and line diagnostics. Automatic fixes may leave findings that need manual changes. |
| Hook environment fails to initialize | Read the installation error for missing dependencies or download failures. If the cached environment is broken, run `pre-commit clean`, then retry. |
| Automatic fixes conflict with unstaged changes | Separate or stage the intended edits before retrying; inspect the working tree first. |

Pinned hook environments are installed automatically when needed. `pre-commit autoupdate` changes the repository configuration; use it for a deliberate hook-version update, not routine troubleshooting. See the [command reference](https://pre-commit.com/#command-line-interface).

## Secret-detection findings

Inspect each reported location. For a real credential, remove it and use the appropriate configuration or secret mechanism. If it was exposed in a commit or shared elsewhere, arrange revocation or rotation with its owner.

After carefully reviewing the findings and confirming that any values to be retained are false positives or safe test fixtures, regenerate the baseline from the repository root:

```bash
detect-secrets scan --baseline .secrets.baseline
```

Use the `detect-secrets` version pinned in `.pre-commit-config.yaml`. This updates the baseline file without manually editing its entries; the scan itself does not establish that a finding is safe. Review the resulting diff, rerun the hook, and include the baseline change in the MR with an explanation of the accepted findings.

The [detect-secrets guide](https://github.com/Yelp/detect-secrets#usage) describes baseline updates and auditing.
