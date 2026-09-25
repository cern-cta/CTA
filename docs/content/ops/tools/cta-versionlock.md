# `cta-versionlock`

Inspect and manage RPM dependency version locks supplied by `cta-release`.

## Usage

```text
cta-versionlock [options] action
```

Use `--help` for options and paths. The default files are `/etc/yum/pluginconf.d/versionlock.cta` and `/etc/yum/pluginconf.d/versionlock.list`.

| Action | Purpose |
| --- | --- |
| `list-cta` | List CTA's supplied version locks. |
| `list-yum` | List the host's version locks. |
| `compare` | Compare the two sets of locks. |
| `apply` | Add CTA's supplied locks to the host file. |
| `remove` | Remove CTA's locks; this does not restore the previous host file. |
| `check-installed` | Check installed packages against the host's locks. |

See [RPM installation](../deployment/installation/rpm-packages.md#installation) for the deployment workflow.
