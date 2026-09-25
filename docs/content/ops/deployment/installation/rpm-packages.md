# RPM Packages

Install CTA from RPM repositories and manage the packaged services, configuration files, and host integration.

## Installation

**1. Set up the repository for the selected CTA release:**

Operators can choose between a *stable* or a *testing* channel for CTA:

- *stable*:
    - These releases have been deployed at CERN *in production* successfully.
- *testing*:
    - Updated more frequently, with more recent releases.
    - They are expected to work (CI testing was successful) but have not been deployed in production at CERN.

Use the repository URL for your chosen channel, CTA release series, platform, and architecture from the [CTA package repository](https://cta-public-repo.web.cern.ch/). The following is a template; replace `REPOSITORY_URL` before use:

```ini
[cta]
name=CERN Tape Archive
baseurl=REPOSITORY_URL
enabled=1
gpgcheck=0
```

Save the completed repository definition under `/etc/yum.repos.d/`.

**2. Search and/or install *cta-release*:**

The `cta-release` package will install the needed `.repo` files and the `cta-versionlock` tool (see next step).

```shell
dnf search cta-release --showduplicates
dnf install cta-release-<cta_version>.el9.x86_64
```

or, to get the latest version:

```shell
dnf install cta-release
```

**3. Configure CTA repositories and dependencies:**

The `cta-versionlock` tool will update the versionlock file on the machine to lock CTA and all its dependencies to the right versions.

```shell
cta-versionlock apply
```

**4. Install additional repositories**

`epel-release` repository is needed before going further.

```
dnf install epel-release
```

**5. Install CTA:**

By default, CTA only comes with Postgres support for the catalogue. To enable Oracle support, you will have to explicitly install `cta-lib-catalogue-occi`. See section below.

Choose the appropriate packages, as needed.

```shell
dnf install cta-frontend # For the disk buffer + admin commands
dnf install cta-maintd # For executing various routines necessary for the correct working of CTA
dnf install cta-taped # For the drive. Must be on a server connected to tape drives
dnf install cta-rmcd # For the mediachanger. Must be on every server where a cta-taped process is running
```

### Note on Catalogue Database Support

CTA supports multiple database backends for the catalogue: Postgres and Oracle. Thanks to a plugin system, CTA only needs one of these two RPMs installed, depending on which backend is used:

- `cta-lib-catalogue-occi`
- `cta-lib-catalogue-postgres`

By default, installing any CTA package requiring `cta-lib-catalogue` will only install `cta-lib-catalogue-postgres`.

- That means: `dnf install cta-taped` will bring in `cta-lib-catalogue-postgres`, but **not** `cta-lib-catalogue-occi`

If you are using Oracle for the catalogue, you will have to explicitly install `cta-lib-catalogue-occi`

- In this case: `dnf install cta-lib-catalogue-occi cta-taped` will not bring in `cta-lib-catalogue-postgres`

In general, CTA will be happily installed as long as at least one of `cta-lib-catalogue-postgres` or `cta-lib-catalogue-occi` is present.

## Installed files

The exact contents depend on the selected CTA package. The following locations form the common packaging contract; they are not an exhaustive file inventory.

| Content | Standard location | Ownership and purpose |
| --- | --- | --- |
| Executables | `/usr/bin` | Programs installed by the RPM |
| Service environment | `/etc/sysconfig/cta-*` | Administrator-managed environment for units that use a sysconfig file |
| Systemd vendor units | `/usr/lib/systemd/system` | Unit definitions supplied by CTA RPMs |
| Logrotate policies | `/etc/logrotate.d` | Host log-rotation integration supplied by the relevant RPM |
| Persistent logs | `/var/log/cta` | Created for systemd services through `LogsDirectory=` |
| Runtime data | `/run/cta` | Ephemeral state created for applicable services through `RuntimeDirectory=` |
| Manual pages | `/usr/share/man` | Command documentation, possibly compressed by the build system |
| Licenses | `/usr/share/licenses` | `COPYING`, installed for every CTA package as RPM license material |
| Package documentation | `/usr/share/doc` | Documentation, including the generated `CHANGELOG.md` |
| Example configuration | `/etc/cta/*.example.*` | Vendor examples, refreshed by package upgrades |
| Logging schema | `/etc/cta/cta-logging.schema.json` | Machine-readable log schema for CTA |
| Repository definitions | `/etc/yum.repos.d` | CTA repository files installed by `cta-release` |
| Repository keys | `/etc/pki/rpm-gpg` | Package-signing keys installed by `cta-release` |
| Version lock configuration | `/etc/yum/pluginconf.d/versionlock.cta` | CTA dependency version locks installed by `cta-release` |
| Working configuration | `/etc/cta` | Configuration supplied and managed by the administrator |
| Systemd overrides | `/etc/systemd/system` | Local units, drop-ins, and other administrator overrides |

Use RPM itself when an exact package inventory is required:

```shell
rpm -ql cta-maintd
rpm -qf /usr/lib/systemd/system/cta-maintd.service
```

### Changelog, license, and manual pages

RPM records documentation and licenses separately from ordinary package files. Query the installed package rather than relying on a version-dependent directory name:

On the supported Enterprise Linux platform, the generated changelog is normally available as `/usr/share/doc/cta-common/CHANGELOG.md`, while a package's license is normally `/usr/share/licenses/<package>/COPYING` (for example, `/usr/share/licenses/cta-maintd/COPYING`). RPM queries are authoritative if the distribution changes these paths.

```shell
# CHANGELOG.md is provided by cta-common.
rpm -qd cta-common

# COPYING is packaged with every CTA RPM.
rpm -qL cta-maintd

# Show installed documentation and read a manual page.
rpm -qd cta-maintd
man cta-maintd
```

`rpm -qL` lists files marked as licenses. `rpm -qd` lists files marked as documentation, including manpages and `CHANGELOG.md`. The latter describes project changes across CTA releases. CTA does not maintain a duplicate manual RPM changelog.

Other useful package queries are:

```shell
rpm -qi cta-maintd                  # Summary, version, license, and metadata
rpm -q --requires cta-maintd        # Hard dependencies
rpm -q --recommends cta-maintd      # Weak dependencies such as systemd and logrotate
rpm -V cta-maintd                   # Verify installed files and metadata
getcap /usr/bin/cta-tape-label      # Inspect packaged executable capabilities
```

## Configure services before enabling them

Files named `*.example.*` illustrate supported configuration. They are not protected local configuration and an upgrade may replace them with newer examples. Copy or render the required values into a working configuration file without the `.example` component, following the relevant [CTA configuration documentation](../../configuration/service-runtime.md#component-examples).

CTA service packages deliberately do not start or enable services when first installed. This prevents an unconfigured daemon from starting with example, incomplete, or site-inappropriate settings. After supplying and validating working configuration, the administrator explicitly enables and starts the required instance. For example:

```shell
systemctl enable --now cta-maintd.service
systemctl status cta-maintd.service
```

Unit names and required configuration differ between services. Check the files installed by the package before applying the example command.

## Package lifecycle

| RPM operation | Service behavior | Administrator responsibility |
| --- | --- | --- |
| Initial installation | The service is neither enabled nor started. | Supply working configuration, validate it, and then explicitly enable or start the service. |
| Upgrade | The enabled or disabled state is preserved. A service is restarted only if it was running before the upgrade. | Follow the operational upgrade procedure and verify the restarted service. An inactive service remains inactive. |
| Removal | The service is stopped and disabled. | Confirm that removal is intended and preserve any site-owned configuration that is still needed. |

Package upgrades do not automatically restore obsolete configuration from `.rpmsave` files. Treat such files as historical material to inspect manually, not as configuration that the RPM will reactivate.

## Vendor units and local overrides

CTA owns the unit files under `/usr/lib/systemd/system`. Do not edit these files directly: an RPM upgrade may replace them. Put site-specific changes in a drop-in under `/etc/systemd/system`, preferably by running:

```shell
systemctl edit cta-maintd.service
systemctl daemon-reload
systemctl cat cta-maintd.service
```

The last command shows the vendor unit together with all active overrides. Keep the override as small as possible so improvements to the packaged unit continue to apply.

### Log and runtime directories

In CTA, packaged daemon units use `LogsDirectory=` to create `/var/log/cta` and its shared `old` archive directory with the configured ownership. They use `RuntimeDirectory=` for ephemeral per-service state below `/run/cta`. Systemd creates these directories before starting the service and manages their lifecycle; RPM installation no longer creates `/var/log/cta` itself.

The unit passes the corresponding runtime path to the daemon with `--runtime-dir`. The directory contains the runtime metadata described in [Service Runtime](../../configuration/service-runtime.md#runtime-directory). When creating multiple instances, give every instance a unique runtime directory, configuration file, and log file.

This behavior is systemd-specific. A container image does not acquire it merely by installing the RPM. Kubernetes manifests or another container runtime must provide any required volumes, writable directories, and ownership.

## Logging and rotation

On a conventional host, CTA can write service logs to `/var/log/cta`. All CTA daemons handle `SIGHUP` consistently: it reopens the log file descriptor without reloading configuration. Packaged logrotate policies therefore use rename-based rotation, create a new active file, and send `SIGHUP` to the main process of the corresponding default unit and any loaded template instances.

Rotated files are moved to `/var/log/cta/old`, which must be on the same physical device as the active log for rename-based rotation. The policies use `sharedscripts`, safe wildcard patterns, and tolerate services that are not active. If a policy specifies `hourly`, the system-wide logrotate job must also run hourly for that directive to take effect.

Systemd and logrotate are weak package dependencies. They remain available by default for regular host installations, while a container build can omit them by excluding weak dependencies. CTA containers log to standard output by default, and the deployment is responsible for collection, retention, rotation, writable directories, and ownership.

For log formats and schemas, see [Logging](../../monitoring/logging.md).

## Next steps

Continue with the shared [configuration and commissioning steps](index.md#after-installation).
