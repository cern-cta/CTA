# RPM SPEC and Service Integration

This page defines the packaging conventions for CTA RPM SPEC files, systemd units, and logrotate policies. For the resulting operator-visible behavior, see [RPM Packages and Services](../../ops/deployment/rpm-packages-and-services.md).

!!! warning "CTA 6 and later"

    The common service interfaces in this document describe the CTA 6 packaging target. CTA 5 services are not yet consistent in their runtime-directory and log-reopening behavior.

## RPM SPEC conventions

### Configuration and documentation

- Packaged example files such as `/etc/cta/*.example.*` MUST NOT be marked `%config(noreplace)`. They are vendor documentation and SHOULD be replaced on upgrade so that they describe the installed software version.
- Working, administrator-owned configuration MUST remain distinct from packaged examples. Package scriptlets MUST NOT automatically restore obsolete `.rpmsave` files.
- `COPYING` MUST be installed with `%license`, not as ordinary documentation.
- The generated `CHANGELOG.md` MUST be packaged with `cta-common`. A manually maintained RPM `%changelog` section MUST NOT duplicate the project changelog.

### Paths and file metadata

- SPEC files SHOULD use RPM path macros such as `%{_bindir}`, `%{_sbindir}`, `%{_unitdir}`, `%{_mandir}`, `%{_docdir}`, and `%{_licensedir}` instead of spelling out distribution paths.
- Manpage entries MUST be independent of the compressor selected by the build environment, for example `%{_mandir}/man1/example.1*`.
- Required Linux capabilities MUST be declared as RPM file metadata rather than assigned imperatively in a scriptlet. This lets RPM install, verify, and restore the intended metadata.
- Package-to-package dependencies MUST carry a version constraint whenever the packages must be upgraded together. In particular, all CTA library dependencies MUST have these version constraints.
- Interpreted tools MUST explicitly require their interpreter. `cta-release`, for example, MUST depend on Python 3.

### Build and validation

- Unit tests MUST run in the standard `%check` phase. Build failures and test failures then have the expected RPM semantics and can be handled by standard tooling.
- Summaries and descriptions MUST accurately identify each subpackage and SHOULD be checked for copied text and typographical errors.
- Obsolete RPM constructs MUST NOT be added. This includes `Group`, `BuildRoot`, `%defattr`, and unnecessary `ldconfig` scriptlets on platforms where dependency generation and file triggers provide the behavior.

## Systemd integration

Systemd service units supplied by an RPM MUST be installed in `%{_unitdir}` (`/usr/lib/systemd/system` on the supported platform). `/etc/systemd/system` is reserved for administrator units and overrides.

Service package scriptlets MUST preserve the following lifecycle:

- Initial installation MUST NOT enable or start a service.
- An upgrade MUST preserve the enabled or disabled state.
- An upgrade MUST restart a service only when it was already running.
- Final package removal MUST stop and disable the service.

Administrators remain responsible for providing working configuration before enabling a daemon. Scriptlets MUST NOT infer that an installed example is safe to run in a particular deployment.

Systemd SHOULD be a weak runtime dependency for service packages and MUST NOT be an unnecessary build requirement. This preserves normal host integration while allowing container images to omit systemd by disabling weak dependencies. The build still requires `systemd-rpm-macros`, which supplies the RPM lifecycle macros without requiring the systemd runtime itself.

### Unit-file conventions

- Units that write persistent CTA logs MUST use `LogsDirectory=cta cta/old` rather than relying on the RPM to create `/var/log/cta` and its rotation archive.
- CTA daemon units MUST use a per-service `RuntimeDirectory=` and pass the matching `/run` location with `--runtime-dir`. Directory, configuration, and log paths MUST be unique when multiple unit instances share a host.
- Ownership and permissions SHOULD be expressed through systemd directory directives and the service user, avoiding package-time creation of runtime state.
- Restart policy, shutdown behavior, timeouts, resource limits, dependency ordering, and security hardening MUST reflect the daemon's actual behavior. They MUST NOT be copied mechanically between `cta-maintd`, `cta-taped`, and `cta-rmcd`.
- Site-specific settings MUST be left to administrator drop-ins under `/etc/systemd/system`.

## Logrotate integration

Logrotate policies for CTA services MUST:

- use rename-based rotation rather than copying and truncating an open log;
- create the replacement active log with the correct CTA user, group, and mode;
- send `SIGHUP` to the main process of the affected default unit and all loaded template instances; in CTA this reopens logs without reloading configuration;
- tolerate the absence of an active default unit or template instance;
- keep the `olddir` archive on the same physical device as the active log so rename-based rotation works;
- keep policy ordering, retention settings, comments, and wildcard handling consistent across CTA services;
- avoid matching rotated files again through an overly broad wildcard; and
- remain a weak dependency, because containers log to standard output by default and normally delegate retention to the deployment platform.

Container manifests MUST define any non-standard file logging, volume, directory ownership, collection, and retention behavior. These concerns do not belong in a host-oriented RPM scriptlet.
