# Dependency Management

## Rust Libraries

Please avoid pulling dependencies from outside the official cargo registry (e.g. GitHub repos). There is a **cooldown period of 14 days**
for every crate. `cargo` will by default refuse to update to packages which do not respect this cooldown period. However, in exceptional
circumstances (e.g. a CVE), you may end up getting a complaint from `cargo deny` in CI. In that case, you can override the cooldown check
for the crate in question:

```shell
$ CARGO_RESOLVER_INCOMPATIBLE_PUBLISH_AGE=allow cargo update -p <crate> --precise <version>
```

!!! warning "Use sparingly"

    This should only be done in exceptional circumstances, e.g. an urgent release after a CVE in a dependency.

## Vendored Dependency Packages

Package definitions, build commands, tagging, publication, and dependency backports are documented in the [cta-dependencies README](https://gitlab.cern.ch/cta/cta-dependencies/-/blob/master/README.md). Follow that repository's procedure to add or update a package, then adopt the published version in CTA as described below.

!!! warning "Coordinate changes to shared repositories"

    Do not update `cta-ci-*` package repositories without explicit coordination with the operations team. Despite their names, these repositories are also used by operations; changes can affect systems outside CI. Agree on the scope, validation, and timing before updating them.

### Adopt the dependency in CTA

- Update the relevant platform entries in `project.json`, including `buildRequires` and `versionlock` where applicable. Keep generated RPM dependency constraints consistent; see [RPM SPEC and Service Integration](../../guides/conventions/rpm-packaging.md).
- If a CI execution image contains the dependency, follow [Add or update an image dependency](ci-maintenance.md#add-or-update-an-image-dependency). Changing a package repository or version constraint alone does not replace already-built, pinned images.
- Rebuild and validate the affected CTA components, including relevant unit and system tests. Check runtime behaviour as well as compilation when updating a linked library.
- Review the resulting package dependencies and container contents. Include relevant compatibility changes and operator actions in the CTA changelog.
