# CTA Versioning

Version conventions for CTA software, catalogue schemas, and published documentation. These have separate version numbers and compatibility rules.

## Software and package versions

- Source release tags use `v<major>.<minor>.<patch>`, for example `v6.12.0`. Packaging-only changes also increment the patch version.
- The only optional tag suffixes are `.devN` for development releases and `.rcN` for release candidates, where `N` is a positive integer. Use at most one suffix.
- One Git tag identifies the source for all supported build configurations. Backend and platform suffixes never appear in Git tags or Python package versions.
- The leading `v` belongs to the Git tag and is omitted from artifact versions.
- Historical tags remain readable temporarily for previous-release discovery, until maintained release lines have a final tag in the new format.
- Published tags and artifacts must not be moved or replaced with different release contents.

CI derives the artifact variant from the build configuration:

| Scheduler | Oracle support | Variant |
| --- | --- | --- |
| objectstore | enabled | none |
| PostgreSQL | enabled | `pgsched` |
| objectstore | disabled | `pgcat` |
| PostgreSQL | disabled | `pgall` |

- RPMs use packaging revision `1`, followed by the variant and platform: `6.12.0-1.pgsched.el9`.
- Debian packages use revision `1`, followed by `.variant` when applicable: `6.12.0-1.pgsched`. The distribution target is selected by the build and repository rather than appended to the version.
- RPM and Debian upstream versions translate `.devN` and `.rcN` to `~devN` and `~rcN`, so development releases sort before candidates and candidates before final releases.
- Python versions use standard development and candidate suffixes: `6.12.0.dev1`, `6.12.0rc1`, and `6.12.0`.
- Container image tags retain dots because they cannot contain `~`. The variant and platform follow the source version: `6.12.0.rc2.pgall.el10`.
- Build variants share package names and are compared as versions by package managers. Repository selection or version pinning must prevent unintended backend changes.

See the [Release Procedure](../../contributing/maintainers/releases.md) for version increments, release preparation, the build matrix, and publication gates.

## Catalogue schema versions

- Catalogue schemas use `major.minor` versions, independently of CTA software versions. Schema release tags have no `v` prefix, for example `15.0`.
- Increment the minor version for backward-compatible schema changes. Increment the major version for changes incompatible with the CTA software using the previous schema, starting the new major at `.0`.
- Create a new schema version before changing released SQL definitions. Released schema versions and tags MUST remain unchanged.
- The schema version is defined in `catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake`. CTA's `project.json` records the included version in `catalogueVersion` and the supported schema **major** versions in `supportedCatalogueVersions`.
- CTA checks the catalogue's major version for compatibility. A transition CTA release may support both the old and new schema majors, but declaring them in `supportedCatalogueVersions` does not provide compatibility by itself: both combinations must work and be tested.
- Publishing a schema release or updating CTA's schema submodule does not migrate an existing database. Required intermediate versions and deployment order must be documented as part of the upgrade path.

For implementation and migration guidance, see [Catalogue Schema Changes](../../internals/components/catalogue/schema-development.md). For tagging and CTA integration, see [Catalogue Schema Releases](../../contributing/maintainers/catalogue-schema-releases.md).

## Documentation versions

- Documentation is published per CTA **minor series**, such as `6.12`, rather than separately for every patch release. For example, `v6.12.0` publishes to `6.12`; a newer release in that series updates the same documentation version.
- Publication runs from protected canonical release tags. Development and release-candidate tags do not publish documentation. Merging a documentation change alone does not update the public site.
- `latest` points to the highest published minor series. Updating an older series does not move this alias backwards, and an older release tag cannot overwrite documentation already published from a newer release in the same series.
- Write pages for the release represented by the selected documentation version. Put historical feature introductions in the [Changelog](../../../changelog.md), while retaining dependency constraints and migration details needed to use or upgrade that release.
- Documentation-only backports can update an existing series without publishing RPMs or container images. Currently they still require a new canonical software release tag and consume a release version.

See [Documentation Site](../../contributing/maintainers/documentation-site.md) for publication and the [documentation backport procedure](../../contributing/maintainers/documentation-site.md#documentation-backports).
