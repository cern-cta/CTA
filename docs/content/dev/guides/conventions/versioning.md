# CTA Versioning

Version conventions for CTA software, catalogue schemas, and published documentation. These have separate version numbers and compatibility rules.

## Software and package versions

Source release tags use `v<major>.<minor>.<patch>-<package>`, for example `v6.12.0-1`.
The first three numbers identify the software release; the number after `-` is the packaging revision.
The major number must match `majorVersion` in `project.json` when creating a release with the release tool.

The only optional tag suffixes are `.devN` for development releases and `.rcN` for release candidates, where `N` is a positive integer.
Use at most one suffix, for example `v6.12.0-1.dev1` or `v6.12.0-1.rc2`.
The release tool accepts explicit prerelease tags; `release tag v6.12.0-1 --release-candidate` selects the next unused RC number automatically.
The changelog command uses the unsuffixed base version.
Release status accepts either the base version to show all its tags or a specific prerelease tag.

One Git tag identifies the source for all supported build configurations.
Do not append backend or platform suffixes to Git tags.
CI derives those suffixes for package versions and container image tags instead:

| Scheduler | Oracle support | Artifact variant suffix |
| --- | --- | --- |
| objectstore | enabled | none |
| PostgreSQL | enabled | `.pgsched` |
| objectstore | disabled | `.pgcat` |
| PostgreSQL | disabled | `.pgall` |

The platform suffix follows the variant, such as `.el9`, `.el10`, or `.debian13`.
For example, source tag `v6.12.0-1.rc2` produces image tag `6.12.0-1.rc2.pgall.el10` and RPM version `6.12.0-1~rc2.pgall.el10` for an EL10 PostgreSQL build without Oracle.
RPM and Debian package versions translate `.devN` and `.rcN` to `~devN` and `~rcN`, ensuring development releases sort before candidates and candidates before final releases.
Container image tags retain dots because they cannot contain `~`.
The leading `v` belongs to the Git tag and is omitted from artifact versions.
Debian currently supports only `pgall` and does not publish release packages or images.

The main tag pipeline builds the default EL9/objectstore configuration with Oracle.
Only the EL9 PostgreSQL scheduler child with Oracle starts automatically on tags; the remaining supported combinations are optional manual triggers.
The release tool and CI's `prepare-cta-version` job share the same source-tag validator.
Invalid tags fail that job, and release variant triggers wait for validation to succeed.
Historical four-number tags remain available for previous-release discovery but are no longer accepted as new release tags.
Published tags and artifacts must not be moved or replaced with different release contents.

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

- Documentation is published per CTA **minor series**, such as `6.12`, rather than separately for every patch or package revision. For example, `v6.12.0-1` publishes to `6.12`; a newer release in that series updates the same documentation version.
- Publication runs from protected canonical release tags. Development and release-candidate tags do not publish documentation; variant child pipelines do not publish it either. Merging a documentation change alone does not update the public site.
- `latest` points to the highest published minor series. Updating an older series does not move this alias backwards, and an older release tag cannot overwrite documentation already published from a newer release in the same series.
- Write pages for the release represented by the selected documentation version. Put historical feature introductions in the [Changelog](../../../changelog.md), while retaining dependency constraints and migration details needed to use or upgrade that release.
- Documentation-only backports can update an existing series without publishing RPMs or container images. Currently they still require a new canonical software release tag and consume a release version.

See [Documentation Site](../../contributing/maintainers/documentation-site.md) for publication and the [documentation backport procedure](../../contributing/maintainers/documentation-site.md#documentation-backports).
