# CTA Versioning

Version conventions for CTA software, catalogue schemas, and published documentation. These have separate version numbers and compatibility rules.

## Software and package versions

Git release tags use `v<family>.<major>.<minor>.<patch>-<package>[.rcN][.<variant>]` and do not include the platform.
`project.json` defines the release family as a positive integer string in `releaseFamily` (currently `"6"`).
New release requests must match that family; historical tags remain readable.
Builds remove the leading `v` and append the platform once.
For example, `v6.12.0.0-1.pgall` produces `6.12.0.0-1.pgall.el9` on enterprise Linux 9.

The full build version is used by software version output, logs, RPM version-release fields, source archives, container tags and SBOM component metadata.
CMake accepts it as one `CTA_VERSION`; RPM packaging splits it internally at the single hyphen.
Exactly one hyphen is permitted: release components use dots, so `6-dev.example` is valid but `6-dev-example` is rejected.
CMake validates the variant immediately before the platform against `CTA_USE_PGSCHED` and `CTA_WITH_ORACLE`, rejecting missing, conflicting, duplicated or misplaced variant labels without rewriting the supplied version.

| Variant suffix | Scheduler | Oracle support |
| --- | --- | --- |
| None | Objectstore | Enabled |
| `.pgsched` | PostgreSQL | Enabled |
| `.pgcat` | Objectstore | Disabled |
| `.pgall` | PostgreSQL | Disabled |

GitLab development builds use `<family>-<pipeline-id>git<short-sha>[.<variant>].<platform>`.
The `.pre` job `prepare-cta-version` reads the family and resolves the primary pipeline version.
Its 30-day dotenv report provides `CTA_RELEASE_FAMILY`, `CTA_VERSION_BASE` and `CTA_VERSION` directly to consumers through explicit artifact dependencies.
The auxiliary PostgreSQL package build resolves its own variant from `CTA_VERSION_BASE`; it does not change the primary pipeline version.
Preparation is skipped when testing an existing image or refreshing CI tooling images.
Child pipelines use their own pipeline IDs.
GitHub analysis builds use the GitHub run ID and the first eight commit SHA characters in the same format.
Local builds default to `<family>-dev` from `project.json`, resolved using the local configuration; the default configuration produces `6-dev.pgcat.el9`.

Existing images supplied through the custom-image-tag input are used verbatim, including historical tag formats.
CI tooling images and dependency versions have independent naming conventions.
See the [Release Procedure](../../contributing/maintainers/releases.md) for creating and publishing releases.

## Catalogue schema versions

- Catalogue schemas use `major.minor` versions, independently of CTA software versions. Schema release tags have no `v` prefix, for example `15.0`.
- Increment the minor version for backward-compatible schema changes. Increment the major version for changes incompatible with the CTA software using the previous schema, starting the new major at `.0`.
- Create a new schema version before changing released SQL definitions. Released schema versions and tags MUST remain unchanged.
- The schema version is defined in `catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake`. CTA's `project.json` records the included version in `catalogueVersion` and the supported schema **major** versions in `supportedCatalogueVersions`.
- CTA checks the catalogue's major version for compatibility. A transition CTA release may support both the old and new schema majors, but declaring them in `supportedCatalogueVersions` does not provide compatibility by itself: both combinations must work and be tested.
- Publishing a schema release or updating CTA's schema submodule does not migrate an existing database. Required intermediate versions and deployment order must be documented as part of the upgrade path.

For implementation and migration guidance, see [Catalogue Schema Changes](../../internals/components/catalogue/schema-development.md). For tagging and CTA integration, see [Catalogue Schema Releases](../../contributing/maintainers/catalogue-schema-releases.md).

## Documentation versions

- Documentation is published per CTA **minor series**, such as `6.12`, rather than separately for every patch or package revision. For example, `v6.12.0.0-1` publishes to `6.12`; a newer release in that series updates the same documentation version.
- Publication runs from protected canonical release tags. Prerelease and variant tags do not publish documentation. Merging a documentation change alone does not update the public site.
- `latest` points to the highest published minor series. Updating an older series does not move this alias backwards, and an older release tag cannot overwrite documentation already published from a newer release in the same series.
- Write pages for the release represented by the selected documentation version. Put historical feature introductions in the [Changelog](../../../changelog.md), while retaining dependency constraints and migration details needed to use or upgrade that release.
- Documentation-only backports can update an existing series without publishing RPMs or container images. Currently they still require a new canonical software release tag and consume a release version.

See [Documentation Site](../../contributing/maintainers/documentation-site.md) for publication and the [documentation backport procedure](../../contributing/maintainers/documentation-site.md#documentation-backports).
