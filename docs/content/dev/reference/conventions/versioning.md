# CTA Versioning

Version conventions for CTA software, catalogue schemas, and published documentation. These have separate version numbers and compatibility rules.

## Software and package versions

!!! note "Versioning policy under revision"

    The software and package versioning policy is undergoing revision. Follow the current [Release Procedure](../../contributing/maintainers/releases.md) and release tooling when preparing a release; the finalized conventions will be documented here.

## Catalogue schema versions

- Catalogue schemas use `major.minor` versions, independently of CTA software versions. Schema release tags have no `v` prefix, for example `15.0`.
- Increment the minor version for backward-compatible schema changes. Increment the major version for changes incompatible with the CTA software using the previous schema, starting the new major at `.0`.
- Create a new schema version before changing released SQL definitions. Released schema versions and tags MUST remain unchanged.
- The schema version is defined in `catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake`. CTA's `project.json` records the included version in `catalogueVersion` and the supported schema **major** versions in `supportedCatalogueVersions`.
- CTA checks the catalogue's major version for compatibility. A transition CTA release may support both the old and new schema majors, but declaring them in `supportedCatalogueVersions` does not provide compatibility by itself: both combinations must work and be tested.
- Publishing a schema release or updating CTA's schema submodule does not migrate an existing database. Required intermediate versions and deployment order must be documented as part of the upgrade path.

For implementation and migration guidance, see [Catalogue Schema Changes](../internals/catalogue/schema-development/index.md). For tagging and CTA integration, see [Catalogue Schema Releases](../../contributing/maintainers/catalogue-schema-releases.md).

## Documentation versions

- Documentation is published per CTA **minor series**, such as `6.12`, rather than separately for every patch or package revision. For example, `v6.12.0.0-1` publishes to `6.12`; a newer release in that series updates the same documentation version.
- Publication runs from protected canonical release tags. Prerelease and variant tags do not publish documentation. Merging a documentation change alone does not update the public site.
- `latest` points to the highest published minor series. Updating an older series does not move this alias backwards, and an older release tag cannot overwrite documentation already published from a newer release in the same series.
- Write pages for the release represented by the selected documentation version. Put historical feature introductions in the [Changelog](../../../changelog.md), while retaining dependency constraints and migration details needed to use or upgrade that release.
- Documentation-only backports can update an existing series without publishing RPMs or container images. Currently they still require a new canonical software release tag and consume a release version.

See [Documentation Site](../../contributing/maintainers/documentation-site.md) for publication and the [documentation backport procedure](../../contributing/maintainers/documentation-site.md#documentation-backports).
