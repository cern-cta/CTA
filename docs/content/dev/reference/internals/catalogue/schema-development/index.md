# Developing catalogue schema changes

This section covers designing, implementing, and testing catalogue schema changes. Developers need to validate upgrades before a schema is released; tagging and publication belong in [Catalogue Schema Releases](../../../../contributing/maintainers/catalogue-schema-releases.md). For upgrades on deployed instances, see [Operations](../../../../../ops/run-and-maintain/upgrades/catalogue-schema/index.md).

## Development workflow

1. Decide whether the change is [backward-compatible](compatible-changes.md) or [backward-incompatible](incompatible-changes.md). Identify the CTA versions that must work before and after the migration. Incompatible changes may require intermediate software and schema releases.
2. [Create a new schema version](new-version.md) in the `catalogue/cta-catalogue-schema` submodule before modifying SQL. Follow the [naming conventions](naming.md) and preserve previously released schema definitions.
3. Add the corresponding [Liquibase migration scripts](migration-scripts.md), including upgrade preconditions, schema-version and status updates, and rollback where supported. Document irreversible changes explicitly.
4. [Test fresh catalogue creation and migrations](testing.md), including CTA behaviour against the supported old and new schemas.
5. Submit the schema changes through an MR in the schema repository. Link any CTA compatibility changes and test results; merge only after review and validation. Coordinate tagging and the final CTA submodule update with the release maintainers.

The schema repository is separate from CTA: committing its changes does not update CTA’s submodule pointer. During development, a CTA integration branch can reference the candidate schema commit for testing; the released CTA version must reference the reviewed schema release.
