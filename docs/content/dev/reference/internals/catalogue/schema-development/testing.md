# Testing catalogue migrations

Validate schema changes in a disposable development environment before requesting a release. A successful build of the schema repository generates SQL but does not establish that a populated catalogue can be upgraded.

## What to validate

- Create a fresh catalogue with the new schema and verify it with [cta-catalogue-schema-verify](../../../../../ops/tools/cta-catalogue-schema-verify.md).
- Start with each supported source schema, populate representative data, apply the migration, and verify the resulting schema and data.
- Exercise CTA against the old and new schemas where compatibility is required, including any intermediate upgrade stages.
- Test rollback where supported. Record irreversible operations and recovery requirements; a successful upgrade does not imply it can be rolled back.

## CTA migration test

Use a CTA integration branch referencing the candidate schema commit. Update `project.json` to describe the intended `catalogueVersion` and `supportedCatalogueVersions`, then run `test-catalogue-schema-update` in its MR pipeline. The job is normally manual on branches and automatic on tags and schedules.

The test uses the schema submodule’s commit unless `tests.catalogue_schema_update.schema_checkout_ref` is explicitly overridden in the system-test configuration. A schema release tag is therefore not required to test a candidate change. Check that any override points to the revision being reviewed.

The test applies the migration, checks the resulting version and schema, then rolls back and verifies the original schema. Check the job logs and artifacts and link the result from the schema MR. See [Testing and CI](../../../testing/ci/index.md) for investigating failures.

### Current coverage limits

The CI job requires exactly two supported schema major versions and tests between their `.0` versions. It skips with only one supported major and fails with more than two. Verify that the job actually ran the migration; a successful skipped job is not validation.

Minor-version migrations, additional source versions, backend combinations, and data-dependent changes need validation beyond this job. Keep the compatibility list accurate rather than changing it merely to enable the test. Use the [catalogue upgrade procedure](../../../../../ops/run-and-maintain/upgrades/catalogue-schema/index.md) in a disposable environment for paths the job does not cover, and record the exact schema commits, software versions, and results.

## Validation against a production database clone

TODO: Integrate the production-clone validation workflow tracked in [CTA #904](https://gitlab.cern.ch/cta/CTA/-/work_items/904). The intended sequence is:

1. Use `cta-dev` to deploy the selected bridge/pivot CTA version against an isolated clone of the production database, still on the previous catalogue schema. Preserve the cloned data rather than initializing or resetting the catalogue.
2. Establish baseline catalogue health checks, then deploy the catalogue-updater and migrate the clone to the new schema.
3. Repeat the checks with the bridge/pivot CTA version against the upgraded catalogue.
4. Upgrade CTA to the target version beyond the bridge/pivot release and repeat the checks again.

Define a minimal set of checks for schema/version consistency, CTA catalogue connectivity, representative read-only queries, and migration-specific data invariants. General system-test suites can create or delete catalogue entries and should not be run unchanged against the clone. Any write checks need explicitly scoped test data and cleanup. Keep the environment isolated from production services.

The exact `cta-dev` commands, updater configuration, and health checks remain to be implemented and documented through the issue.
