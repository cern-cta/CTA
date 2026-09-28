# Catalogue Schema Releases

Catalogue schemas are released from the separate `cta-catalogue-schema` repository, included in CTA at `catalogue/cta-catalogue-schema`. Validate the candidate schema commit with CTA before tagging it. Tag that reviewed and tested commit, then reference its tag in the final CTA release. The CTA `release` tool does not create schema tags.

For authoring schema changes and migrations, see [Schema Development](../../reference/internals/catalogue/schema-development/index.md).

## Review and coordinate

Before tagging the schema, collect the following evidence in the CTA release issue. If changelog preparation has not created it yet, create it using the Release issue template, the title `Release <version>` (for example, `Release v6.12.0.0-1`), and the `type::release` label. The release tool will reuse it:

- The reviewed schema MR and exact schema and CTA commit SHAs used for validation. Check that the schema version, generated SQL, migration scripts, and `ReleaseNotes.md` agree.
- Links to fresh-catalogue and migration-test results, identifying the source and destination schema versions and database backends tested. Record rollback limitations and any required intermediate upgrades.
- Evidence that the bridge CTA version works with the required old and new schemas, and a deployment order agreed with operations.

A successful schema-repository build does not by itself validate a database upgrade. If the schema changes after testing, validate the revised commit before tagging it.

## Tag the schema

After merging the schema changes and checking their CI results, create a tag on the reviewed commit in the schema repository. Existing schema tags use `major.minor` without a `v` prefix, for example `15.0`.

From a checkout of the schema repository, replace the example version and commit below:

```bash
# Refresh the commits and existing tags before selecting the release.
git fetch origin --tags
# Tag the exact reviewed schema commit.
git tag -a 16.0 REVIEWED_COMMIT_SHA -m "Catalogue schema 16.0"
# Publish the schema tag for CTA to reference.
git push origin refs/tags/16.0
```

Verify that the published tag points to the intended commit and its pipeline succeeds. Do not move an existing release tag to include later fixes.

## Integrate into CTA

On a CTA contribution branch, update the submodule to the published schema tag. From the CTA repository root:

```bash
# Fetch the published schema release.
git -C catalogue/cta-catalogue-schema fetch origin --tags
# Select the schema tag to include in CTA (replace the example version).
git -C catalogue/cta-catalogue-schema switch --detach 16.0
```

Update `project.json`: set `catalogueVersion` to the new schema version and list the supported schema **major** versions in `supportedCatalogueVersions`. For a transition from `15.0` to `16.0`, use the following values, provided CTA has been validated against both:

```json
{
  "catalogueVersion": 16.0,
  "supportedCatalogueVersions": [15, 16]
}
```

Keep support for the old schema in the bridge release. Remove it only in a later, separately reviewed CTA change once the supported upgrade path no longer requires it; tagging the new schema is not a reason to remove compatibility.

A transition CTA release provides a tested bridge between the old and new schemas. Listing both versions declares compatibility; the code must actually work with both. Some incompatible changes also require an intermediate schema that retains old structures while introducing their replacements. See [Transition versions](../../reference/internals/catalogue/schema-development/incompatible-changes.md#transition-versions) for the development approach, and record the required deployment order in the release issue.

The CTA integration MR must include both the submodule pointer and `project.json` changes. Submit them together through the normal [GitLab contribution workflow](../gitlab.md). Include a [changelog entry](../changelog.md) explaining the schema upgrade and any operator action. Keep the CTA schema release focused on schema integration and necessary compatibility changes.

## Verify and release CTA

Review the [migration validation results](../../reference/internals/catalogue/schema-development/testing.md) for the final schema and CTA revisions before merging and publishing. Confirm the intended upgrade paths were exercised, including cases outside the automated test’s coverage. See also [validation against a production database clone](../../reference/internals/catalogue/schema-development/testing.md#validation-against-a-production-database-clone).

Follow [Release Procedure](releases.md) to publish the accompanying CTA version. Verify that the published CTA artifacts and the selected updater image/configuration use the reviewed schema revision and migration scripts.

## Hand over to operations

The catalogue-updater container provides the tools used to apply and verify migrations. Its image build and configuration are maintained in the [cta-catalogue-updater repository](https://gitlab.cern.ch/cta/eoscta-operations/containers/cta-catalogue-updater) (CERN-internal).

Agree with operations on the updater image and configuration, and arrange any required changes before deployment. Record the following in the release issue:

- Schema tag and commit, bridge CTA version, and subsequent target CTA version where applicable.
- Exact updater image tag or digest and the configuration selecting the reviewed migration.
- Any remaining deployment prerequisites; link the validation evidence collected above rather than duplicating it.
- The order of CTA and schema upgrades, including any intermediate releases and checks between stages.

Publishing a schema tag or CTA release does not upgrade a running catalogue. Deployment instructions belong in [Upgrading the CTA catalogue schema](../../../ops/run-and-maintain/upgrades/catalogue-schema/index.md).
