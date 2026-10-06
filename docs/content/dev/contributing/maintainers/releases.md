# Release Procedure

Use the `release` tool in `ci/release/` to prepare the changelog and release issue, then tag the reviewed release. CI builds and tests the tagged software; maintainers review the results and start publication.

## Set up the release tool

Run commands from the repository root, with Python 3 and Git available and `origin` pointing to CERN GitLab. Start with a clean checkout of the target branch (`main` by default).

```bash
export PATH="$PWD/ci/release:$PATH"
release --help
```

The tool needs a GitLab personal access token with the `api` scope and permission to create release issues, MRs, and protected release tags. It reads `GITLAB_TOKEN` or `~/.config/cta/gitlab-api-token`; if neither is available, it prompts for a token and offers to save it. Git pushes use your normal repository authentication.

## Choose the version

Commands take an explicit, unsuffixed version such as `v6.12.0.0-1`:

```text
v<xrootd>.<major>.<minor>.<patch>-<package>
```

| Field | When it changes |
| --- | --- |
| `xrootd` | Release family associated with XRootD; new releases must match `releaseFamily` in `project.json`. |
| `major` | A new catalogue schema is introduced. |
| `minor` | A regular release without a new catalogue schema. |
| `patch` | Fixes are backported to an older release. |
| `package` | Packaging or build revisions without changes to software behaviour. |

Catalogue schema releases should contain only the schema dependency and compatibility changes unless other code changes are necessary. Reference a tagged schema release with its migrations, and include both old and new supported catalogue versions in `project.json` for the upgrade. See [Catalogue Schema Releases](catalogue-schema-releases.md).

## Prepare and review the changelog

```bash
release changelog v6.12.0.0-1
```

The command synchronizes the target branch, generates changes since the previous numeric CTA release, and opens the changelog in Git’s configured editor. Review the entries and resolve the “Commits missing valid Changelog trailers” section: move relevant changes into their categories and remove entries that do not belong in the changelog.

After confirmation, the tool creates or reuses the release issue, changelog branch, and MR, and prints their links. Add release requirements and deployment considerations to the issue. Review and merge the changelog MR through the normal GitLab workflow, then wait for the resulting commit’s branch pipeline to succeed.

Inspect progress with:

```bash
release status v6.12.0.0-1
```

## Create release tags

```bash
release tag v6.12.0.0-1
```

The tool selects the merged changelog MR’s commit, checks that it belongs to the target branch, and checks its release metadata and push pipeline. It opens an editor for the tag description and asks for confirmation before pushing the selected tags atomically. Resolve warnings about missing metadata or an unsuccessful pipeline before proceeding.

The command creates the base tag and asks whether to add the `pgsched`, `pgcat`, and `pgall` variants. These select the PostgreSQL scheduler, PostgreSQL catalogue, or both. Use `--suffix` to create only selected variants, without the base tag:

```bash
release tag v6.12.0.0-1 --suffix pgsched --suffix pgcat
```

For a release candidate, let the tool select the next unused `.rcN`:

```bash
release tag v6.12.0.0-1 --release-candidate
```

To publish the validated candidate as a final release, run `release tag` with the same base version and target branch, without `--release-candidate`. Confirm that it selects the tested commit, then review the final tag pipelines before publishing.

The tool always selects the merged changelog MR’s commit. Repeating `--release-candidate` only advances the RC number; it does not include fixes merged afterwards. If the candidate needs code changes, the current tool requires preparing a new base release version and changelog MR containing those fixes, then tagging and validating that candidate. Do not assume a new RC number means a new source revision.

Every selected tag must be new. Do not move published tags. To preview a command without making changes, put `--dry-run` before the subcommand:

```bash
release --dry-run tag v6.12.0.0-1
```

Use `release tag --help` for other options. `--yes` skips confirmations, including warnings about the release commit’s pipeline; reserve it for automation that has already checked the release state.

## Review CI results

Tag pipelines automatically build the release and run the configured unit and system tests, including tests that are normally manual on branches. The `stress-test` job also runs automatically in default tag pipelines, except for `pgcat` and `pgall` variants, where it remains manual. Run it explicitly for those variants when needed for release validation.

Check the actual test results before publishing: **the stress test allows failure, so a successful overall pipeline does not establish that it passed**. Check catalogue migration results when the release introduces a schema upgrade. For logs and artifacts, see [Testing and CI](../../guides/testing/ci/index.md).

The stress-test and publication jobs post status notes with job links on the release issue. Review these notes and add any remaining validation evidence, such as the stress-test dashboard and timeframe. If a note is missing, inspect the job directly; issue reporting is best-effort.

## Publish packages and images

Software and artifact versions omit the Git tag's leading `v` and include the build platform.
For example, tag `v6.12.0.0-1.pgall` produces software version and image tag `6.12.0.0-1.pgall.el9`, and RPMs such as `cta-taped-6.12.0.0-1.pgall.el9.x86_64.rpm`.
The image-build jobs push these tags to the private registry before release promotion.
The private publication job retains its existing gate but republishes the same image reference; public publication copies it under the same version.
The RPM publication helpers take the full version including platform and match it exactly.
Publication jobs depend directly on `prepare-cta-version` for the full version and release family; this metadata is retained for 30 days.

After validating the release, trigger `internal-release-cta` in each tag pipeline being published. **This also starts public `unstable` RPM publication and private image publication automatically** once their dependencies succeed; it is not an internal-only publication step. These jobs do not enforce completion of all release tests, so check those first.

| Job | Publication |
| --- | --- |
| `internal-release-cta` | Manual: publishes RPMs to the CTA internal repository. |
| `public-release-cta-unstable` | Automatic after internal publication: publishes and verifies public unstable RPMs. |
| `public-release-cta-testing` | Manual after internal publication: publishes RPMs for testing. |
| `public-release-cta-stable` | Manual after internal and unstable publication: publishes RPMs validated in production at CERN. |
| `publish-images-private` | Automatic after internal publication and image builds. |
| `publish-images-public` | Manual for builds without Oracle support. |

Check the publication jobs and their release-issue notes. Documentation publishes independently from canonical protected release tags; see [Documentation Site](documentation-site.md).

## Stable promotion and announcements

Promote to `stable` only after production validation; `unstable` does not imply production validation. Only stable releases should have a GitLab [Release entry](https://gitlab.cern.ch/cta/CTA/-/releases) and an announcement on the [CTA Community forum](https://cta-community.web.cern.ch/). Include the relevant changelog and any upgrade or migration instructions in both. Do not create these announcements for unstable, testing, or release-candidate versions.

Complete the release issue checklist and close it when its work is done. Stable promotion and its announcements may happen later.

## Maintenance releases and backports

Start from the tag of the release being fixed, rather than the current `main` branch. Choose a new version and create a branch named `release/<new-release>`. For example, to backport a reviewed fix from `v6.12.0.0-1` into `v6.12.0.1-1`:

```bash
# Fetch the release tags and commits to backport.
git fetch origin --tags
# Start from the exact release being fixed.
git switch --detach v6.12.0.0-1
# Create the target branch for the maintenance release.
git switch -c release/v6.12.0.1-1
# Apply the fix and record its original commit (replace FIX_COMMIT_SHA).
git cherry-pick -x FIX_COMMIT_SHA
# Publish the branch so the release tool can target it.
git push -u origin release/v6.12.0.1-1
```

Cherry-pick any required prerequisite fixes in order. If conflicts occur, resolve them, stage the resolved files, and run `git cherry-pick --continue`; use `git cherry-pick --abort` to abandon the attempt. Review and validate any adaptations needed for the older release.

Stay on `release/v6.12.0.1-1` and run the following from the repository root to prepare the changelog, using that same branch as the MR target:

```bash
release changelog v6.12.0.1-1 --target-branch release/v6.12.0.1-1
```

Check that the generated changelog covers the intended backports, then review and merge its MR into the release branch. Ordinary pushes to release branches do not start a pipeline with the current CI rules. Run a `DEFAULT` pipeline manually on the release branch, confirm its SHA matches the release commit shown by `release status`, and check its results before tagging:

```bash
release status v6.12.0.1-1 --target-branch release/v6.12.0.1-1
release tag v6.12.0.1-1 --target-branch release/v6.12.0.1-1
```

The tag command checks specifically for a **push** pipeline, so it will still warn even after a successful manual pipeline. After verifying the exact release commit’s results, accept the prompt to continue without a successful push pipeline.

Use the same version and target branch throughout. Review the tag pipeline and publish using the steps above. For documentation-only fixes, see [Documentation backports](documentation-site.md#documentation-backports).

## Recovering from a partially failed publication

1. Check the failed job’s logs, the release-issue notes, and the destination repository or registry. A job may have uploaded artifacts before failing; other publication jobs may already have succeeded.
2. For a temporary infrastructure or authentication failure, fix the cause and retry the failed job in the original tag pipeline, using the same artifacts. Check downstream publication jobs afterwards, including automatic unstable and private-image publication.
3. Verify the intended packages or images are available and record the recovery in the release issue. Do not promote the release to `stable` while publication or validation remains incomplete.

If the fix requires changing the release contents, create a new release version and tag through the normal procedure. Do not move an existing tag or replace published artifacts with different contents under the same version. If the original build artifacts have expired, establish whether the exact artifacts can be recovered before retrying; otherwise prepare a new release.
