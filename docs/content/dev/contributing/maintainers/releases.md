# Release Procedure

Use the `release` tool to prepare the changelog and release issue, tag the reviewed commit, and follow its CI pipeline through validation and promotion.

## Set up the release tool

Start from a clean checkout of the target branch (`main` by default), with Python 3, Git, and `origin` pointing to CERN GitLab.
Run commands from the repository root:

```bash
export PATH="$PWD/ci/release:$PATH"
release --help
```

The tool reads a GitLab personal access token with `api` scope from `GITLAB_TOKEN` or `~/.config/cta/gitlab-api-token`, or prompts for one.
The token needs permission to create release issues and MRs; Git pushes use your normal repository authentication, which must allow protected release tags.

## Prepare the release

Choose an unsuffixed version such as `v6.1.0` using `v<major>.<minor>.<patch>`:

| Field | When it changes |
| --- | --- |
| `major` | The major software series, matching `majorVersion` in `project.json`. |
| `minor` | A new feature release. |
| `patch` | Bug fixes, backports, or packaging and build changes. |

See [Versioning](../../guides/conventions/versioning.md) for artifact suffixes and [Catalogue Schema Releases](catalogue-schema-releases.md) for schema upgrades.

```bash
release changelog v6.1.0
```

The tool synchronizes the target branch and opens generated changes since the previous final release in Git’s editor.
Review the entries, including commits with missing Changelog trailers, then confirm to create the release issue and changelog MR.
Add release requirements and deployment considerations to the issue, merge the MR, and check the resulting branch pipeline.

```bash
release status v6.1.0
```

Status shows the issue and MR, then published tags and their pipelines, including child variants.
Pass a specific prerelease version to inspect only that candidate.

## Tag the release

```bash
release tag v6.1.0
```

The tool selects the changelog MR’s merge commit, checks its metadata, target-branch ancestry, and pipeline, then asks you to edit the tag description and confirm publication.
Resolve any warnings before proceeding.
Use `release --dry-run tag ...` to preview without changes; `--yes` skips confirmations but does not bypass the pipeline requirement.
Use `--allow-unsuccessful-pipeline` only when intentionally tagging without a successful pipeline for the selected commit.

Only `.devN` and `.rcN` are accepted as tag suffixes.
Development releases do not require a changelog entry, merged changelog MR, or release issue.
RCs and final releases require the base version’s changelog entry and merged changelog MR, and default to that MR’s merge commit.

```bash
release tag v6.1.0 --development
release tag v6.1.0 --release-candidate
```

Use `--ref` to select a specific commit or tag reachable from the target branch.
Prepare and merge the changelog before tagging an RC.
To include later fixes, use `--ref` with a commit containing the changelog merge and entry; promote the tested candidate with the same explicit reference:

```bash
release tag v6.1.0 --release-candidate --ref FIXED_COMMIT_SHA
# Promote the exact tested candidate to a final tag.
release tag v6.1.0 --ref v6.1.0.rc2
```

See [Versioning](../../guides/conventions/versioning.md#release-artifact-versions) for the tag-to-artifact mapping and [local and CI development builds](../../guides/conventions/versioning.md#local-and-ci-development-builds) for untagged builds.

## Review CI results

Tags automatically build EL9/objectstore in the main pipeline and EL9/pgsched in a child, both with Oracle.
Start other EL9/EL10 combinations manually from the tag pipeline’s build triggers.
Backend and platform suffixes are added automatically.

Review unit and system tests, and catalogue migration results for schema upgrades.
Stress tests run automatically with Oracle and remain manual without it; dCache tests are manual unless explicitly enabled.
**Stress-test failures do not fail the pipeline**, so check the job itself and record validation evidence in the release issue.
For logs and artifacts, see [Testing and CI](../../guides/testing/ci/index.md).

## Publish packages and images

Publication follows this policy for each selected variant, including manually triggered variants:

| Job | Publication |
| --- | --- |
| `internal-release-cta` | Automatic on tags after package builds. |
| `public-release-cta-unstable` | Automatic for RC and final tags after internal publication; manual for dev tags. |
| `public-release-cta-testing` | Automatic for dev tags after internal publication; manual for RC and final tags. |
| `public-release-cta-stable` | Manual for final tags after internal and unstable publication. |
| `publish-images-private` | Automatic on tags after internal publication and image builds. |
| `publish-images-public` | Automatic after stable publication and image builds, only without Oracle support. |

Dev/RC tags can publish to `unstable` and `testing`, but cannot publish to `stable`.
Publication does not wait for all tests; review their results before manual promotion, and promote to `stable` only after production validation.
Check publication jobs and their status notes on the release issue.
Documentation publishes independently from protected final release tags; see [Documentation Site](documentation-site.md).

For stable releases, create a GitLab [Release entry](https://gitlab.cern.ch/cta/CTA/-/releases) and an announcement on the [CTA Community forum](https://cta-community.web.cern.ch/), including the changelog and upgrade instructions.
Close the release issue when its checklist is complete; stable promotion and announcements may happen later.

## Maintenance releases and backports

Create the maintenance branch from the release being fixed and cherry-pick the reviewed fixes:

```bash
git fetch origin --tags
git switch -c release/v6.1.1 v6.1.0
git cherry-pick -x FIX_COMMIT_SHA
git push -u origin release/v6.1.1
release changelog v6.1.1 --target-branch release/v6.1.1
```

Review and merge the changelog MR into that branch.
Pushes to `release/*` branches start CI automatically.
Verify the successful pipeline’s SHA matches the changelog commit shown by `release status`.
Use the same target branch when tagging:

```bash
release status v6.1.1 --target-branch release/v6.1.1
release tag v6.1.1 --target-branch release/v6.1.1
```

The tool accepts a successful push or manually started pipeline for the exact commit and target branch.
Continue with tag-pipeline validation and publication as above.
For documentation-only fixes, see [Documentation backports](documentation-site.md#documentation-backports).

## Recover from failed publication

Inspect the failed job and destination: some artifacts may already have been published.
For infrastructure failures, retry the original job with the same artifacts, check downstream publication, and record the outcome in the release issue.
If artifacts have expired, recover the originals or prepare a new release.
If release contents need changing, use a new version; never move published tags or replace artifacts with different contents under the same version.
