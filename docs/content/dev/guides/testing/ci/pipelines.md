# CI Pipelines

Use this guide to choose a pipeline, interpret its results, and investigate failures. For the wider validation process, see [CI Overview](index.md). Pipeline inputs are defined in `.gitlab-ci.yml`; individual jobs and their implementation details live under `.gitlab/ci/`.

## Pipeline design

Each pipeline has a defined purpose and a selected configuration: platform, scheduler, Oracle support, and other inputs. Additional configurations can run as separate child pipelines using the same CI definition with different inputs. This keeps each variant's results and build artifacts identifiable and lets developers request additional coverage when needed.

Pipeline types select the workflow, such as normal validation, dependency regression testing, or pipeline-image updates. They are distinct from build variants: a PostgreSQL scheduler child pipeline can still use the `DEFAULT` type.

Jobs use explicit `needs` dependencies so independent work can run in parallel once its prerequisites are ready. Shared runners and resources still limit concurrency, particularly for system and stress tests. Longer or specialized checks may be manual or scheduled to keep routine feedback timely.

CI jobs use pinned execution images so package updates do not silently change the environment of an existing pipeline definition. Updating those images is a separate [maintenance workflow](../../../contributing/maintainers/ci-maintenance.md#weekly-pipeline-image-updates).

## Choose and run a pipeline

Merge requests and pushes to the default branch normally trigger validation automatically. To run a pipeline explicitly, open the [CTA pipelines page](https://gitlab.cern.ch/cta/CTA/-/pipelines), choose **New pipeline**, select the branch or tag, and set the inputs for the intended test.

Ordinary feature-branch pushes do not create push pipelines. Use the branch’s MR pipeline or start a pipeline manually if you need validation before opening an MR.

| `pipeline-type` | Use |
| --- | --- |
| `DEFAULT` | Validate the selected revision with the configured builds and tests. |
| `REGR_AGAINST_CTA_BRANCH` | Test a dependency combination, either building CTA from the selected revision or using an existing CTA image. |
| `REGR_AGAINST_CTA_VERSION` | Compatibility alias for `REGR_AGAINST_CTA_BRANCH`; use the latter for new runs. |
| `UPDATE_PIPELINE_IMAGES` | Rebuild the images used to execute CI jobs and propose updated image tags. See [CI Maintenance](../../../contributing/maintainers/ci-maintenance.md#weekly-pipeline-image-updates). |

For a regression pipeline, the main inputs are:

- `custom-cta-image-tag`: use an existing CTA image instead of building the selected Git revision. Leave empty to build that revision.
- `custom-eos-image-tag`: select the EOS image to test.
- `custom-xrootd-version`: select the XRootD package version for builds.

Use the input descriptions in the pipeline form for supported formats and defaults. Select compatible scheduler and Oracle settings for the chosen images. A dependency build option does not change packages already installed in an existing CTA image. The selected Git revision still determines the pipeline configuration and test code.

[Scheduled pipelines](https://gitlab.cern.ch/cta/CTA/-/pipeline_schedules) provide additional periodic coverage; investigate their failures even when the MR pipeline passed. Schedule administration and pinned CI-image updates are covered in [CI Maintenance](../../../contributing/maintainers/ci-maintenance.md).

## Read pipeline results

Use the live graph on the GitLab pipeline page to inspect the jobs and dependencies for that run. Job availability depends on the pipeline source, inputs, and changed files. Stages group jobs; explicit `needs` dependencies also control when jobs can start.

### Job status

- **Failed:** inspect the job log and reports to identify the failed check.
- **Manual:** the job needs an explicit start. Whether it blocks the pipeline depends on its configuration.
- **Skipped:** the job did not execute; this is not evidence that its checks passed.
- **Allowed to fail:** inspect the result even if the overall pipeline succeeds, particularly for release validation.

### Merge-train validation

In merge-train pipelines, the guard can avoid repeating validation when it finds an equivalent successful pipeline. Guarded jobs may finish successfully without rerunning their checks. Open `check-merge-train-duplicate` to see the decision and the earlier pipeline used as evidence. If the guard cannot establish equivalence, validation runs normally.

### Child pipelines and external checks

Follow downstream pipeline links when additional configurations are tested there. For asynchronous external checks, a successful trigger only confirms that the check was started; inspect its own result. Confirm that the pipeline and reports correspond to the revision under review.

## Useful jobs

| Job | When it matters |
| --- | --- |
| `danger-review` | Read the MR feedback and address contribution-metadata and documentation findings. See [Contributing through CERN GitLab](../../../contributing/gitlab.md). |
| `test-catalogue-schema-update` | Review migration results when changing catalogue schemas or migrations. See [Testing Migrations](../../../internals/components/catalogue/testing.md). |
| `stress-test-python` | Assess sustained workload behaviour and performance using [Stress Tests](../stress-tests.md). |
| `build-cta-rpms-no-ccache` / `reset-ccache` | If you suspect a compilation-cache problem, compare the no-ccache build where available. The manual `reset-ccache` job clears the MR branch's cache; retry the affected build afterwards. |

For publication jobs and release validation, follow [Release Procedure](../../../contributing/maintainers/releases.md). When modifying jobs, keep their rationale beside the YAML and follow [GitLab CI Conventions](../../conventions/gitlab.md).

## Investigating CI failures

Open the failed job and read its log to identify the failing command or test.

### Reports, logs, and artifacts

When jobs publish test or code-quality reports, use GitLab’s pipeline and MR report views to find individual failures and diagnostics, then open the relevant job log for context.

When the job publishes artifacts, browse or download them from its GitLab job page. Depending on the job, these may contain test reports, build outputs, or diagnostic logs such as `test_logs/`. Availability depends on what the job collected and whether the artifacts have expired. See [GitLab job artifacts](https://docs.gitlab.com/ci/jobs/job_artifacts/) for the download options.

Artifacts belong to the job and pipeline that produced them. For a child pipeline, open the child job to obtain its outputs; check the revision and variant before using downloaded packages or reports. Download evidence you need before its retention period expires.

### Download artifacts locally

The repository provides a helper that downloads and extracts one job’s artifacts. From the repository root:

```sh
./ci/ci-download-artifacts.sh <job-id-or-job-url>
```

The helper requires GitLab API authentication and prompts for a token when needed. By default, it extracts artifacts under `tmp/ci-job-artifacts/<job-id>/`; use `--help` for options.

### Investigate core dumps with ci-debug

From the repository root, use `ci-debug` to open an interactive debug container for a CI pipeline:

```sh
./ci/ci-debug.sh <pipeline-id-or-url>
```

The script downloads artifacts from failed system-test jobs and mounts them under `/artifacts` in the pipeline's debug image. It starts the pipeline's manual debug-image build if needed. Add `--job <job-name>` to select a specific system-test job; for a child pipeline, supply that child's pipeline ID or URL.

It requires Podman, `curl`, `jq`, and `unzip`, and prompts for GitLab API authentication (a token with the `api` scope) and container-registry login when needed. The pipeline must provide the debug-image job and retain the relevant artifacts.

### Reproduce or retry a failure

Use the collected evidence to reproduce the failure with the relevant [local test suite](../system-tests.md) or [debugging workflow](../../tools-and-environment/debugging.md). If the failure appears unrelated to your change, discuss it with the team.

Retry a job when a transient failure has been resolved and you want to rerun it for the same revision and pipeline inputs. Start a new pipeline when changing the revision or inputs. Inspect the cause before retrying; repeated retries are not a substitute for investigating a reproducible failure.
