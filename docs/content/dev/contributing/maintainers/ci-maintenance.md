# CI Maintenance

Procedures for maintaining CI infrastructure. For pipeline usage, logs, and test development, see [Testing and CI](../../guides/testing/ci/index.md).

## Weekly pipeline-image updates

The maintainer responsible for CI should take ownership of the weekly pipeline-image refresh and review and merge its update MR each week.

CI uses immutable, versioned images selected by `PIPELINE_IMAGE_VERSION` in `.gitlab-ci.yml`. Package updates are picked up when the refresh builds a new image set, so they cannot change the pinned images used by existing CI pipelines. A breaking package update should surface in the image-refresh build or the update MR’s pipeline before the new version is adopted.

Check that the `UPDATE_PIPELINE_IMAGES` pipeline has built the full image set, then review the generated version-update MR and its CI results and merge it through the normal GitLab workflow. If a build fails, investigate whether an upstream package update broke the image or CTA build and address the cause before merging.

To start a refresh manually, run a pipeline with `pipeline-type` set to `UPDATE_PIPELINE_IMAGES`. On the default branch, `update-pipeline-image-version` opens the MR automatically after the image builds; on other branches, start that job manually.

### Add or update an image dependency

Use this procedure when a CI job needs a new tool or a change to an existing image dependency. If the package itself must first be added or updated in `cta-dependencies`, follow [CTA Dependencies](dependencies.md). See [GitLab CI Conventions](../../guides/conventions/gitlab.md#execution-images-and-scripts) for deciding between image-build and runtime installation.

1. On a feature branch, update the Dockerfile for the image used by the affected job. Shared images live in `ci/docker/pipeline/`; platform-specific build and test images use `ci/docker/cta/<platform>/build.Dockerfile` and `test.Dockerfile`. Keep shared images platform-independent and put any dependency-specific explanation beside the installation command.
2. Push the branch and start a pipeline on it with `pipeline-type` set to `UPDATE_PIPELINE_IMAGES`. This builds and publishes a new, versioned image set from your changed Dockerfiles. Check that all image builds succeed.
3. Start the manual `update-pipeline-image-version` job. It opens a version-update MR targeting your feature branch. Review and merge that MR into the feature branch to adopt the new `PIPELINE_IMAGE_VERSION` there.
4. Run normal validation on the updated feature branch and check that the affected jobs succeed using the new images. Add any job changes that depend on the new tool after adopting the image version.
5. Submit the feature branch through the normal review and merge workflow. Include both the Dockerfile changes and the image-version update so the default branch adopts the tested images.

Changing a Dockerfile alone does not update the pinned images used by ordinary pipelines. If you revise the Dockerfile again, repeat the image build and version update before validating it.

## Scheduled pipelines

Agree with the team which pipelines should run on a schedule and how often. Create or edit those schedules in GitLab’s pipeline schedules; their target branch, timing, and inputs are visible there and do not need a separate inventory.

The maintainer responsible for CI should keep the schedules working, investigate failures, and take over ownership when needed. After changing a schedule, run it from the schedules page and check the result so that schedule-specific CI rules are exercised.

## Custom runner maintenance

Runner provisioning and host configuration are documented in [Tape Operations: CI](https://tapeoperations.docs.cern.ch/dev/ci/) (CERN-internal).

1. Open **Settings → CI/CD → Runners** in GitLab, find the runner, and pause it. Pausing stops new jobs; it does not stop jobs already running. See [GitLab’s runner controls](https://docs.gitlab.com/ci/runners/runners_scope/#pause-or-resume-a-project-runner).
2. Open the runner’s details and inspect its jobs. Wait until all active jobs have finished, including any still preparing or canceling. Do not start host maintenance while a job is active.
3. Apply the required updates using the internal setup procedure. From a CTA checkout, run the health check as the runner’s execution user with its normal Kubernetes context:

    ```bash
    bash ci/checks/check_runner_healthy.sh
    ```

4. Fix any failed checks, then resume the runner in GitLab. Open its job list and follow the next system-test job it picks up. If none is queued, retry `test-client` from a recent successful pipeline; for the stress runner, start `stress-test-python`.
5. Check the runner identity shown on the job page or at the start of its log, and confirm the job succeeds. A retry may be assigned to another eligible runner; in that case, follow a job actually assigned to the maintained runner. If it fails because of the maintenance, pause the runner again and resolve the problem.

## Credentials and service accounts

Do not schedule credential rotations on a Friday unless unavoidable; leave time during the working week to detect and resolve problems.

Rotate credentials at their provider, then update the corresponding **Settings → CI/CD → Variables** entry in GitLab, including inherited group variables where applicable. Preserve the variable’s protection, masking, and environment scope.

If the provider supports overlapping credentials, install the replacement before revoking the old one. If rotation invalidates the old credential immediately, coordinate a short interruption and update the CI/CD variable promptly; avoid starting affected jobs during the change.

For Kubernetes secrets, pause the affected runner and wait for its active jobs to finish as described above before rotating the credential and updating the secret. Update each affected cluster or namespace, then resume the runner. Check the next affected job for authentication or image-pull errors; retry jobs that failed during the rotation after the new credential is in place.
