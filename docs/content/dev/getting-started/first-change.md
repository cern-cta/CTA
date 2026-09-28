# Your First Change

Use this guide to take a small change from your checkout to review. Before changing service code, complete [Environment Setup](environment-setup.md) and the [EOS archive and retrieve walkthrough](../reference/integrations/eos/walkthrough.md). For documentation-only changes, a local checkout is sufficient.

## Choose a focused change

Browse the [open issues labelled “good first issue”](https://gitlab.cern.ch/cta/CTA/-/work_items?state=opened&label_name%5B%5D=good%20first%20issue) for suggested starting points. You can also propose a small bug fix, missing test, or documentation correction on the [community forum](https://cta-community.web.cern.ch/) or by emailing [cta-support@cern.ch](mailto:cta-support@cern.ch).

Agree on the task and scope with the team before starting. Define what should be different when the change is complete and how you will verify it. Keep unrelated cleanup for a separate contribution.

Prepare your branch through the appropriate contribution route:

- **CERN GitLab:** Follow [Contributing through CERN GitLab](../contributing/gitlab.md) for issue tracking, branches, and merge requests.
- **GitHub:** Follow [Contributing through GitHub](../contributing/github.md). Work in your fork; a maintainer handles the GitLab issue, CI, and merge request.

## Make the change

Use [Project Structure](project-structure.md) to locate the component and its nearby tests. Read the relevant [coding conventions](../reference/conventions/coding/general.md), then make the smallest change that addresses the task.

For a bug fix, add or update a test that reproduces the failure and verifies the corrected behaviour. Update documentation when the change affects how CTA is configured or used.

## Build and test

Run commands from the repository root. Choose validation that exercises the change; the EOS client suite below checks common client workflows, not a substitute for a regression test specific to your fix.

### C++ changes

Build the packages and run the compiled unit tests:

```sh
cta-dev build --enable-unit-tests
```

Repeat this step as you edit. Building packages does not update the running development instance. The `client` suite exercises a broad range of CTA functionality through EOS and is a useful starting point for validation. It takes a few minutes to run, in addition to the build and deployment time:

```sh
cta-dev all client
```

This rebuilds packages and images, replaces the deployment in `dev`, and runs the full client suite with setup, verification, and teardown. It resets the development catalogue and scheduler; expect the data and fixtures from the introductory walkthrough to be replaced.

For selecting another suite or running specific tests, see [System tests with cta-dev](../reference/tools/development-workflow.md#system-tests). For backend choices or manual testing, see [Development Workflow](../reference/tools/development-workflow.md#common-workflows). Investigate failures before submitting the change, using [Debugging](../reference/tools/debugging.md) and [Working with Development Pods](../reference/tools/development-pods.md) as needed.

### Documentation changes

Follow [Documentation Changes](../contributing/documentation.md) to install the documentation dependencies, preview the edited pages, and run the strict build. You do not need to build or deploy CTA for a documentation-only change.

## Submit for review

In the description of your GitLab merge request or GitHub pull request, include:

- The problem being addressed, with a link to the issue or discussion.
- What changes for the user or developer after your fix.

Check [Writing Changelog Entries](../contributing/changelog.md) to determine whether changelog metadata is needed.

Follow [Contributing through CERN GitLab](../contributing/gitlab.md) for CERN GitLab or [Contributing through GitHub](../contributing/github.md) for the maintainer-assisted route. These pages cover submission requirements, automated checks, and handling review feedback. For GitLab, use the [MR checklist](../contributing/gitlab.md#mr-checklist) before requesting review.
