# Contributing through CERN GitLab

CERN GitLab is the primary route for contributing to CTA. It requires a CERN computing account and CTA project permissions; see [Prerequisites & Access](../getting-started/prerequisites.md). For pre-agreed tasks without direct GitLab access, use the [GitHub route](github.md).

## Track the work

Find or create an issue in the [CTA GitLab project](https://gitlab.cern.ch/cta/CTA/-/issues). Check for existing discussions first and use the relevant issue template. Describe the problem or proposed improvement, its scope, and the expected outcome; include reproduction details for bugs. Agree on substantial changes with the team before implementing them.

Use the project’s [current labels](https://gitlab.cern.ch/cta/CTA/-/labels): add one `type::` label, such as `type::bug` or `type::addition`, and ideally at least one scoping label identifying the affected component or context. Keep the issue’s assignee and status current while working on it.

## Prepare the merge request

Create a feature branch based on `main`, preferably from its GitLab issue so the work is linked automatically. Keep the generated issue number in the branch name. For work without an issue, choose a short, descriptive lowercase name with hyphens, such as `improve-build-diagnostics`; avoid underscores and spaces.

Open an MR targeting `main`, assign it to yourself, and keep it marked as a draft while work is in progress. Use a concise title describing the resulting change and its scope, for example `[scheduler] Fix ...`. The title becomes the default squash-commit summary.

Fill in the description and checklist supplied by GitLab. Replace placeholder text with the problem and what changes for the user or developer. Link the issue using `Closes #<issue-number>` if merging will complete it; otherwise, use a plain issue reference. Mention special validation or limitations where relevant to the review.

Apply the issue’s relevant labels to the MR. Danger requires exactly one `type::` label and warns when no additional scoping label is present. Two other labels are particularly useful to reviewers:

- **`changelog: required`** — the change needs a changelog entry. Follow [Changelog Entries](changelog.md) for title and commit-metadata guidance; the label alone does not generate an entry.
- **`needs documentation`** — include documentation updates in the MR. Danger requires changes under `docs/` when this label is present; reviewers check that the updates cover the change.

## Check CI

Check the MR’s pipeline results and automated comments. Address failed jobs and required corrections; explain intentional warnings and discuss failures that appear unrelated to your change with the team.

For an explanation of the jobs, see [CI Overview](../guides/testing/ci/index.md) and [CI Pipelines](../guides/testing/ci/pipelines.md). Start debugging a failed job with its log, then use available artifacts for test reports and diagnostic logs. See [Investigating CI failures](../guides/testing/ci/pipelines.md#investigating-ci-failures).

## Request and respond to review

### MR checklist

Before requesting review, check that:

- The MR targets `main`, has an assignee, and is marked ready.
- The title and description explain the change, and the issue is linked where applicable.
- The MR template is complete and labels are set, including `changelog: required` and `needs documentation` where applicable.
- Relevant tests and documentation accompany the change.
- Required CI jobs pass and automated feedback has been addressed.

### Review handoff

Assign a reviewer when the MR is ready; ask the team if you are unsure whom to choose. Reviewers check the implementation, scope, tests, and documentation, then submit their review so the author knows the feedback is ready to act on.

After receiving a review:

1. Reply in the review threads to explain how you addressed the feedback or discuss a different approach.
2. Push revisions to the same branch and check the updated CI results. Leave questions needing further discussion open.
3. When ready for another look, use [**Re-request a review**](https://docs.gitlab.com/user/project/merge_requests/reviews/#re-request-a-review) next to the reviewer’s name in the MR’s **Reviewers** section. This notifies the reviewer and creates a new to-do item. Add a short comment summarizing the changes or outstanding questions.

## Merge

Before merging, confirm that review feedback is resolved, the final revision has the required approvals, and CI passes. Resolve any conflicts or failures reported by GitLab.

CTA uses [GitLab merge trains](https://docs.gitlab.com/ci/pipelines/merge_trains/). Use GitLab’s merge controls to queue the MR, enable squashing, and use the provided commit-message template with the appropriate changelog metadata. GitLab validates the queued changes and merges them when the train’s checks succeed.

Select **Delete source branch** so GitLab removes the branch automatically. Issues linked with a closing instruction such as `Closes #<issue-number>` close automatically when the MR merges; plain references do not close them. If the MR is closed without merging, leave the issue’s status consistent with any work still needed.
