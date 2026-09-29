# Repository Administration

Procedures for maintaining project access, settings, and integrations. Agree changes to repository policy with the team before applying them. Contributor-facing rules belong in [Contributing through CERN GitLab](../gitlab.md).

## Access and permissions

Before granting access, confirm the contribution or responsibilities with the team. CERN GitLab developer access requires a CERN computing account; contacting the team does not guarantee that an account can be provided. See [Prerequisites & Access](../../getting-started/prerequisites.md).

Normal team members inherit repository permissions from CERN e-group membership, synchronized through LDAP at the **CTA group** level. To grant, change, or remove that access, update the relevant e-group membership through its administrators. LDAP-managed members cannot be added or removed directly from the GitLab group member list; synchronization can take up to one hour to appear.

Check the account’s inherited permissions before granting direct access. Add direct membership only in the **CTA repository**, and only when permissions beyond those inherited from e-groups are needed—for example, for an external contributor or additional maintainer responsibilities. Agree the role with the team and grant only the access needed.

When someone leaves or changes responsibilities, update the relevant e-group membership and review any direct repository access separately. Removing direct access does not remove inherited access. Transfer any schedules or integrations they own before removing access, and rotate affected credentials where necessary; see [CI Maintenance](ci-maintenance.md).

## Merge settings and branch protection

When changing project settings, preserve the [merge workflow](../gitlab.md#merge): merge trains, required approvals and successful CI, squashing with the project’s commit-message template, and source-branch deletion after merge. Inspect the active merge-request settings in GitLab rather than assuming they are all defined in the repository.

Check branch and tag protection alongside those settings:

- Keep `main` protected so ordinary changes go through reviewed MRs. Apply the agreed protection to maintained release branches as well.
- Restrict release-tag creation to the accounts responsible for publication. Keep protection patterns consistent with the tags produced by the [release tool](releases.md), including selected build variants and release candidates.
- Preserve protection of `gl-pages` and the publication permissions described in [Documentation Site](documentation-site.md#publishing).

After a settings change, check its effect on the next MR or relevant pipeline. Do not create a release tag merely to test protection settings: tag pipelines can publish documentation automatically.

## Issue and merge request templates

Issue templates live in `.gitlab/issue_templates/`; update them through an MR. The release tool uses `Release.md` when creating its tracking issue, so keep it aligned with [Release Procedure](releases.md).

The default MR description template is maintained in `.gitlab/merge_request_templates/Default.md`. Update it through an MR; GitLab uses the version on the default branch.

Leave the default MR description in GitLab project settings empty. Setting a template there overrides the repository template for new MRs, so changes to the checked-in file would no longer take effect. It does not modify the file itself. See [description-template precedence](https://docs.gitlab.com/user/project/description_templates/#priority-of-default-description-templates).

Keep the MR description’s `Description` and `Checklist` headings, placeholder text, and checklist items consistent with `ci/danger/Dangerfile`, which checks them. After editing, open the new-MR form to check the generated description and verify Danger on an MR using it. Coordinate template and check changes so contributors are not given a template that CI rejects.

Squash and merge commit-message templates are maintained in GitLab project settings, separately from the MR description template. Keep the MR title and reference, issue links, and editable changelog trailer consistent with [Changelog Entries](../changelog.md).

## Automated checks and bots

Maintain Danger rules in `ci/danger/Dangerfile` and its job in `.gitlab/ci/report.gitlab-ci.yml`. GitLab Duo review instructions live in `.gitlab/duo/mr-review-instructions.yaml`. Change these through reviewed MRs and check the resulting automated feedback before merging.

The [CTA Triage Bot](https://gitlab.cern.ch/cta/ci/cta-triage-bot) is maintained in a separate repository. It keeps issues and MRs up to date by posting warnings or reminders for missing information and closing stale items according to its policies. Update `policies/*-triage-policies.yml` through an MR in that repository; its README describes the current rules.

The bot runs through its own [scheduled pipeline](https://gitlab.cern.ch/cta/ci/cta-triage-bot/-/pipeline_schedules), so feedback is periodic rather than immediate. Agree policy and schedule changes with the team, especially changes that close issues or MRs, and check the next scheduled run’s results.

When renaming labels or changing required metadata, update affected checks, templates, and contributor guidance together. In particular, preserve the intended handling of `type::` labels, `changelog: required`, and `needs documentation`; their contributor-facing meaning is documented in [Track the work and prepare the MR](../gitlab.md#track-the-work).

Keep bot permissions limited to their tasks. For token replacement, follow [credential rotation](ci-maintenance.md#credentials-and-service-accounts).

## GitHub mirror administration

CERN GitLab is the authoritative repository; the [GitHub mirror](https://github.com/cern-cta/CTA) receives its published changes. Contributions from GitHub follow [Importing GitHub Contributions](github-contributions.md), rather than being merged independently on GitHub.

Inspect **Settings → Repository → Mirroring repositories** in GitLab for the destination, push direction, selected refs, authentication, and latest update status. Preserve the agreed branch/tag selection when changing configuration; see [GitLab push mirroring](https://docs.gitlab.com/user/project/repository/mirror/push/).

If the mirror stops updating:

1. Read the reported error and check destination permissions, expired credentials, and any divergent refs. Identify and preserve unexpected GitHub-only commits before resolving divergence.
2. Correct the cause, then use **Update now** to retry the mirror. Check its status after completion.
3. Compare the mirrored branch commit SHA with GitLab and confirm any expected release tags are present on GitHub.
