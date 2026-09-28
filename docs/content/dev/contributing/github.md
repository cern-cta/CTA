# Contributing through GitHub

For tasks agreed with the CTA team in advance, contributors without CERN developer access can submit changes through the [GitHub mirror](https://github.com/cern-cta/CTA). Before starting work or opening a pull request, post on the [community forum](https://cta-community.web.cern.ch/) or email [cta-support@cern.ch](mailto:cta-support@cern.ch) to agree on the scope and confirm maintainer availability for the GitLab handoff.

GitLab remains the authoritative repository for CI, final approval, and merging. A maintainer handles the GitLab side, following the same [CERN GitLab contribution conventions](gitlab.md) for issue tracking, MR preparation, review, CI, and merging. You do not need a CERN computing account for this contribution route.

!!! info "Maintainer availability"

    We welcome contributions. The GitHub route requires manual work from maintainers to review changes, transfer revisions to GitLab, run CI, and relay feedback. Our capacity for this work is limited, so please discuss the scope with the team before starting substantial work. Reviews and synchronization happen as maintainer time permits; we cannot commit to daily updates or a fixed turnaround time. Small, focused contributions and consolidated revisions help make this process manageable.

## Prepare and submit a change

1. Fork the GitHub mirror into your own GitHub account and follow [Prerequisites & Access](../getting-started/prerequisites.md#github-fork) to prepare your checkout.
2. Create a feature branch in your fork based on the mirror’s `main` branch.
3. Make your changes, following the [coding conventions](../reference/conventions/coding/general.md). Include relevant tests, documentation, and [changelog information](changelog.md).
4. Open a pull request against `cern-cta/CTA:main`. Describe the problem and what changes for the user or developer. Link any related discussion or issue.

## Review and CI

Use the GitHub pull request for discussion and revisions. A maintainer creates or links the corresponding GitLab issue, imports a reviewed revision into a GitLab branch, and opens a merge request for the normal CI and approval process. The two requests are linked so you can follow the handoff.

Push revisions to the same branch in your GitHub fork. The maintainer reviews and imports each updated revision into GitLab for testing; this handoff is manual. GitHub checks do not replace the required GitLab CI.

The maintainer relays actionable review feedback and relevant CI failures to the GitHub pull request. Keep contributor changes on the GitHub branch so the two requests stay aligned.

## Merging

After review and CI pass for the final GitLab revision, the maintainer merges the GitLab merge request. The resulting change reaches GitHub through the normal mirror update. The maintainer then closes the GitHub pull request with a link to the merged GitLab request or commit.

The GitHub pull request is not merged independently into the mirror. A squash merge in GitLab may leave GitHub showing the pull request as closed rather than merged; the closing comment records where the contribution was integrated.

Maintainers should follow [Importing GitHub Contributions](maintainers/github-contributions.md).
