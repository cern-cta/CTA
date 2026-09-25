# Automated Contribution Checks

There are two GitLab bots active: the Danger bot and the Triage bot. The aim of these bots is to reduce the amount of manual work needed by developers to keep the repositories clean.

## Danger Bot

The Danger bot runs as part of a CI job on every merge request. The Danger bot is meant to replace any "you forgot to do X" type comments on a merge request.
Each developer has to do a number of things to ensure that a merge request is clear to a reviewer (or to anyone else looking at it). This includes:

- A concise and descriptive title
- A clear description
- A descriptive set of labels

While automatically verifying the actual descriptiveness is a hard problem to automate, we can at the very least verify the presence of them. This is one of the main tasks of the Danger bot.
In addition to this, it can also produce various other warnings and errors depending on how the bot is configured. At the moment, it does the following:

- **Title**
    - Must begin with one of the allowed scope prefixes: `[CI]`, `[Misc]`, `[Tools]`, `[catalogue]`, `[frontend]`, `[scheduler]`, `[taped]`, `[rmcd]`
    - Must be 72 characters or fewer (soft limit)
- **Description**
    - Must not be empty
    - Must not contain the placeholder comment
    - Should be at least 40 characters (soft check)
    - Must include all required checklist acknowledgements
- **Labels**
    - Must include exactly one `type::` label (e.g., `type::bug`)
    - Must include exactly one `workflow::` label (e.g., `workflow::testing`)
    - Must include exactly one `priority::` label (e.g., `priority::high`)
    - Should include at least one additional label for scoping/context
- **Assignee**
    - Must have an assignee set
- **Reviewers**
    - Should have at least one reviewer assigned
- **Merge Request Size**
    - Warns if more than 500 lines are changed

All this is defined in `ci/danger/Dangerfile` in the CTA repository.

## Triage Bot

The goal of the triage bot is to ensure issue and merge request hygiene. Without this, issues and merge requests can be forgotten, leading to a lot of pollution in a given repository.
It is then up to the developers to take the initiative to clean all this manually. The triage bot solves this issue (and more).

The triage bot runs as part of a scheduled pipeline in the CTA Triage Bot repository. It runs once per day.
While the specifics of the triage bot policies can be found in that repository, it has a few main policies that it enforces on every repository in the CTA group:

- Marking old inactive issues/merge requests as stale
- Closing issues/merge requests that have been stale for too long
- Enforcing correct labelling of issues
- Enforcing issue completeness by checking that there is an assignee and a description
