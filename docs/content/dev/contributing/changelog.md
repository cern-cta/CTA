# Changelog Entries

The changelog tells users what changed between releases. It lives in the repository’s `CHANGELOG.md` and is published as [Changelog](../../changelog.md).

The **`changelog: required` MR label** tells reviewers that an entry is needed. The **`Changelog:` trailer on the final squash commit** controls its inclusion and category during generation. The label alone does not generate an entry.

## When an entry is needed

Include an entry for user-visible changes: new features, bug fixes, configuration or command-line changes, package changes, deprecations, and performance or security improvements.

Changes affecting only development, such as test updates, CI maintenance, or internal refactoring, normally do not need an entry. Judge the effect on users rather than the files changed: a refactoring that also fixes user-visible behaviour still warrants an entry.

Avoid separate entries for intermediate fixes to a feature that has not yet been released. The changelog should describe the final behaviour of that feature.

## Prepare the entry

1. Add **`changelog: required`** to the MR when an entry is needed.
2. Write the MR title as the intended commit summary and changelog text.
3. When merging, check the final squash-commit message in GitLab. Replace the template’s category alternatives with exactly one supported trailer, such as `Changelog: fix`. If no entry is needed, remove the entire `Changelog:` line.

For the title:

- Start with a scope prefix accepted by Danger, then an imperative verb such as “Add”, “Fix”, or “Remove”. Danger reports the current accepted prefixes.
- Describe the user-visible outcome rather than the implementation details.
- Keep it concise; aim for 72 characters or fewer. GitLab’s changelog uses the commit title, which can be truncated for long commit subjects. The 72-character recommendation leaves room for the MR reference appended by the squash template.

GitLab’s [changelog documentation](https://docs.gitlab.com/user/project/changelogs/) identifies the entry title as the commit title. Its [commit implementation](https://gitlab.com/gitlab-org/gitlab/-/blob/master/app/models/commit.rb) truncates subjects of 100 characters or more to a shorter title ending in `...`. Keep the final squash-commit subject, including its MR reference, below that threshold.

Describe the outcome users care about. Supporting work such as updating tests, documentation, or internal helpers normally does not belong in the title: it is part of delivering the change.

| Avoid | Prefer | Why |
| --- | --- | --- |
| `[Tools] Add storage-class filtering and update system tests` | `[Tools] Add storage-class filtering to archive file listings` | Name the capability and where it applies; test updates are supporting work. |
| `[taped] Improve error handling` | `[taped] Fix crash when reopening log files` | Identify the observable problem being fixed. |
| `[scheduler] Fixed incorrect mount selection` | `[scheduler] Fix incorrect mount selection` | Use the imperative mood after the scope prefix. |
| `[frontend] Add a null check in the request handler` | `[frontend] Fix crash on malformed requests` | Describe the effect rather than the implementation technique. |

These are illustrative titles. If a change affects only tests or documentation, name that work in the MR title, but normally omit the changelog trailer.

A completed squash-commit message might look like this, using illustrative MR and issue numbers:

```text
[taped] Fix crash when reopening log files (cta/CTA!1234)

Closes #1234

Changelog: fix
```

Keep the MR reference and relevant issue links from GitLab’s template. The title conventions also apply to MRs that do not need a changelog entry; omit the trailer for those changes. For [GitHub contributions](github.md), the maintainer handling the GitLab merge prepares the final commit metadata.

## Categories

Use one of the values configured in `.gitlab/changelog_config.yml`:

| Value | Use for |
| --- | --- |
| `addition` | New features or capabilities. |
| `fix` | Bug fixes. |
| `change` | Changes to existing behaviour. |
| `deprecation` | Features marked for future removal. |
| `removal` | Removed features or interfaces. |
| `security` | Security fixes or improvements. |
| `performance` | Performance improvements. |
| `other` | User-visible changes that do not fit another category. |

Deprecation and removal happen at different stages: use `deprecation` when a feature remains available but users should stop relying on it and migrate to an alternative. Use `removal` when the feature is actually removed. A deprecation entry should identify the alternative and, if known, the planned removal release; the later removal needs its own entry.

Ordinary contributions prepare the commit metadata rather than editing `CHANGELOG.md` directly. During release preparation, the tooling generates a draft from commit trailers and maintainers review it before publication. See [Release Procedure](maintainers/releases.md) for that workflow.
