
# Writing Changelog Entries

The changelog can be found in `CHANGELOG.md` in the root of the CTA repository. It is used to convey relevant changes in CTA for a given release to the user.
It is also published in the documentation as [Release Notes](../../release-notes.md).
Changelog entries are automatically generated based on the contents of the commit that will be merged before every release.

## What warrants a changelog entry

- Any user-facing change should have a changelog entry. Examples:
    - Feature additions/changes
    - Most bug fixes
    - Changes in the config file structure or commandline interface of the various CTA packages
    - Name changes to a CTA package
- Any developer-facing change should **not** have a changelog entry. Examples:
    - Changes to the CI
    - Update to README files
    - Updates to the tests
    - Refactorings
- If a feature is introduced and then fixed or changed **within** the same release, the fix/change should **not** have a changelog entry.

Remember, the changelog is for users to understand what changed in the software between releases.
For a full overview of what changed, developers can always look at the commit history.

## Writing good changelog entries

Changelog entries should:

- start with a verb in the imperative mood. If in doubt, just think: "this commit will `<commit-title>`" Some examples:
    - **Good**: [rmcd] Fix smc request handling always hitting 5s timeout
    - **Good**: [scheduler] Update scheduler to handle multiple frontend connections
    - **Bad**: Fixed issue where logs were not written correctly `-> past tense, not imperative`
    - **Bad**: Retry logic for repack implemented `-> noun phrase, not imperative`
    - **Bad**: Fixes retry logic `-> third person, not imperative`
    - **Bad**: Adding new support for something `-> gerund, not imperative`
- be concise and descriptive. Examples:
    - **Good**: [taped] Add support for log rotation on CTA tape daemons
    - **Good**: [Misc] Bump EOS version to 5.8.2
    - **Bad**: Logging improvements `-> vague`
    - **Bad**: Miscellaneous bug fixes and improvements `-> meaningless`
    - **Bad**: Fix queueing issues `-> not descriptive about what issue is being solved`
- focus on the end-result instead of the implementation
    - **Good**: [frontend] Fill xrd::cta::response field in case of grpc error
    - **Good**: [taped] Fix taped core dumping due to logging concurrent modifications
    - **Bad**: Refactored the repack manager class to add a new retry field `-> too many implementation details`
    - **Bad**: Updated variable names for consistency `-> unless it affects behavior, not user-relevant`
    - **Bad**: Used a try-catch block in logwriter.cpp `-> talks about how, not what or why`
- not have unnecessary capitalization
- not contain spelling errors
- not start with `Resolve "`

> Note that some of the above examples could be a bit more descriptive. However, try to keep the entries/commit summaries under 72 characters in length.

It is typically useful to know the scope of the commit. As such, adding a prefix is recommended if suitable. Currently, we have the following (non-exhaustive) list of prefixes:

- `[CI] My commit message`
- `[Misc] My commit message`
- `[Tools] My commit message`
- `[catalogue] My commit message`
- `[frontend] My commit message`
- `[scheduler] My commit message`
- `[taped] My commit message`
- `[rmcd] My commit message`

For changes that are mechanical, stylistic, or span multiple components (e.g., formatting, logging adjustments, or comment changes), use the `[Misc]` prefix. If a change is logically scoped to one component, prefer using the relevant prefix. Split commits when that improves clarity.

## How to generate a changelog entry

Changelog entries are generated automatically right before the release of a new CTA version. A changelog entry is generated for every commit containing a `Changelog: ` trailer.
By default, you will see the following squash commit template in the GitLab UI of merge requests:

```txt
%{title} (%{reference})

%{issues}

Changelog: addition/fix/change/deprecation/removal/security/performance/other -- remove this line if no changelog entry required
```

The last line is what determines if and where your commit ends up in the changelog. The following options are available:

- `addition`: for any new features added
- `fix`: for any bug fixes
- `change`: for any features that changed in functionality
- `deprecation`: for any changes that deprecated (but did not remove) functionality
- `removal`: for any changes that removed functionality
- `security`: for any serious security fixes/changes
- `performance`: for any changes improving performance but not changing functionality
- `other`: for any changes that do not fall in the categories above

As described above, not every commit should end up in the changelog. If your commit should not end up in the changelog, simply remove the last line; ensure that the `Changelog: ` trailer is not present.
