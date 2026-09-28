# Importing GitHub Contributions

Use this manual procedure to bring a [GitHub contribution](../github.md) into CERN GitLab for testing and merging. GitLab remains the authoritative repository; the GitHub mirror receives the merged result through the existing mirror update.

## Review and import

1. Agree on the scope with the contributor in the GitHub pull request. Create or identify the corresponding CTA GitLab issue and link it to the pull request.
2. Fetch the contributor’s branch into a local checkout and record the exact GitHub head commit SHA. In this example, `origin` is the CERN GitLab repository; replace the fork URL, contributor branch, and local branch name:

    ```bash
    # Update the GitLab baseline for comparison.
    git fetch origin main
    # Download the contributor’s proposed changes.
    git fetch https://github.com/CONTRIBUTOR/CTA.git contributor-branch
    # Keep this revision on a local branch for review and later push.
    git branch github-123-description FETCH_HEAD
    # Print the exact revision to record in the GitLab MR.
    git rev-parse github-123-description
    # List commits not yet in GitLab main.
    git log --oneline origin/main..github-123-description
    # Inspect changes since the branches diverged.
    git diff origin/main...github-123-description
    ```

    Save the SHA printed by `git rev-parse` for the GitLab MR description. The local branch pins the fetched revision even if the contributor later updates their GitHub branch.

3. Review the changes for security risks before importing them into GitLab or running them locally. Pay particular attention to CI configuration, build scripts, tests, and other code executed by the pipeline: imported code may be able to access internal services or credentials available to CI jobs. Resolve any concerns before starting a pipeline. Marking the MR as draft does not prevent CI from running.
4. Push the reviewed commits to a dedicated contribution branch in CERN GitLab, preserving their authorship. Follow the project’s [branch conventions](../gitlab.md#prepare-the-merge-request) for the GitLab branch:

    ```bash
    # Upload the reviewed branch to GitLab for an MR.
    git push origin github-123-description:github-123-description
    ```

5. Open a [merge request](../gitlab.md) targeting `main`. Record the GitHub pull request URL and imported head SHA in the description, together with any changes made during integration. Link the GitLab MR from the GitHub pull request.

A branch push alone does not start the normal feature CI with the current workflow rules. The GitLab merge request provides the MR pipeline and final approval process.

## Revisions and feedback

Keep contributor-facing discussion on GitHub. Relay actionable GitLab review feedback and relevant CI results there, excluding internal credentials or sensitive infrastructure details.

For each contributor update, fetch and review the new revision before updating the GitLab branch. Using the same local branch as above:

```bash
# Refresh the target branch for reviewing the complete contribution.
git fetch origin main
# Download the contributor’s updated branch.
git fetch https://github.com/CONTRIBUTOR/CTA.git contributor-branch
# Save the exact revision so another fetch cannot change what is reviewed.
github_head=$(git rev-parse FETCH_HEAD)
# Inspect changes since the previously imported revision.
git diff github-123-description "$github_head"
# Review the complete contribution against GitLab main as well.
git diff "origin/main...$github_head"
# Print the new GitHub SHA to record in the MR.
printf '%s\n' "$github_head"
```

After reviewing the changes, use a clean working tree to update the import branch:

```bash
# Switch to the branch used for the GitLab MR.
git switch github-123-description
# Advance it to the reviewed revision without replacing existing commits.
git merge --ff-only "$github_head"
# Push the reviewed update to the existing GitLab MR.
git push origin github-123-description:github-123-description
```

Stop if the merge or push is rejected. A contributor rebase or maintainer changes may require reconciling the branches; do not force-push over changes without reviewing them. Update the MR description with the new GitHub SHA and check the new CI results. Do not automatically synchronize incoming revisions into the GitLab project.

Ask the contributor to make revisions on their GitHub branch. If integration requires maintainer changes or rebasing, explain those changes on the pull request and keep the GitLab MR description accurate. CI and approval must apply to the final GitLab revision, including integration changes.

## Merge and close

1. Confirm that the final GitLab revision has passed the required CI and review checks. Results for an earlier GitHub revision are not sufficient.
2. Merge through the normal GitLab process. Preserve the contributor’s attribution in the resulting commit; when squashing, check the author and add co-author attribution where appropriate.
3. Let the normal GitLab-to-GitHub mirror update publish the resulting `main`. Do not merge the GitHub pull request separately.
4. Close the GitHub pull request with a link to the merged GitLab MR and resulting commit. Squashing may prevent GitHub from recognizing it as merged automatically.
5. Remove the temporary GitLab contribution branch according to the normal cleanup procedure.
