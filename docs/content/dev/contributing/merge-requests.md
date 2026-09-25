# Merge Requests

No pushing is allowed directly to `main`, so any update to the CTA codebase must go through a merge request.

**The developer should:**

1. Review the MR title.
    - The title should be clear, concise and suitable as a changelog entry. By default, the title of the MR will become the commit summary in the squashed commit.
    - **Important**: As a general rule: `MR title = squashed commit message = changelog entry`. As such, carefully read through the `Writing good changelog entries` section to write a good MR title
    - Don't forget to remove the `Resolve: ` prefix!
2. Fill the description:
    - Add a summary/list of the changes that were made. Developers looking at the MR should get an idea of what the MR updated just by looking at the description.
    - Clarify if the MR requires any documentation updates using the checkbox. If so, update the relevant pages under `docs/content/` in the same MR.
    - Provide links to the corresponding issue if not already present.
3. Ensure the changes are finalised and that the CI passes.
4. Assign a reviewer:
    - Usually, the reviewer is simply the next name on a "round robin" list of CTA developers, so code reviews are spread evenly around the team. There is no central list for this at the time of writing, so best to sync internally with the dev team.
    - The developer can choose a specific reviewer if they want, for example because they want the opinion of someone with a particular expertise.
5. Wait for a review and resolve any issues raised by the reviewer.

If the MR has been approved and the CI pipeline passes the MR is ready to be merged. After this, the developer can merge the branch to `main`. **Commits should be squashed when merging**, so that only a single commit is merged.

- When merging the changes back into `main`, the branch must be rebased on top of `main` (`git rebase origin/main`). The changes should then be retested, and any conflicts resolved (see [git rebase documentation](https://git-scm.com/docs/git-rebase)).
- If needed, push the rebased feature branch with `git push --force-with-lease`. Be aware that pushing with `--force` is dangerous and potentially destructive! Make sure to use [--force-with-lease](https://git-scm.com/docs/git-push#Documentation/git-push.txt---force-with-leaseltrefnamegtltexpectgt) to (slightly) reduce these dangers.
- After the MR has been merged or closed, the issue should be closed as well if not done automatically.

!!! info

    The [Danger Bot](automated-checks.md) runs on merge requests and will automatically perform a number of checks. Be sure to address these as quickly as possible and ideally before assigning a reviewer.

**The reviewer should:**

1. Check that the MR title is clear, concise and suitable as a changelog entry. See the [Changelog page](changelog.md).
    - Even if the MR contains only developer-facing changes, the MR title (and consequently the squashed commit message) should read as a changelog entry. This ensures that viewing the commit history gives developers a good idea of all the changes that happened (not just the user-facing changes).
2. Confirm that the MR description summarises the changes correctly.
3. Confirm whether documentation changes are needed.
4. Look at the code changes and make comments.
5. Approve the MR once all issues raised by the reviewer have been addressed


## Squashing Commits

While working on the code, you will be making multiple commits. Before this is merged into `main`, these must be squashed into a single well-defined commit. Commits should be focussed; they should be adding and/or fixing one particular thing. This is important for other people inspecting the code or in case of e.g. a rollback. If your commit cannot be properly described by a single hypothetical changelog entry, then you should split up your MR.

If you want to squash all commits into a single commit, then you are highly advised to do this using the GitLab UI in the MR itself as this makes it extremely easy and automatically provides the correct squash commit template. Any commit that ends up in `main` **must** follow squash commit template in the GitLab UI.
Adding an additional description to the commit is optional.

## Manually squashing commits (not recommended)

If you want to manually squash commits, make sure you follow the exact same squash commit template as on GitLab.

Start by identifying the commits in your history:

```sh
> git log --oneline
330ccecf5 (HEAD -> main) Add semi-related feature 2
673d6fc95 Last commit, trust me
3f52ec780 Oh no I forgot something
7b505e72d Fix bug to get feature 1 working
25d2b370b Add feature 1
50e19209c (origin/main, origin/HEAD) Changelog versioning update
```

Identify until which commit you want to squash. In this case, the previous commit from the main branch was `50e19209c` (`Changelog versioning update`). Now you can start an interactive rebase from this commit, which will allow you squash commits starting at this commit. Note that the commit you provide to the `rebase` command is exclusive; this means that this commit will not be part of the rebase.

```sh
git rebase -i 50e19209c
```

Will open the following in your configured git text editor:

```vim
pick 25d2b370b Add feature 1
pick 7b505e72d Fix bug to get feature 1 working
pick 3f52ec780 Oh no I forgot something
pick 673d6fc95 Last commit, trust me
pick 330ccecf5 Add semi-related feature 2
```

These commits are ordered in reverse chronological order. For the purpose of squashing, we can mark the commits as one of the following:

- `pick` keeps the commit as is.
- `squash` combines this commit with the previous one and lets you edit.
- `fixup` combines this commit with the previous one but discards its commit message (useful if you want to simplify the commit history).

If you want to squash everything into a single commit, we can mark the commits as follows:

```vim
pick 25d2b370b Add feature 1
squash 7b505e72d Fix bug to get feature 1 working
squash 3f52ec780 Oh no I forgot something
squash 673d6fc95 Last commit, trust me
squash 330ccecf5 Add semi-related feature 2
```

After this, you will be able to edit the commit message of the new squashed commit.

If you want to squash this commit history into two separate commits, you can do e.g.:

```vim
pick 25d2b370b Add feature 1
squash 7b505e72d Fix bug to get feature 1 working
squash 3f52ec780 Oh no I forgot something
pick 673d6fc95 Last commit, trust me
squash 330ccecf5 Add semi-related feature 2
```

In this case, we will get two separate commits. The first commit combining `25d2b370b`-`3f52ec780` and the second commit combining `673d6fc95`-`330ccecf5`.

Once again, you will be able to edit the commit message of both of these new squashed commits. When doing so, ensure that you follow the Git commit template specified above.

Once done with the squashing, we have rewritten history (and they said it couldn't be done). This has the unfortunate side-effect that we cannot directly push to our branch. Instead, we will have to force push (not in the jedi way) to the branch that we are currently on. Force pushing is always dangerous as it overwrites the remote history, potentially erasing commits. As such, if you screw it up there is no way to get it back without a backup. As such, it does not hurt to have a quick backup somewhere of the branch in case something goes wrong. With all these warnings out of the way, execute the following to update the remote branch with the new git history:

```sh
git push --force-with-lease
```

Note that we use `--force-with-lease` instead of `--force`. This prevents scenarios in which you are overwriting commits that are on the remote, but were not on your local branch (e.g. because someone else added them and you did not pull the changes). See it as a small extra safeguard.

At this point the added commits should be clean, compliant with the required format and ready to be merged into the `main` branch.
