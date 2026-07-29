# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Repository-scoped Git operations for the CTA release CLI."""

from __future__ import annotations

import subprocess
from pathlib import Path
from collections.abc import Sequence


class GitError(RuntimeError):
    """A failure to validate or operate on the local Git repository."""


class Git:
    """Run safe, repository-scoped Git operations for release workflows."""

    def __init__(self, root: Path, dry_run: bool = False, allow_unclean: bool = False):
        """Create a Git runner rooted at a repository."""
        self.root = root
        self.dry_run = dry_run
        self.allow_unclean = allow_unclean

    def run(self, args: Sequence[str], mutate: bool = False) -> str:
        """Run Git with an argument list, printing mutations during dry runs."""
        command = ["git", *args]
        if mutate and self.dry_run:
            print("DRY-RUN:", " ".join(command))
            return ""
        try:
            result = subprocess.run(
                command,
                cwd=self.root,
                check=True,
                text=True,
                capture_output=True,
            )
        except FileNotFoundError as error:
            raise GitError("Required command 'git' is not installed") from error
        except subprocess.CalledProcessError as error:
            detail = error.stderr.strip() or error.stdout.strip()
            raise GitError(f"{' '.join(command)} failed: {detail}") from error
        return result.stdout.strip()

    def validate_repository(self, branch: str, remote: str, fetch: bool = True) -> str:
        """Validate a clean checkout synchronized with its remote branch."""
        if self.run(["rev-parse", "--show-toplevel"]) != str(self.root.resolve()):
            raise GitError(f"Run release from repository root {self.root}")
        if self.allow_unclean:
            print(
                "WARNING: --allow-unclean skips worktree and current-branch checks; "
                f"the release commit is still selected from {branch}."
            )
        else:
            if self.run(["status", "--porcelain"]):
                raise GitError("Working tree is not clean; commit or stash changes before releasing")
            current = self.run(["branch", "--show-current"])
            if current != branch:
                raise GitError(f"Current branch is {current!r}; switch to {branch!r} before releasing")
        if fetch:
            self.run(["fetch", "--tags", remote, branch], mutate=True)
        local = self.run(["rev-parse", branch])
        remote_sha = self.run(["rev-parse", f"{remote}/{branch}"])
        if local != remote_sha:
            raise GitError(
                f"{branch} is not synchronized with {remote}/{branch} (local {local[:12]}, remote {remote_sha[:12]})"
            )
        return local

    def resolve_tag_target(
        self,
        remote: str,
        default_branch: str,
        ref: str | None = None,
        fetch: bool = True,
    ) -> tuple[str, str]:
        """Validate the worktree and resolve a requested tag target to a commit."""
        if self.run(["rev-parse", "--show-toplevel"]) != str(self.root.resolve()):
            raise GitError(f"Run release from repository root {self.root}")
        if self.allow_unclean:
            print(
                "WARNING: --allow-unclean skips the clean-worktree check; "
                "the requested tag target is still resolved explicitly."
            )
        elif self.run(["status", "--porcelain"]):
            raise GitError("Working tree is not clean; commit or stash changes before tagging")
        if fetch:
            self.run(["fetch", "--tags", remote, default_branch], mutate=True)
        resolved_ref = ref or f"{remote}/{default_branch}"
        try:
            commit = self.run(["rev-parse", "--verify", f"{resolved_ref}^{{commit}}"])
        except GitError as error:
            raise GitError(
                f"Tag target {resolved_ref!r} does not resolve to a commit; "
                "use a commit SHA, local branch, remote-tracking branch, or tag"
            ) from error
        return resolved_ref, commit

    def tags(self) -> list[str]:
        """List local tag names."""
        output = self.run(["tag", "--list"])
        return output.splitlines() if output else []

    def editor_command(self) -> str:
        """Return the editor selected by Git's standard precedence rules."""
        editor = self.run(["var", "GIT_EDITOR"])
        if not editor:
            raise GitError("Git did not resolve an editor; configure core.editor")
        return editor

    def local_tag_commit(self, tag: str) -> str | None:
        """Resolve a local tag to its commit, or return None when absent."""
        try:
            return self.run(["rev-list", "-n", "1", tag])
        except GitError:
            return None

    def remote_tag_commit(self, remote: str, tag: str) -> str | None:
        """Resolve a remote tag to its commit, including annotated tags."""
        output = self.run(["ls-remote", "--tags", remote, f"refs/tags/{tag}^{{}}"])
        if not output:
            output = self.run(["ls-remote", "--tags", remote, f"refs/tags/{tag}"])
        return output.split()[0] if output else None

    def create_tag(self, remote: str, tag: str, commit: str, description: str) -> None:
        """Create an annotated tag and push it with an explicit refspec."""
        self.run(["tag", "-a", tag, commit, "-m", description], mutate=True)
        self.run(["push", remote, f"refs/tags/{tag}:refs/tags/{tag}"], mutate=True)


def discover_repository_root() -> Path:
    """Return the current Git repository root with an actionable failure."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            text=True,
            capture_output=True,
        )
    except FileNotFoundError as error:
        raise GitError("Required command 'git' is not installed") from error
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() or "current directory is not a Git repository"
        raise GitError(f"Could not locate repository root: {detail}") from error
    return Path(result.stdout.strip())
