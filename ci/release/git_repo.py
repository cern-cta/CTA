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

    def run(self, arguments: Sequence[str], mutate: bool = False) -> str:
        """Run Git with an argument list, printing mutations during dry runs."""
        command = ["git", *arguments]
        if mutate and self.dry_run:
            print("DRY-RUN:", " ".join(command))
            return ""
        try:
            completed_process = subprocess.run(
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
        return completed_process.stdout.strip()

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
        target_ref: str,
        fetch: bool = True,
    ) -> str:
        """Validate the worktree and resolve an explicit tag target to a commit."""
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

        try:
            return self.run(["rev-parse", "--verify", f"{target_ref}^{{commit}}"])
        except GitError as error:
            raise GitError(
                f"Tag target {target_ref!r} does not resolve to a commit; "
                "use a commit SHA, local branch, remote-tracking branch, or tag"
            ) from error

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

    def local_tag_commit(self, tag_name: str) -> str | None:
        """Resolve a local tag to its commit, or return None when absent."""
        try:
            return self.run(["rev-list", "-n", "1", tag_name])
        except GitError:
            return None

    def remote_tag_commit(self, remote: str, tag_name: str) -> str | None:
        """Resolve a remote tag to its commit, including annotated tags."""
        output = self.run(["ls-remote", "--tags", remote, f"refs/tags/{tag_name}^{{}}"])
        if not output:
            output = self.run(["ls-remote", "--tags", remote, f"refs/tags/{tag_name}"])
        return output.split()[0] if output else None

    def create_tag(self, target_commit: str, tag_name: str, description: str) -> None:
        """Create one annotated local tag using the multi-tag implementation."""
        self.create_tags(target_commit, {tag_name: description})

    def create_tags(self, target_commit: str, tag_descriptions: dict[str, str]) -> None:
        """Create multiple annotated local tags at one commit."""
        for tag_name, description in tag_descriptions.items():
            self.run(["tag", "-a", tag_name, target_commit, "-m", description], mutate=True)

    def push_tags(self, remote: str, tag_names: Sequence[str]) -> None:
        """Push explicit tag refspecs atomically to prevent partial publication."""
        if not tag_names:
            return

        refspecs = [f"refs/tags/{tag_name}:refs/tags/{tag_name}" for tag_name in tag_names]
        self.run(["push", "--atomic", remote, *refspecs], mutate=True)


def discover_repository_root() -> Path:
    """Return the current Git repository root with an actionable failure."""
    try:
        completed_process = subprocess.run(
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
    return Path(completed_process.stdout.strip())
