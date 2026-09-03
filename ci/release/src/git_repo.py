# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Repository-scoped Git operations for the CTA release CLI."""

from __future__ import annotations

import subprocess
from pathlib import Path
from collections.abc import Sequence

from confirmation import confirm


class GitError(RuntimeError):
    """A failure to validate or operate on the local Git repository."""


class Git:
    """Run safe, repository-scoped Git operations for release workflows."""

    def __init__(self, root: Path, dry_run: bool = False):
        """Create a Git runner rooted at a repository."""
        self.root = root
        self.dry_run = dry_run

    def confirm_checkout_warnings(self, warnings: Sequence[str]) -> None:
        """Show checkout warnings and require confirmation before continuing."""
        confirm(
            "Continue with this checkout?",
            "Release declined; no changes were made",
            warnings=warnings,
            dry_run=self.dry_run,
        )

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

    def validate_target_branch(self, branch: str) -> None:
        """Require a valid Git branch name before using it as an argument or ref."""
        try:
            self.run(["check-ref-format", "--branch", branch])
        except GitError as error:
            raise GitError(f"Invalid target branch {branch!r}") from error

    def validate_repository(self, branch: str, remote: str, fetch: bool = True) -> str:
        """Validate a checkout and fast-forward its local release branch."""
        self.validate_target_branch(branch)
        if self.run(["rev-parse", "--show-toplevel"]) != str(self.root.resolve()):
            raise GitError(f"Run release from repository root {self.root}")
        current = self.run(["branch", "--show-current"])
        checkout_warnings = []
        if self.run(["status", "--porcelain"]):
            checkout_warnings.append("The working tree is not clean")
        if current != branch:
            checkout_warnings.append(
                f"The current branch is {current!r}, not {branch!r}; "
                f"the release commit will still be selected from {branch}"
            )
        if checkout_warnings:
            self.confirm_checkout_warnings(checkout_warnings)
        if fetch:
            self.run(["fetch", "--force", "--tags", remote, branch], mutate=True)
        local = self.run(["rev-parse", branch])
        remote_sha = self.run(["rev-parse", f"{remote}/{branch}"])
        if local != remote_sha:
            try:
                self.run(["merge-base", "--is-ancestor", branch, f"{remote}/{branch}"])
            except GitError as error:
                raise GitError(
                    f"Cannot fast-forward {branch} to {remote}/{branch}; the local branch contains other commits"
                ) from error
            if current == branch:
                self.run(["merge", "--ff-only", f"{remote}/{branch}"], mutate=True)
            else:
                self.run(["branch", "--force", branch, f"{remote}/{branch}"], mutate=True)
        return remote_sha

    def resolve_remote_branch(self, remote: str, branch: str, fetch: bool = True) -> str:
        """Refresh and resolve one remote-tracking branch."""
        self.validate_target_branch(branch)
        if self.run(["rev-parse", "--show-toplevel"]) != str(self.root.resolve()):
            raise GitError(f"Run release from repository root {self.root}")

        if self.run(["status", "--porcelain"]):
            self.confirm_checkout_warnings(
                [f"The working tree is not clean; the release commit will be read from {remote}/{branch}"]
            )

        if fetch:
            self.run(["fetch", "--force", "--tags", remote, branch], mutate=True)

        try:
            return self.run(["rev-parse", "--verify", f"{remote}/{branch}^{{commit}}"])
        except GitError as error:
            raise GitError(f"Remote branch {remote}/{branch} does not resolve to a commit") from error

    def is_ancestor(self, commit: str, descendant: str) -> bool:
        """Return whether one commit is reachable from another."""
        try:
            self.run(["merge-base", "--is-ancestor", commit, descendant])
        except GitError:
            return False
        return True

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

    def remote_tag_commits(self, remote: str, tag_names: Sequence[str]) -> dict[str, str]:
        """Resolve selected lightweight or annotated remote tags with one request."""
        if not tag_names:
            return {}
        patterns = [pattern for name in tag_names for pattern in (f"refs/tags/{name}", f"refs/tags/{name}^{{}}")]
        output = self.run(["ls-remote", "--tags", remote, *patterns])
        commits: dict[str, str] = {}
        for line in output.splitlines():
            fields = line.split()
            if len(fields) != 2:
                continue
            commit, ref = fields
            name = ref.removeprefix("refs/tags/")
            peeled = name.endswith("^{}")
            name = name.removesuffix("^{}")
            if name in tag_names and (peeled or name not in commits):
                commits[name] = commit
        return commits

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
