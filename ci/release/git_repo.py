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

    def __init__(self, root: Path, dry_run: bool = False):
        """Create a Git runner rooted at a repository."""
        self.root = root
        self.dry_run = dry_run

    def confirm_checkout_warnings(self, warnings: Sequence[str]) -> None:
        """Show checkout warnings and require confirmation before continuing."""
        for warning in warnings:
            print(f"WARNING: {warning}")
        if self.dry_run:
            print("DRY-RUN: would ask for confirmation before continuing")
            return
        try:
            answer = input("Continue with this checkout? [y/N] ").strip().lower()
        except EOFError as error:
            raise GitError("Checkout confirmation requires an interactive terminal") from error
        if answer not in ("y", "yes"):
            raise GitError("Release declined; no changes were made")

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
        """Validate a checkout and fast-forward its local release branch."""
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

        if self.run(["status", "--porcelain"]):
            self.confirm_checkout_warnings(
                ["The working tree is not clean; the requested tag target will still be resolved explicitly"]
            )

        if fetch:
            self.run(["fetch", "--force", "--tags", remote, default_branch], mutate=True)

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

    def remote_tag_names(self, remote: str) -> set[str]:
        """List every remote tag name with one request."""
        output = self.run(["ls-remote", "--tags", "--refs", remote])
        if not output:
            return set()
        prefix = "refs/tags/"
        return {
            ref.removeprefix(prefix)
            for line in output.splitlines()
            if len(fields := line.split()) == 2 and (ref := fields[1]).startswith(prefix)
        }

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
