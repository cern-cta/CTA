# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Exercise release ordering and real mike publication against a local Git remote."""

# Use unittest for the temporary-repository integration tests.
# ruff: noqa: PT009, PT027

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from typing import Any

from typing_extensions import override

from publish import publication_decision, release_key


class VersionTests(unittest.TestCase):
    def test_canonical_versions(self):
        self.assertEqual(release_key("v6.2.4-1"), (6, 2, 4, 0, 1))
        self.assertEqual(release_key("v6.12.0.0-1"), (6, 12, 0, 0, 1))
        self.assertGreater(release_key("v6.12.0.0-1"), release_key("v6.9.99.0-1"))
        self.assertGreater(release_key("v6.12.0.0-10"), release_key("v6.12.0.0-2"))

    def test_noncanonical_tags(self):
        for tag in ["6.2.4-1", "v6.2", "v6.2.4-1.rc1", "v6.2.4-1.pgsched", "v6.2.4-1.pgcat", "v6.2.4-1.pgall"]:
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                release_key(tag)

    def test_missing_provenance_and_retagging_fail_closed(self):
        with self.assertRaises(KeyError):
            publication_decision("v6.2.4-1", "new", [{"version": "6.2"}])
        versions = [{"version": "6.2", "properties": {"source_tag": "v6.2.4-1", "source_commit": "old"}}]
        with self.assertRaises(ValueError):
            publication_decision("v6.2.4-1", "new", versions)


class PublicationTests(unittest.TestCase):
    @override
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "checkout"
        self.remote = self.root / "remote.git"
        self.publisher = Path(__file__).with_name("publish.py").resolve()
        self.env = dict(os.environ, PATH=f"{Path(sys.executable).parent}:{os.environ['PATH']}")
        self.env.pop("GITLAB_CI", None)
        self.command("git", "init", "--bare", str(self.remote), cwd=self.root)
        self.command("git", "init", "--initial-branch=main", str(self.repo), cwd=self.root)
        self.command("git", "config", "user.name", "Docs test")
        self.command("git", "config", "user.email", "docs-test@example.invalid")
        self.command("git", "remote", "add", "origin", str(self.remote))
        (self.repo / "docs/content").mkdir(parents=True)
        (self.repo / "docs/mkdocs.yml").write_text(
            "site_name: Test\nsite_url: https://example.invalid/\n"
            "docs_dir: content\nsite_dir: ../build/docs/site\nstrict: true\n"
        )
        (self.repo / "docs/content/index.md").write_text("# Test documentation\n")
        self.command("git", "add", "docs")
        self.command("git", "commit", "--message", "Source")
        self.command("git", "checkout", "--orphan", "gl-pages")
        self.command("git", "rm", "-r", "--force", "docs")
        for version in ["v4", "v5"]:
            directory = self.repo / "public" / version
            directory.mkdir(parents=True)
            (directory / "index.html").write_text(f"Historical {version}")
        (self.repo / "public/latest").symlink_to("v5", target_is_directory=True)
        (self.repo / "public/versions.json").write_text(
            json.dumps(
                [
                    {"version": "v5", "title": "v5", "aliases": ["latest"]},
                    {"version": "v4", "title": "v4", "aliases": []},
                ]
            )
        )
        self.command("git", "add", "public")
        self.command("git", "commit", "--message", "Historical documentation")
        self.command("git", "push", "origin", "gl-pages")
        self.command("git", "checkout", "main")

    def command(self, *args: str, cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(  # noqa: S603 - Fixed test commands in temporary repositories.
            args,
            cwd=cwd or self.repo,
            env=self.env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if check and result.returncode:
            self.fail(f"Command failed: {args}\n{result.stdout}")
        return result

    def deploy(self, tag: str) -> tuple[Path, list[dict[str, Any]]]:
        self.command("git", "tag", "--force", tag)
        self.command(sys.executable, str(self.publisher), "--tag", tag)
        output = self.repo / "build/docs/public"
        return output, json.loads((output / "versions.json").read_text())

    def test_versions_aliases_retries_and_preservation(self):
        for tag in ["v6.9.1-1", "v6.12.0.0-1", "v6.9.2-1", "v6.12.1.0-2"]:
            self.deploy(tag)
        output, versions = self.deploy("v6.12.1.0-2")
        latest = next(version for version in versions if "latest" in version["aliases"])
        self.assertEqual(latest["version"], "6.12")
        self.assertEqual(latest["properties"]["source_tag"], "v6.12.1.0-2")
        self.assertEqual((output / "v4/index.html").read_text(), "Historical v4")
        self.assertEqual((output / "v5/index.html").read_text(), "Historical v5")
        self.assertIn("latest/", (output / "index.html").read_text())
        self.assertIn("6.12/", (output / "latest/index.html").read_text())
        self.assertFalse((output / "latest").is_symlink())
        previous = self.command("git", "rev-parse", "gl-pages").stdout
        self.deploy("v6.12.0.0-1")
        self.assertEqual(previous, self.command("git", "rev-parse", "gl-pages").stdout)

    def test_refreshes_stale_pages_branch(self):
        self.deploy("v6.9.1-1")
        old = self.command("git", "rev-parse", "gl-pages").stdout.strip()
        self.deploy("v6.12.0.0-1")
        # Simulate a second serialized publisher whose local branch is stale.
        self.command("git", "update-ref", "refs/heads/gl-pages", old)
        _, versions = self.deploy("v6.9.2-1")
        self.assertIn("6.12", [entry["version"] for entry in versions])
        self.assertEqual(next(entry["version"] for entry in versions if "latest" in entry["aliases"]), "6.12")

    def test_missing_snippet_prevents_publication(self):
        config = self.repo / "docs/mkdocs.yml"
        with config.open("a") as stream:
            stream.write("markdown_extensions:\n  - pymdownx.snippets:\n      check_paths: true\n")
        (self.repo / "docs/content/index.md").write_text('--8<-- "missing-file.md"\n')
        self.command("git", "add", "docs")
        self.command("git", "commit", "--message", "Missing snippet")
        self.command("git", "tag", "v6.12.0.0-1")
        previous = self.command("git", "ls-remote", "origin", "gl-pages").stdout
        result = self.command(sys.executable, str(self.publisher), "--tag", "v6.12.0.0-1", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(previous, self.command("git", "ls-remote", "origin", "gl-pages").stdout)


if __name__ == "__main__":
    unittest.main()
