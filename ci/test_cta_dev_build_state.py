# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Exercise build-state decisions without starting containers or compiling CTA."""

# ruff: noqa: PT009

import json
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("cta-dev.sh")
HARNESS = r"""
fixture_root="$2"
# Point repository discovery at the fixture while sourcing the real script and helpers.
git() {
  case "$*" in
    *--show-toplevel*) printf '%s\n' "$fixture_root" ;;
    *describe*) printf 'v1\n' ;;
    *) command git "$@" ;;
  esac
}
source "$1"
run_fixture() {
  platform="$3"
  clean_build_dirs="${4:-false}"
  local build_exit_code="${5:-0}"
  enable_internal_repos=false
  internal_repos_forced_public=true

  # Keep the real build decisions and state writes; replace external build operations.
  nproc() { printf '4\n'; }
  container_exists() { return 0; }
  container_status() { printf 'running\n'; }
  podman() {
    [[ "$1" == exec ]] || return 1
    printf '%s\n' "$@" > "$project_root/arguments"
    if [[ " $* " == *" --clean-build-dir "* ]]; then
      rm -rf "$project_root/build/$platform"
    fi
    mkdir -p "$project_root/build/$platform"
    printf 'CTA_BUILD_MODE:STRING=binary-package\n' > "$project_root/build/$platform/CMakeCache.txt"
    return "$build_exit_code"
  }
  build_cta
}
run_fixture "$@"
"""


class BuildStateTests(unittest.TestCase):
    def run_build(self, root, platform, clean=False, build_exit_code=0):
        (root / "project.json").write_text((SCRIPT.parent.parent / "project.json").read_text())
        result = subprocess.run(  # noqa: S603 - fixed harness and local test fixtures
            [
                "/bin/bash",
                "-c",
                HARNESS,
                "build-state-test",
                str(SCRIPT),
                str(root),
                platform,
                str(clean).lower(),
                str(build_exit_code),
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, build_exit_code, result.stdout + result.stderr)
        return result

    def state_file(self, root, platform):
        return root / "build/.cta-dev-state" / f"{platform}.json"

    def test_switching_platforms_preserves_incremental_builds(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.run_build(root, "el9")
            el9_state = self.state_file(root, "el9").read_bytes()
            for platform in ("el10", "debian13"):
                result = self.run_build(root, platform)
                self.assertNotIn("Automatically cleaning", result.stdout + result.stderr)
                self.assertNotIn("changed:", result.stdout + result.stderr)
                self.assertEqual(self.state_file(root, "el9").read_bytes(), el9_state)
            self.run_build(root, "el9")
            arguments = (root / "arguments").read_text().splitlines()
            self.assertIn("--skip-cmake", arguments)
            self.assertNotIn("--clean-build-dir", arguments)

    def test_legacy_state_is_imported_only_for_its_platform(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.run_build(root, "el9")
            legacy = root / ".cta-dev-build-state.json"
            self.state_file(root, "el9").rename(legacy)
            self.run_build(root, "el10")
            self.assertTrue(legacy.exists())
            self.assertFalse(self.state_file(root, "el9").exists())
            self.run_build(root, "el9")
            self.assertFalse(legacy.exists())
            self.assertTrue(self.state_file(root, "el9").exists())
            self.assertIn("--skip-cmake", (root / "arguments").read_text().splitlines())

    def test_new_state_takes_precedence_over_legacy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.run_build(root, "el9")
            legacy_state = json.loads(self.state_file(root, "el9").read_text())
            legacy_state["configuration"]["schedulerType"] = "pgsched"
            legacy = root / ".cta-dev-build-state.json"
            legacy.write_text(json.dumps(legacy_state))
            self.run_build(root, "el9")
            self.assertTrue(legacy.exists())
            arguments = (root / "arguments").read_text().splitlines()
            self.assertIn("--skip-cmake", arguments)
            self.assertNotIn("--clean-build-dir", arguments)

    def test_platform_cleanup_preserves_state_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.run_build(root, "el9")
            self.run_build(root, "el10")
            el9_state = self.state_file(root, "el9").read_bytes()
            self.run_build(root, "el10", clean=True)
            self.assertIn("--clean-build-dir", (root / "arguments").read_text().splitlines())
            self.assertEqual(self.state_file(root, "el9").read_bytes(), el9_state)
            self.assertTrue(json.loads(self.state_file(root, "el10").read_text())["buildSuccessful"])

    def test_failed_clean_build_retains_unsuccessful_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.run_build(root, "el10")
            self.run_build(root, "el10", clean=True, build_exit_code=1)
            self.assertFalse(json.loads(self.state_file(root, "el10").read_text())["buildSuccessful"])
            self.run_build(root, "el10")
            self.assertNotIn("--skip-cmake", (root / "arguments").read_text().splitlines())

    def test_mismatched_platform_state_cannot_be_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.run_build(root, "el9")
            self.run_build(root, "el10")
            self.state_file(root, "el10").write_bytes(self.state_file(root, "el9").read_bytes())
            self.run_build(root, "el10")
            self.assertIn("--clean-build-dir", (root / "arguments").read_text().splitlines())


if __name__ == "__main__":
    unittest.main()
