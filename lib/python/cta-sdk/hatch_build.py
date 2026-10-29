from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


def _has_git_repo(root: Path) -> bool:
    result = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


class CustomBuildHook(BuildHookInterface):
    """Generate protobuf sources during hatch builds."""

    def initialize(self, version: str, build_data: dict) -> None:
        root = Path(self.root)
        gen_script = root / "scripts" / "gen_proto.py"

        module_spec = importlib.util.spec_from_file_location("ctaops_cli_gen_proto", gen_script)
        if module_spec is None or module_spec.loader is None:
            raise RuntimeError(f"Unable to load module spec from: {gen_script}")

        gen_proto = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(gen_proto)
        out_dir = Path(
            getattr(gen_proto, "OUT_DIR", root / "src" / "cta" / "sdk" / "client" / "grpc" / "protobuf_generated")
        )

        if _has_git_repo(root):
            gen_proto.ensure_submodule()
            gen_proto.generate()
        elif not (out_dir.exists() and any(out_dir.glob("*_pb2.py"))):
            # if we are not in a Git repository, we expect the generated protobuf sources to already exist
            raise RuntimeError(
                "This build is not running from a Git checkout and generated protobuf sources are missing. "
                "Run scripts/gen_proto.py from a repository checkout before building from a source snapshot."
            )
