#!/usr/bin/env python

# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Regenerate Python code from .proto files (own protos + third-party submodule).

Usage:
    uv run scripts/gen_proto.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from grpc_tools import protoc

ROOT = Path(__file__).parent.parent.resolve()
PROTO_DIRS = [ROOT.parents[1] / "protobuf" / "external" / "xrootd-ssi-protobuf-interface" / "eos_cta" / "protobuf"]
OUT_DIR = ROOT / "src" / "cta" / "sdk" / "client" / "grpc" / "protobuf_generated"


def ensure_submodule() -> None:
    """Require the protocol sources to be prepared before building."""
    if not any(PROTO_DIRS[0].glob("*.proto")):
        raise RuntimeError(
            "Missing CTA protocol sources. Run git submodule update --init "
            "lib/protobuf/external/xrootd-ssi-protobuf-interface from the repository root."
        )


def generate() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    init_file = OUT_DIR / "__init__.py"
    if not init_file.exists():
        init_file.touch()

    proto_files = [str(p) for d in PROTO_DIRS if d.exists() for p in d.rglob("*.proto")]

    if not proto_files:
        print(f"No .proto files found under {[str(d) for d in PROTO_DIRS]}", file=sys.stderr)
        sys.exit(1)

    args = (
        ["protoc"]
        + [f"-I{d}" for d in PROTO_DIRS if d.exists()]
        + [
            f"--python_out={OUT_DIR}",
            f"--pyi_out={OUT_DIR}",
            f"--grpc_python_out={OUT_DIR}",
        ]
        + proto_files
    )

    print(f"Generating {len(proto_files)} proto file(s) into {OUT_DIR}")
    ret = protoc.main(args)
    if ret != 0:
        print("protoc failed", file=sys.stderr)
        sys.exit(ret)

    fix_imports()
    print("Done.")


def fix_imports() -> None:
    """Rewrite top-level absolute imports to package-relative ones.

    `protoc` emits cross-file imports as `import cta_common_pb2`, which only
    resolve when OUT_DIR is on sys.path. Rewriting them to relative imports
    is a dirty workaround, but it's easy and works.

    Unfortunately, the people responsible for protocol buffers have decided
    that this is not a problem worth fixing:

    https://github.com/protocolbuffers/protobuf/issues/1491
    """
    modules = sorted(p.stem for p in OUT_DIR.glob("*_pb2.py"))
    if not modules:
        return

    alt = "|".join(map(re.escape, modules))
    pattern = re.compile(rf"^import ({alt})\b", re.MULTILINE)
    targets = list(OUT_DIR.glob("*_pb2.py")) + list(OUT_DIR.glob("*_pb2_grpc.py")) + list(OUT_DIR.glob("*_pb2.pyi"))
    for f in targets:
        text = f.read_text()
        new = pattern.sub(r"from . import \1", text)
        if new != text:
            f.write_text(new)


if __name__ == "__main__":
    ensure_submodule()
    generate()
