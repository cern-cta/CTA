#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Count files in EOS directories using parallel eos file info queries.

Runs inside the client pod. Uses multiprocessing for parallel queries.
Output: prints a single integer (total file count) to stdout.
"""

import argparse
import multiprocessing as mp
import subprocess

def parse_args():
    p = argparse.ArgumentParser(description="Count files in EOS directories")
    p.add_argument("--eos-host", required=True, help="EOS MGM hostname (e.g. ctaeos)")
    p.add_argument("--dest-dir", required=True, help="EOS directory (e.g. /eos/ctaeos/cta/stress)")
    p.add_argument("--num-dirs", type=int, required=True, help="Number of subdirectories")
    p.add_argument("--num-procs", type=int, default=5, help="Number of parallel worker processes")
    return p.parse_args()


def count_dir_files(args) -> int:
    """Count files in a single directory via eos find -f command."""
    eos_host, path = args
    try:
        p = subprocess.run(
            ["eos", f"root://{eos_host}", "find", "-f", path],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=10,
        )
    except subprocess.TimeoutExpired:
        return 0

    if p.returncode != 0 or not p.stdout:
        return 0
    
    print(p.stdout)

    return int(len(p.stdout.strip().splitlines()))


def main():
    args = parse_args()

    paths = [(args.eos_host, f"{args.dest_dir}/{i}") for i in range(args.num_dirs)]

    with mp.Pool(processes=args.num_procs) as pool:
        counts = pool.map(count_dir_files, paths)

    print(sum(counts))


if __name__ == "__main__":
    main()
