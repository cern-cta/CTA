#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Retrieve files from tape using persistent XRootD Python client connections.

Runs inside the client pod. Discovers tape-only files, then issues
retrieve (prepare/stage-in) requests in parallel via multiprocessing.

Output: prints progress lines, then a summary on the last line.
"""

import argparse
import multiprocessing as mp
import os
import subprocess
import time


def parse_args():
    p = argparse.ArgumentParser(description="Retrieve files from tape via XRootD prepare")
    p.add_argument("--eos-host", required=True, help="EOS MGM hostname or IP")
    p.add_argument("--dest-dir", required=True, help="EOS directory containing archived files")
    p.add_argument("--num-dirs", type=int, required=True, help="Number of subdirectories")
    p.add_argument("--num-procs", type=int, default=8, help="Number of parallel worker processes")
    p.add_argument("--krb5-cache", default="/tmp/poweruser1/krb5cc_0", help="Kerberos credential cache for retrieve")
    p.add_argument("--activity", default="T0Reprocess", help="Activity string appended to prepare targets")
    return p.parse_args()


def list_tape_only_files(eos_host, dest_dir, num_dirs):
    """Discover all tape-only files (d0::t1) across subdirectories."""
    tape_files = []
    for i in range(num_dirs):
        dir_path = f"{dest_dir}/{i}"
        p = subprocess.run(
            ["eos", f"root://{eos_host}", "ls", "-ly", dir_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        if p.returncode != 0 or not p.stdout:
            continue

        for line in p.stdout.splitlines():
            if line.startswith("d0::t1"):
                fname = line.rsplit(None, 1)[-1]
                tape_files.append(f"{dir_path}/{fname}")

    return tape_files


def retrieve_worker(work_q, wid, eos_host, krb5_cache):
    """Worker process that issues prepare (stage-in) requests via XRootD."""
    # Switch from SSS to Kerberos (poweruser1) for prepare permissions
    for k in ("XrdSecsssKT", "XRDSSSKT"):
        os.environ.pop(k, None)
    os.environ["XrdSecPROTOCOL"] = "krb5"
    os.environ["KRB5CCNAME"] = krb5_cache

    from XRootD import client  # type: ignore
    from XRootD.client.flags import PrepareFlags  # type: ignore

    fs = client.FileSystem(f"root://{eos_host}")
    err_budget = 10

    while True:
        target = work_q.get()
        if target is None:
            work_q.task_done()
            return

        status, _ = fs.prepare([target], PrepareFlags.STAGE)

        if not status.ok and err_budget > 0:
            print(f"[retrieve worker {wid}] prepare failed: {target}, error: {status.message}", flush=True)
            err_budget -= 1

        work_q.task_done()


def main():
    args = parse_args()

    print(
        f"xrootd_retrieve: {args.num_dirs} dirs, {args.num_procs} procs",
        flush=True,
    )
    print(f"  source: {args.dest_dir}", flush=True)
    print(f"  activity: {args.activity}", flush=True)

    # Discover tape-only files
    print("Discovering tape-only files...", flush=True)
    tape_files = list_tape_only_files(args.eos_host, args.dest_dir, args.num_dirs)
    num_targets = len(tape_files)
    print(f"Found {num_targets} tape-only files to retrieve", flush=True)

    if num_targets == 0:
        print("No files to retrieve, exiting.", flush=True)
        return

    # Append activity to each target
    targets = [f"{path}?activity={args.activity}" for path in tape_files]

    # Launch workers
    work_q = mp.JoinableQueue()
    procs = []
    for wid in range(args.num_procs):
        p = mp.Process(
            target=retrieve_worker,
            args=(work_q, wid, args.eos_host, args.krb5_cache),
            daemon=True,
        )
        p.start()
        procs.append(p)

    t0 = time.time()

    # Enqueue all targets
    for target in targets:
        work_q.put(target)

    # Send stop sentinels
    for _ in range(args.num_procs):
        work_q.put(None)

    # Wait for completion
    work_q.join()
    elapsed = time.time() - t0
    rate = num_targets / max(elapsed, 0.001)

    print(f"Retrieve done: {num_targets} files in {elapsed:.1f}s ({rate:.1f} files/s)", flush=True)


if __name__ == "__main__":
    main()
