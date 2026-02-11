#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Archive files to EOS using persistent XRootD Python client connections.

Runs inside the client pod. Uses multiprocessing with persistent XRootD
File objects for high throughput on many small files.

Output: prints progress lines, then a JSON summary on the last line.
"""

import argparse
import json
import multiprocessing as mp
import os
import subprocess
import time


def parse_args():
    p = argparse.ArgumentParser(description="Archive files via persistent XRootD client")
    p.add_argument("--eos-host", required=True, help="EOS MGM hostname (e.g. ctaeos)")
    p.add_argument("--dest-dir", required=True, help="EOS destination directory (e.g. /eos/ctaeos/cta/stress)")
    p.add_argument("--num-files", type=int, required=True, help="Total number of files to create")
    p.add_argument("--num-dirs", type=int, default=10, help="Number of subdirectories")
    p.add_argument("--num-procs", type=int, default=8, help="Number of parallel worker processes")
    p.add_argument("--file-size", type=int, default=512, help="File size in bytes")
    p.add_argument("--sss-keytab", default="/etc/eos.keytab", help="SSS keytab path")
    return p.parse_args()


def mkdir_dirs(eos_host, dest_dir, num_dirs):
    """Create subdirectories under dest_dir."""
    for i in range(num_dirs):
        path = f"{dest_dir}/{i}"
        subprocess.run(
            ["eos", f"root://{eos_host}", "mkdir", "-p", path],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True,
        )


def worker(work_q: mp.JoinableQueue, wid: int, eos_host: str, dest_dir: str,
           num_dirs: int, file_size: int):
    # Import here so XrdSecsssKT is already set in the environment
    from XRootD import client
    from XRootD.client.flags import OpenFlags

    _ = client.FileSystem(f"root://{eos_host}")
    err_budget = 3

    while True:
        file_num = work_q.get()
        if file_num is None:
            work_q.task_done()
            return

        subdir = file_num % num_dirs
        payload = os.urandom(file_size)

        dest_url = f"root://{eos_host}/{dest_dir}/{subdir}/test{file_num:07d}"

        f = client.File()
        st, _ = f.open(dest_url, OpenFlags.NEW | OpenFlags.WRITE, 0o644)
        if st.ok:
            f.write(payload, 0)
            f.close()
        else:
            if err_budget > 0:
                print(f"[worker {wid}] open failed: {dest_url} :: {st}", flush=True)
                err_budget -= 1

        work_q.task_done()


def main():
    args = parse_args()

    # Set SSS keytab before any XRootD client usage
    os.environ["XrdSecsssKT"] = args.sss_keytab
    os.environ.setdefault("XRD_LOGLEVEL", "Error")

    print(f"xrootd_archive: {args.num_files} files, {args.num_dirs} dirs, "
          f"{args.num_procs} procs, {args.file_size}B each", flush=True)
    print(f"  dest: {args.dest_dir}", flush=True)

    # Create subdirectories
    mkdir_dirs(args.eos_host, args.dest_dir, args.num_dirs)

    # Launch workers
    work_q = mp.JoinableQueue()
    procs = []
    for wid in range(args.num_procs):
        p = mp.Process(
            target=worker,
            args=(work_q, wid, args.eos_host, args.dest_dir, args.num_dirs, args.file_size),
            daemon=True,
        )
        p.start()
        procs.append(p)

    t0 = time.time()

    # Enqueue all work
    for i in range(args.num_files):
        work_q.put(i)

    # Send stop sentinels
    for _ in range(args.num_procs):
        work_q.put(None)

    # Wait for completion
    work_q.join()
    elapsed = time.time() - t0
    rate = args.num_files / max(elapsed, 0.001)

    print(f"Archive done: {args.num_files} files in {elapsed:.1f}s ({rate:.1f} files/s)", flush=True)

    # JSON summary on last line for programmatic parsing
    print(json.dumps({
        "files_created": args.num_files,
        "elapsed_secs": round(elapsed, 2),
        "files_per_sec": round(rate, 1),
    }), flush=True)


if __name__ == "__main__":
    main()
