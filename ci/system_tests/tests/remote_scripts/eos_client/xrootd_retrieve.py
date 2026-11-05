# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Retrieve files from tape using persistent XRootD Python client connections.

Runs inside the client pod. Discovers tape-only files in parallel, then issues
batched retrieve (prepare/stage-in) requests via multiprocessing.

Output: prints progress lines, then a summary on the last line.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import multiprocessing as mp
import os
import subprocess
import threading
import time
from multiprocessing.queues import JoinableQueue
from typing import Any


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Retrieve files from tape via XRootD prepare")
    p.add_argument("--eos-host", required=True, help="EOS MGM hostname or IP")
    p.add_argument("--dest-dir", required=True, help="EOS directory containing archived files")
    p.add_argument("--num-dirs", type=int, required=True, help="Number of subdirectories")
    p.add_argument("--num-procs", type=int, default=8, help="Number of parallel worker processes")
    p.add_argument("--krb5-cache", required=True, help="Kerberos credential cache for retrieve")
    p.add_argument("--activity", required=True, help="Activity string appended to prepare targets")
    p.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Files per prepare() call (reduces MGM round-trips at large queue depths)",
    )
    p.add_argument(
        "--discovery-procs",
        type=int,
        default=5,
        help="Parallel threads for tape-only file discovery across subdirectories",
    )
    return p.parse_args()


def _list_tape_only_files_in_dir(args: tuple[str, str, int]) -> list[str]:
    eos_host, dest_dir, i = args
    dir_path = f"{dest_dir}/{i}"
    p = subprocess.run(
        ["eos", f"root://{eos_host}", "ls", "-ly", dir_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    if p.returncode != 0 or not p.stdout:
        return []
    result = []
    for line in p.stdout.splitlines():
        if line.startswith("d0::t1"):
            fname = line.rsplit(None, 1)[-1]
            result.append(f"{dir_path}/{fname}")
    return result


def list_tape_only_files(eos_host: str, dest_dir: str, num_dirs: int, discovery_procs: int) -> list[str]:
    """Discover all tape-only files (d0::t1) across subdirectories in parallel."""
    work = [(eos_host, dest_dir, i) for i in range(num_dirs)]
    tape_files: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(discovery_procs, num_dirs)) as pool:
        for files in pool.map(_list_tape_only_files_in_dir, work):
            tape_files.extend(files)
    return tape_files


def retrieve_worker(work_q: JoinableQueue[Any], wid: int, eos_host: str, krb5_cache: str) -> None:
    """Worker process: issues batched prepare (stage-in) requests via a persistent XRootD connection."""
    for k in ("XrdSecsssKT", "XRDSSSKT"):
        os.environ.pop(k, None)
    os.environ["XrdSecPROTOCOL"] = "krb5"  # noqa: SIM112 - XRootD requires this exact mixed-case name
    os.environ["KRB5CCNAME"] = krb5_cache

    from XRootD import client  # type: ignore[reportMissingImports]
    from XRootD.client.flags import PrepareFlags  # type: ignore[reportMissingImports]

    fs = client.FileSystem(f"root://{eos_host}")
    err_budget = 10

    while True:
        batch = work_q.get()
        if batch is None:
            work_q.task_done()
            return

        status, _ = fs.prepare(batch, PrepareFlags.STAGE)

        if not status.ok and err_budget > 0:
            print(f"[retrieve worker {wid}] prepare failed for batch of {len(batch)}: {status.message}", flush=True)
            err_budget -= 1

        work_q.task_done()


def main() -> None:
    args = parse_args()

    print(
        f"xrootd_retrieve: {args.num_dirs} dirs, {args.num_procs} procs, batch size {args.batch_size}",
        flush=True,
    )
    print(f"  source: {args.dest_dir}", flush=True)
    print(f"  activity: {args.activity}", flush=True)

    print("Discovering tape-only files...", flush=True)
    t_discover = time.time()
    tape_files = list_tape_only_files(args.eos_host, args.dest_dir, args.num_dirs, args.discovery_procs)
    num_targets = len(tape_files)
    print(
        f"Found {num_targets} tape-only files in {time.time() - t_discover:.1f}s",
        flush=True,
    )

    if num_targets == 0:
        print("No files to retrieve, exiting.", flush=True)
        return

    targets = [f"{path}?activity={args.activity}" for path in tape_files]

    work_q: JoinableQueue[Any] = mp.JoinableQueue()
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

    for i in range(0, num_targets, args.batch_size):
        work_q.put(targets[i : i + args.batch_size])

    for _ in range(args.num_procs):
        work_q.put(None)

    # Print every 120 s so the Kubernetes exec websocket receives data and is
    # not cut by the API-server / LB TCP idle timeout (~2 h).
    _stop_heartbeat = threading.Event()

    def _heartbeat() -> None:
        while not _stop_heartbeat.wait(120):
            print(f"xrootd_retrieve: running... {time.time() - t0:.0f}s elapsed", flush=True)

    hb_thread = threading.Thread(target=_heartbeat, daemon=True)
    hb_thread.start()

    work_q.join()
    _stop_heartbeat.set()
    hb_thread.join()

    elapsed = time.time() - t0
    rate = num_targets / max(elapsed, 0.001)
    print(f"Retrieve done: {num_targets} files in {elapsed:.1f}s ({rate:.1f} files/s)", flush=True)


if __name__ == "__main__":
    main()
