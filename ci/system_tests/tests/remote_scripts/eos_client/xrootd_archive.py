# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Archive files to EOS using persistent XRootD Python client connections.

Runs inside the client pod. Uses multiprocessing with persistent XRootD
File objects for high throughput on many small files.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
from multiprocessing.queues import JoinableQueue
import os
import time
from typing import Any


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Archive files via persistent XRootD client")
    p.add_argument("--eos-host", required=True, help="EOS MGM hostname (e.g. ctaeos)")
    p.add_argument("--dest-dir", required=True, help="EOS destination directory (e.g. /eos/ctaeos/cta/stress)")
    p.add_argument("--num-files", type=int, required=True, help="Total number of files to create")
    p.add_argument("--num-dirs", type=int, default=10, help="Number of subdirectories")
    p.add_argument("--num-procs", type=int, default=8, help="Number of parallel worker processes")
    p.add_argument("--file-size", type=int, default=512, help="File size in bytes")
    p.add_argument("--batch-size", type=int, default=1000, help="Files per batch (to reduce queue contention)")
    p.add_argument(
        "--write-files-in-chunks",
        action="store_true",
        default=False,
        help="Write large files in 1MB chunks (useful for large files, several MBs)",
    )
    return p.parse_args()


HEADER_SIZE = 16  # 4 hex subdir + 8 hex file_num + 4 zeros
CHUNK_1MB = 1024 * 1024  # 1MB pre-allocated once per worker


def worker(
    work_q: JoinableQueue[Any],
    wid: int,
    eos_host: str,
    dest_dir: str,
    num_dirs: int,
    file_size: int,
    write_in_chunks: bool,
) -> None:
    # Import here so the Kerberos environment is set before XRootD is loaded.
    from XRootD import client  # type: ignore[reportMissingImports]
    from XRootD.client.flags import OpenFlags  # type: ignore[reportMissingImports]

    _ = client.FileSystem(f"root://{eos_host}")
    err_budget = 3

    # Pre-allocate zero chunk once per worker to avoid per-file allocation overhead
    zeros_chunk = b"\x00" * CHUNK_1MB

    while True:
        batch = work_q.get()
        if batch is None:
            work_q.task_done()
            return

        # Process all files in the batch (start, end) range
        start, end = batch
        for file_num in range(start, end):
            subdir = file_num % num_dirs
            dest_url = f"root://{eos_host}/{dest_dir}/{subdir}/test{file_num:07d}"

            f = client.File()
            st, _ = f.open(dest_url, OpenFlags.DELETE | OpenFlags.WRITE, 0o644)
            if not st.ok:
                if err_budget > 0:
                    print(f"[worker {wid}] open failed: {dest_url} :: {st}", flush=True)
                    err_budget -= 1
                continue

            # Write compressible content: unique header + zero padding
            # Header format: 4 hex subdir + 8 hex file_num + 4 zeros (16 bytes total)
            header = f"{subdir:04x}{file_num:08x}0000".encode("ascii")

            if write_in_chunks:
                # Write header first, then zero padding in 1MB chunks (for large files)
                st, _ = f.write(header, 0)
                if not st.ok:
                    if err_budget > 0:
                        print(f"[worker {wid}] write header failed: {dest_url} :: {st}", flush=True)
                        err_budget -= 1
                    f.close()
                    continue

                offset = HEADER_SIZE
                remaining = file_size - HEADER_SIZE
                while remaining > 0:
                    n = min(remaining, CHUNK_1MB)
                    st, _ = f.write(zeros_chunk[:n], offset)
                    if not st.ok:
                        if err_budget > 0:
                            print(f"[worker {wid}] write zeros failed: {dest_url} :: {st}", flush=True)
                            err_budget -= 1
                        break
                    offset += n
                    remaining -= n
            else:
                # Write entire payload in one go (optimal for small files)
                payload = header + b"\x00" * (file_size - HEADER_SIZE)
                st, _ = f.write(payload, 0)
                if not st.ok:
                    if err_budget > 0:
                        print(f"[worker {wid}] write failed: {dest_url} :: {st}", flush=True)
                        err_budget -= 1
                    f.close()
                    continue

            f.close()

        work_q.task_done()


def main() -> None:
    args = parse_args()

    print(
        f"xrootd_archive: {args.num_files} files, {args.num_dirs} dirs, {args.num_procs} procs, {args.file_size}B each",
        flush=True,
    )
    print(f"  dest: {args.dest_dir}", flush=True)

    if args.file_size < HEADER_SIZE:
        raise ValueError(f"File size must be >= {HEADER_SIZE} bytes")

    # For now, use Kerberos authentication for copying/archival. Testing showed no
    # significant performance difference between Kerberos and SSS. To switch to
    # SSS in the future, configure the keytab and protocol before starting workers:
    # os.environ["XrdSecsssKT"] = "/etc/eos.keytab"
    # os.environ["XrdSecPROTOCOL"] = "sss"
    # os.environ.setdefault("XRD_LOGLEVEL", "Error")
    #
    # Match the established xrdcp archival path: write files as user1 via Kerberos.
    os.environ["KRB5CCNAME"] = "/tmp/user1/krb5cc_0"
    os.environ["XrdSecPROTOCOL"] = "krb5"  # noqa: SIM112 - XRootD requires this exact mixed-case name

    # Launch workers
    work_q = mp.JoinableQueue()
    procs = []
    for wid in range(args.num_procs):
        p = mp.Process(
            target=worker,
            args=(
                work_q,
                wid,
                args.eos_host,
                args.dest_dir,
                args.num_dirs,
                args.file_size,
                args.write_files_in_chunks,
            ),
            daemon=True,
        )
        p.start()
        procs.append(p)

    t0 = time.time()

    # Enqueue work in batches to reduce queue contention
    # Instead of 50M individual items, enqueue 50K batches of 1000
    batch_size = args.batch_size
    for start in range(0, args.num_files, batch_size):
        end = min(start + batch_size, args.num_files)
        work_q.put((start, end))

    # Send stop sentinels
    for _ in range(args.num_procs):
        work_q.put(None)

    # Wait for completion
    work_q.join()
    elapsed = time.time() - t0
    rate = args.num_files / max(elapsed, 0.001)

    print(f"Archive done: {args.num_files} files in {elapsed:.1f}s ({rate:.1f} files/s)", flush=True)


if __name__ == "__main__":
    main()
