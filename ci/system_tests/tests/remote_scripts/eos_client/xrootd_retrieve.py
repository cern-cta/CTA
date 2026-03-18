#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Retrieve files from tape using persistent XRootD Python client connections.

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

# TODO: checkout implementation from the branch 1483-write-stress-test-in-python

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


def prepare_worker(work_q: mp.JoinableQueue, wid: int, eos_host: str, dest_dir: str, num_dirs: int, file_size: int):
    # Worker process: switch from SSS to Kerberos (poweruser1) for prepare permissions
    for k in ("XrdSecsssKT", "XRDSSSKT"):
        os.environ.pop(k, None)
    os.environ["XrdSecPROTOCOL"] = "krb5"
    os.environ["KRB5CCNAME"] = KRB5CC_POWER

    from XRootD import client
    from XRootD.client.flags import PrepareFlags

    err_budget = 10

    fs_url = f"root://{EOS_MGM_HOST}"
    fs = client.FileSystem(fs_url)

    while True:

        target = q.get()
        if target is None:
            q.task_done()
            return

        status, _ = fs.prepare([target], PrepareFlags.STAGE)

        if not status.ok and err_budget > 0:
            print(f"[prepare worker {wid}] prepare failed: {target}, error message: {status.message}", flush=True)
            err_budget -= 1

        q.task_done()
