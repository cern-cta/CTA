#!/usr/bin/env python3
"""
Quick benchmark: xrdcp --parallel vs persistent XRootD Python client.

Usage (on the client pod):
  python3 -u bench_archive.py

Env vars (all optional):
  NB_FILES           total files to write (default 500)
  NB_PROCS           worker count for Python approach (default 10)
  XRDCP_PARALLEL     --parallel value for xrdcp (default NB_PROCS)
  EOS_MGM_HOST       EOS MGM hostname (default ctaeos)
  EOS_DIR            base EOS path (default /eos/ctaeos/cta)
  SSSKEY             SSS keytab path (default /etc/eos.keytab)
"""

"""
To run it, copy it to the client pod and execute:

  kubectl -n <ns> cp ci/orchestration/tests/bench_archive.py cta-client-0:/root/bench_archive.py -c client
  kubectl -n <ns> exec cta-client-0 -c client -- python3 -u /root/bench_archive.py

  Defaults: 500 files, 10 processes. Tune with env vars:

  kubectl -n <ns> exec cta-client-0 -c client -- bash -c '
    NB_FILES=2000 NB_PROCS=40 python3 -u /root/bench_archive.py
"""

import os
import subprocess
import multiprocessing as mp
import uuid
import time
import shutil
import tempfile

SSSKEY = os.environ.get("SSSKEY", "/etc/eos.keytab")
os.environ["XrdSecsssKT"] = SSSKEY
os.environ["XRD_LOGLEVEL"] = os.environ.get("XRD_LOGLEVEL", "Error")

from XRootD import client
from XRootD.client.flags import OpenFlags

NB_FILES = int(os.environ.get("NB_FILES", "10000"))
NB_PROCS = int(os.environ.get("NB_PROCS", "10"))
XRDCP_PARALLEL = int(os.environ.get("XRDCP_PARALLEL", str(NB_PROCS)))
EOS_MGM_HOST = os.environ.get("EOS_MGM_HOST", "ctaeos")
EOS_DIR = os.environ.get("EOS_DIR", "/eos/ctaeos/cta")

RUN_ID = str(uuid.uuid4())[:8]
BENCH_DIR = f"{EOS_DIR}/bench_{RUN_ID}"


def eos_mkdir(path):
    subprocess.run(
        ["eos", f"root://{EOS_MGM_HOST}", "mkdir", "-p", path],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True,
    )


def eos_rm(path):
    subprocess.run(
        ["eos", f"root://{EOS_MGM_HOST}", "rm", "-rF", path],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


# -------------------------------------------------------------------
# Approach 1: xrdcp --parallel (one subprocess per directory batch)
# -------------------------------------------------------------------

def bench_xrdcp():
    dest_dir = f"{BENCH_DIR}/xrdcp"
    eos_mkdir(dest_dir)

    # Create local temp files
    tmpdir = tempfile.mkdtemp(prefix="bench_xrdcp_")
    for i in range(NB_FILES):
        fpath = os.path.join(tmpdir, f"file{i:07d}")
        with open(fpath, "wb") as f:
            f.write(f"{i:016x}".encode("ascii"))

    t0 = time.time()
    p = subprocess.run(
        [
            "xrdcp", "--parallel", str(XRDCP_PARALLEL),
            "--recursive", tmpdir + "/",
            f"root://{EOS_MGM_HOST}/{dest_dir}/",
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    elapsed = time.time() - t0

    shutil.rmtree(tmpdir)

    if p.returncode != 0:
        print(f"  xrdcp FAILED: {p.stderr.strip()}")

    return elapsed


# -------------------------------------------------------------------
# Approach 2: persistent XRootD Python client (multiprocessing)
# -------------------------------------------------------------------

def python_worker(work_q: mp.JoinableQueue, wid: int, dest_dir: str):
    _ = client.FileSystem(f"root://{EOS_MGM_HOST}")

    while True:
        file_num = work_q.get()
        if file_num is None:
            work_q.task_done()
            return

        payload = f"{file_num:016x}".encode("ascii")
        dest_url = f"root://{EOS_MGM_HOST}/{dest_dir}/file{file_num:07d}"

        f = client.File()
        st, _ = f.open(dest_url, OpenFlags.NEW | OpenFlags.WRITE, 0o644)
        if st.ok:
            f.write(payload, 0)
            f.close()

        work_q.task_done()


def bench_python():
    dest_dir = f"{BENCH_DIR}/python"
    eos_mkdir(dest_dir)

    work_q = mp.JoinableQueue()

    t0 = time.time()

    procs = []
    for wid in range(NB_PROCS):
        p = mp.Process(target=python_worker, args=(work_q, wid, dest_dir), daemon=True)
        p.start()
        procs.append(p)

    for i in range(NB_FILES):
        work_q.put(i)

    for _ in range(NB_PROCS):
        work_q.put(None)

    work_q.join()
    elapsed = time.time() - t0

    return elapsed


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Benchmark: {NB_FILES} files, {NB_PROCS} procs, xrdcp --parallel {XRDCP_PARALLEL}")
    print(f"Run ID: {RUN_ID}")
    print(f"Dest: {BENCH_DIR}")
    print()

    print("Running: xrdcp --parallel ...")
    t_xrdcp = bench_xrdcp()
    rate_xrdcp = NB_FILES / t_xrdcp
    print(f"  xrdcp:  {t_xrdcp:.2f}s  ({rate_xrdcp:.1f} files/s)")

    print()

    print("Running: persistent XRootD Python client ...")
    t_python = bench_python()
    rate_python = NB_FILES / t_python
    print(f"  python: {t_python:.2f}s  ({rate_python:.1f} files/s)")

    print()
    faster = "python" if t_python < t_xrdcp else "xrdcp"
    ratio = max(t_xrdcp, t_python) / max(0.001, min(t_xrdcp, t_python))
    print(f"Winner: {faster} ({ratio:.1f}x faster)")

    # Cleanup
    print()
    print("Cleaning up...")
    # eos_rm(BENCH_DIR)
    print("Done.")
