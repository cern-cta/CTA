#!/usr/bin/env python3

import os
import subprocess
import multiprocessing as mp
import uuid
from queue import Empty

NB_FILES = int(os.environ.get("NB_FILES", "200000"))
NB_PROCS = int(os.environ.get("NB_PROCS", "50"))
NB_DIRS  = int(os.environ.get("NB_DIRS", "100"))

#EOS_MGM_HOST = os.environ["EOS_MGM_HOST"]
#EOS_DIR = os.environ["EOS_DIR"]
EOS_MGM_HOST="ctaeos"
EOS_DIR="/eos/ctaeos/cta"
KEYTAB = "/etc/eos.keytab"

RUN_ID = str(uuid.uuid4())

TEST_FILE_PATH_BASE = (
    f"root://{EOS_MGM_HOST}/"
    f"{EOS_DIR}/{RUN_ID}"
)

JUST_PATH = f"{EOS_DIR}/{RUN_ID}"

def mkdir_dirs():
    print(f"Creating run directory: {TEST_FILE_PATH_BASE}")
    for i in range(NB_DIRS):
        path = f"{JUST_PATH}/{i}"
        subprocess.run(
            ["eos", f"root://{EOS_MGM_HOST}", "mkdir", "-p", path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )

def worker(q):
    env = os.environ.copy()
    env["XrdSecsssKT"] = KEYTAB
    env["XRD_LOGLEVEL"] = "Error"

    while True:
        try:
            file_num = q.get(timeout=1)
        except Empty:
            return

        subdir = file_num % NB_DIRS
        payload = f"UNIQ_00{subdir}_{file_num}"
        dest = f"{TEST_FILE_PATH_BASE}/{subdir}/test{file_num:07d}"

        p = subprocess.Popen(
            ["xrdcp", "-", dest],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )
	p.stdin.write(payload.encode())
        p.stdin.close()
        p.wait()

        q.task_done()

if __name__ == "__main__":
    print(f"Run ID     : {RUN_ID}")
    print(f"NB_FILES   : {NB_FILES}")
    print(f"NB_DIRS    : {NB_DIRS}")
    print(f"NB_PROCS   : {NB_PROCS}")
    print(f"TEST_FILE_PATH_BASE : {TEST_FILE_PATH_BASE}")
    mkdir_dirs()

    q = mp.JoinableQueue(maxsize=NB_PROCS * 4)

    workers = []
    for _ in range(NB_PROCS):
        p = mp.Process(target=worker, args=(q,), daemon=True)
        p.start()
        workers.append(p)

    for i in range(NB_FILES):
        q.put(i)

    q.join()
    print("Archive phase done.")