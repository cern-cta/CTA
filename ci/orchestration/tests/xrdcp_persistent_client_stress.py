#!/usr/bin/env python3

import os
import subprocess
import multiprocessing as mp
import uuid
import time
import re

# IMPORTANT: set SSS env before importing XRootD / libXrdCl
SSSKEY = os.environ.get("SSSKEY", "/etc/eos.keytab")
os.environ["XrdSecsssKT"] = SSSKEY
os.environ["XRD_LOGLEVEL"] = os.environ.get("XRD_LOGLEVEL", "Error")

from XRootD import client
from XRootD.client.flags import OpenFlags, PrepareFlags

# ----------------------------
# CONFIG
# ----------------------------

NB_FILES = int(os.environ.get("NB_FILES", "2000000"))
NB_PROCS = int(os.environ.get("NB_PROCS", "40"))
NB_DIRS = int(os.environ.get("NB_DIRS", "100"))

NB_FILES_TO_PUT_DRIVES_UP = int(os.environ.get("NB_FILES_TO_PUT_DRIVES_UP", "1000000"))
DRIVE_UP = os.environ.get("DRIVE_UP", ".*")

CHECK_EVERY_SEC = int(os.environ.get("CHECK_EVERY_SEC", "9"))

EOS_MGM_HOST = os.environ.get("EOS_MGM_HOST", "ctaeos")
EOS_DIR = os.environ.get("EOS_DIR", "/eos/ctaeos/cta")

KRB5CC_CTAADMIN2 = os.environ.get("KRB5CC_CTAADMIN2", "/tmp/ctaadmin2/krb5cc_0")

RUN_ID = str(uuid.uuid4())

TEST_FILE_PATH_BASE = f"root://{EOS_MGM_HOST}/{EOS_DIR}/{RUN_ID}"
JUST_PATH = f"{EOS_DIR}/{RUN_ID}"

# ----------------------------
# variables for retrieve
# ----------------------------
STALL_SEC = int(os.environ.get("STALL_SEC", "900"))  # 15 minutes
ACTIVITY = os.environ.get("ACTIVITY", "T0Reprocess")
KRB5CC_POWER = os.environ.get("KRB5CC_POWER", "FILE:/tmp/poweruser1/krb5cc_0")
RETRIEVE_NO_PROGRESS_TIMEOUT = int(os.environ.get("RETRIEVE_NO_PROGRESS_TIMEOUT", "300"))
RETRIEVE_TIMEOUT = int(os.environ.get("RETRIEVE_TIMEOUT", str(6600 + NB_FILES // 10))) # default same as in client_stress_ar.sh


def bootstrap_kerberos_or_die():
    """
    Run the same helper functions you run manually in the client pod.
    Assumes /root/client_helper.sh defines:
      - die()
      - user_kinit
      - eospower_kdestroy
      - eospower_kinit
      - admin_kinit
    """
    shell = r"""
      set -euo pipefail
      source /root/client_helper.sh

      # Get kerberos credentials for user1
      user_kinit
      klist -s || die "Cannot get kerberos credentials for user ${USER}"

      # Get kerberos credentials for poweruser1
      eospower_kdestroy || true
      eospower_kinit

      # Get kerberos credentials for CTA admin
      admin_kinit >/dev/null 2>&1

      # Optional: show what we got (short + useful)
      echo ">>> Kerberos bootstrap OK"
      echo ">>> user cache:"
      klist 2>/dev/null | sed -n "1,4p" || true
      echo ">>> poweruser cache:"
      klist -c FILE:/tmp/poweruser1/krb5cc_0 2>/dev/null | sed -n "1,4p" || true
      echo ">>> admin cache:"
      klist -c FILE:/tmp/ctaadmin2/krb5cc_0 2>/dev/null | sed -n "1,4p" || true
    """

    p = subprocess.run(
        ["bash", "-lc", shell],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if p.returncode != 0:
        print("!!! Kerberos bootstrap failed", flush=True)
        print("stdout:", p.stdout.strip(), flush=True)
        print("stderr:", p.stderr.strip(), flush=True)
        raise SystemExit(1)

    # Show confirmation
    print(p.stdout.strip(), flush=True)


# ----------------------------
# Helpers
# ----------------------------

def mkdir_dirs():
    print(f"Creating run directory: {TEST_FILE_PATH_BASE}", flush=True)
    for i in range(NB_DIRS):
        path = f"{JUST_PATH}/{i}"
        p = subprocess.run(
            ["eos", f"root://{EOS_MGM_HOST}", "mkdir", "-p", path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if p.returncode != 0:
            print(f"!!! eos mkdir failed for {path}", flush=True)
            print("stdout:", p.stdout.strip(), flush=True)
            print("stderr:", p.stderr.strip(), flush=True)
            raise SystemExit(1)


def admin_cta(*args) -> bool:
    env = os.environ.copy()

    # Use the admin cache exactly like your working CLI command
    env["KRB5CCNAME"] = os.environ.get("KRB5CC_CTAADMIN2", "/tmp/ctaadmin2/krb5cc_0")

    # Critical: prevent XRootD SSS from being selected for cta-admin
    for k in (
        "XrdSecsssKT", "XRDSSSKT", "XrdSecPROTOCOL", "XrdSecPROTOCOLS",
        "XrdSecGSISRVNAMES", "X509_USER_PROXY", "X509_CERT_DIR",
    ):
        env.pop(k, None)

    # If your build supports it, explicitly prefer kerberos
    env["XrdSecPROTOCOL"] = "krb5"

    # Also prevent keytab-based overrides
    env.pop("KRB5_CLIENT_KTNAME", None)
    env.pop("KRB5_KTNAME", None)

    print(f">>> CTA: KRB5CCNAME={env['KRB5CCNAME']} cta-admin {' '.join(args)}", flush=True)

    p = subprocess.run(
        ["cta-admin", *args],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if p.returncode != 0:
        print("!!! admin_cta failed", flush=True)
        print("stdout:", p.stdout.strip(), flush=True)
        print("stderr:", p.stderr.strip(), flush=True)
        return False
    return True



def admin_cta_drive_up():
    return admin_cta("drive", "up", DRIVE_UP, "--reason", "PUTTING DRIVE UP FOR TESTS")


def admin_cta_drive_down():
    return admin_cta("drive", "down", DRIVE_UP, "--reason", "PUTTING DRIVE DOWN FOR TESTS")



_FILES_RE = re.compile(r"\bFiles:\s*(\d+)\b")


def _dir_files_count(path: str):
    try:
        p = subprocess.run(
            ["eos", f"root://{EOS_MGM_HOST}", "file", "info", path],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=10,
        )
    except subprocess.TimeoutExpired:
        return 0

    if p.returncode != 0 or not p.stdout:
        return 0

    m = _FILES_RE.search(p.stdout)
    return int(m.group(1)) if m else 0


def count_files_in_namespace():
    # Keep this modest; too high just adds mgm load.
    COUNT_PROCS = int(os.environ.get("COUNT_PROCS", "5"))

    paths = [f"{JUST_PATH}/{i}" for i in range(NB_DIRS)]
    with mp.Pool(processes=COUNT_PROCS) as pool:
        return sum(pool.map(_dir_files_count, paths))


# ----------------------------
# Worker (persistent XRootD)
# ----------------------------

def worker(work_q: mp.JoinableQueue, wid: int):
    _ = client.FileSystem(f"root://{EOS_MGM_HOST}")
    err_budget = 3

    while True:
        file_num = work_q.get()
        if file_num is None:
            work_q.task_done()
            return

        subdir = file_num % NB_DIRS

        # Exactly 16 bytes: 4 hex subdir + 8 hex file_num + 4 zeros
        payload = (f"{subdir:04x}{file_num:08x}" + "0" * 4).encode("ascii")

        dest_url = f"root://{EOS_MGM_HOST}/{EOS_DIR}/{RUN_ID}/{subdir}/test{file_num:07d}"

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


def prepare_worker(q: mp.JoinableQueue, wid: int):
    # Worker process: switch from SSS to Kerberos (poweruser1) for prepare permissions
    for k in ("XrdSecsssKT", "XRDSSSKT"):
        os.environ.pop(k, None)
    os.environ["XrdSecPROTOCOL"] = "krb5"
    os.environ["KRB5CCNAME"] = KRB5CC_POWER

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


def run_prepare_parallel_async(targets):
    """
    Starts prepare workers and returns (queue, done_event).
    Caller can monitor and then wait by calling q.join().
    """
    print(f"Retrieving files back from tapes. (prepare -s) targets={len(targets)} procs={NB_PROCS}", flush=True)

    q = mp.JoinableQueue()

    for wid in range(NB_PROCS):
        mp.Process(target=prepare_worker, args=(q, wid), daemon=True).start()

    for t in targets:
        q.put(t)

    for _ in range(NB_PROCS):
        q.put(None)

    return q


def count_files_on_tape(return_paths: bool = False):
    total = 0
    ontape = 0
    ontape_paths = [] if return_paths else None

    for i in range(NB_DIRS):
        dir_path = f"{JUST_PATH}/{i}"
        p = subprocess.run(
            ["eos", f"root://{EOS_MGM_HOST}", "ls", "-ly", dir_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        if p.returncode != 0 or not p.stdout:
            continue

        lines = p.stdout.splitlines()
        total += len(lines)

        for ln in lines:
            if ln.startswith("d0::t1"):
                ontape += 1
                if return_paths:
                    fname = ln.rsplit(None, 1)[-1]
                    ontape_paths.append(f"{dir_path}/{fname}")

    return total, ontape, ontape_paths


def wait_for_retrieve_completion(total_to_retrieve: int):
    """Poll until tape-only files are retrieved to disk, tracking d0::t1 count decreasing."""
    print(f"[retrieve wait] Waiting for {total_to_retrieve} files to be retrieved from tape.", flush=True)
    print(f"[retrieve wait] poll={CHECK_EVERY_SEC}s no_progress_timeout={RETRIEVE_NO_PROGRESS_TIMEOUT}s timeout={RETRIEVE_TIMEOUT}s", flush=True)

    start = time.time()
    last_progress_time = start
    last_tape_only = total_to_retrieve

    while True:
        _, tape_only, _ = count_files_on_tape(return_paths=False)
        now = time.time()
        elapsed = int(now - start)
        retrieved = total_to_retrieve - tape_only

        if tape_only < last_tape_only:
            last_progress_time = now
            last_tape_only = tape_only

        no_progress_for = int(now - last_progress_time)
        print(f"[retrieve wait] {retrieved}/{total_to_retrieve} retrieved, tape_only={tape_only}, elapsed={elapsed}s, no_progress={no_progress_for}s", flush=True)

        if tape_only <= 0:
            print(f"[retrieve wait] All {total_to_retrieve} files retrieved.", flush=True)
            break

        if no_progress_for >= RETRIEVE_NO_PROGRESS_TIMEOUT:
            print(f"[retrieve wait] No progress for {RETRIEVE_NO_PROGRESS_TIMEOUT}s — treating as done.", flush=True)
            break

        if elapsed >= RETRIEVE_TIMEOUT:
            print(f"[retrieve wait] Absolute timeout ({RETRIEVE_TIMEOUT}s) reached.", flush=True)
            break

        time.sleep(CHECK_EVERY_SEC)

    return total_to_retrieve - tape_only


def wait_for_tape_stall_and_get_prepare_targets():
    last_ontape = -1
    last_change_ts = time.time()

    print(f"[tape check] polling={CHECK_EVERY_SEC}s stall={STALL_SEC}s activity={ACTIVITY}", flush=True)

    while True:
        total, ontape, _ = count_files_on_tape(return_paths=False)
        now = time.time()

        if ontape > last_ontape:
            last_ontape = ontape
            last_change_ts = now

        stalled_for = int(now - last_change_ts)
        print(f"[tape check] total={total} ontape={ontape} stalled_for={stalled_for}s", flush=True)

        if (stalled_for >= STALL_SEC or total == ontape):
            _, ontape2, paths = count_files_on_tape(return_paths=True)
            print(f"[tape check] STALLED >= {STALL_SEC}s -> stage-in list for {ontape2} files", flush=True)
            return [f"{p}?activity={ACTIVITY}" for p in paths]

        time.sleep(CHECK_EVERY_SEC)


def prequeue_retrieve_and_put_drives_up_again():
    print("Waiting for files to be archived.", flush=True)
    prepare_targets = wait_for_tape_stall_and_get_prepare_targets()

    """
    Drives down (prequeue), start prepare requests, then put drives up again
    when on-tape count reaches threshold. Uses CHECK_EVERY_SEC for polling.
    """
    print("Prequeue retrieve: putting drives DOWN.", flush=True)
    admin_cta_drive_down()

    # Start prepare in background
    q = run_prepare_parallel_async(prepare_targets)

    # Put drives up again after threshold (same mechanism as archive)
    drive_up_done = False
    start = time.time()
    while not drive_up_done:
        time.sleep(CHECK_EVERY_SEC)
        _, ontape, _ = count_files_on_tape(return_paths=False)
        rate = ontape / max(1e-9, (time.time() - start))
        print(f"[retrieve prequeue check] ontape={ontape} (~{rate:.1f} Hz)", flush=True)

        if ontape >= NB_FILES_TO_PUT_DRIVES_UP:
            print("[retrieve prequeue check] threshold reached -> putting drives UP.", flush=True)
            admin_cta_drive_up()
            drive_up_done = True

        # safety: don’t loop forever
        if time.time() - start > 3600:
            print("!!! retrieve prequeue monitoring timeout (3600s) reached", flush=True)
            admin_cta_drive_up()
            break

    # Wait for prepare to finish
    q.join()
    print("Prepare stage-in done.", flush=True)

    # Wait for files to actually be retrieved from tape to disk
    total_to_retrieve = len(prepare_targets)
    retrieved = wait_for_retrieve_completion(total_to_retrieve)
    print(f"Retrieve phase complete: {retrieved}/{total_to_retrieve} files retrieved.", flush=True)


# ----------------------------
# Main
# ----------------------------

if __name__ == "__main__":
    print(f"Run ID     : {RUN_ID}", flush=True)
    print(f"NB_FILES   : {NB_FILES}", flush=True)
    print(f"NB_DIRS    : {NB_DIRS}", flush=True)
    print(f"NB_PROCS   : {NB_PROCS}", flush=True)
    print(f"NB_FILES_TO_PUT_DRIVES_UP : {NB_FILES_TO_PUT_DRIVES_UP}", flush=True)
    print(f"CHECK_EVERY_SEC : {CHECK_EVERY_SEC}", flush=True)
    print(f"DRIVE_UP   : {DRIVE_UP}", flush=True)
    print(f"TEST_FILE_PATH_BASE : {TEST_FILE_PATH_BASE}", flush=True)
    print(f"(mkdir base path)   : {JUST_PATH}", flush=True)
    print(f"SSSKEY     : {SSSKEY}", flush=True)
    print(f"KRB5CCNAME  : {KRB5CC_CTAADMIN2}", flush=True)
    print("NOTE: final line printed will be JUST_PATH so callers can capture it.", flush=True)
    bootstrap_kerberos_or_die()
    admin_cta_drive_down()
    mkdir_dirs()

    work_q = mp.JoinableQueue()

    procs = []
    for wid in range(NB_PROCS):
        p = mp.Process(target=worker, args=(work_q, wid), daemon=True)
        p.start()
        procs.append(p)

    # Enqueue all work
    for i in range(NB_FILES):
        work_q.put(i)

    # Monitor namespace count and trigger drives up once
    drive_up_done = False
    start = time.time()
    last_check = 0.0

    while not drive_up_done:
        time.sleep(1)
        now = time.time()
        if now - last_check >= CHECK_EVERY_SEC:
            total_files = count_files_in_namespace()
            rate = total_files / max(1e-9, (now - start))
            print(f"[namespace check] total_files={total_files} (~{rate:.1f} Hz)", flush=True)

            if total_files >= NB_FILES_TO_PUT_DRIVES_UP:
                admin_cta_drive_up()
                drive_up_done = True

            last_check = now

        # Optional early exit: if we already created almost all files, stop monitoring
        # (it will still do a final check below)
        if now - start > 3600 and not drive_up_done:
            # safety: don't loop forever if something is wrong
            print("!!! monitoring timeout (3600s) reached", flush=True)
            break

    # Signal workers to exit cleanly
    for _ in range(NB_PROCS):
        work_q.put(None)

    # Wait until all tasks are marked done
    work_q.join()

    # Final namespace check
    total_files = count_files_in_namespace()
    print(f"[final namespace check] total_files={total_files}", flush=True)
    if (not drive_up_done) and total_files >= NB_FILES_TO_PUT_DRIVES_UP:
        admin_cta_drive_up()
        drive_up_done = True

    print("Archive phase done.", flush=True)

    # Retrieve phase: prequeue + drives up again after threshold, same CHECK_EVERY_SEC
    prequeue_retrieve_and_put_drives_up_again()

    print("Retrieve phase done.", flush=True)
    print(JUST_PATH, flush=True)
