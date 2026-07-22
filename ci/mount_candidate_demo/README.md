# Mount Candidate Demo

This directory contains a small operator demo for the mount-candidate scheduler
flow. It is meant to run against a local CTA development deployment. The demo
script is intentionally a linear example: run it as a script, or copy/paste the
commands into a terminal and inspect the state between steps.

## Requirements

The `mountcandidate` command requires the PostgreSQL scheduler backend. The demo
also assumes the standard development pod names, especially `cta-cli-0`,
`cta-client-0` and `cta-scheduler-postgres-db`.

The deployment must use one logical library with two drives:

```sh
./ci/cta-dev.sh up --scheduler-type pgsched --spawn-options "--one-logical-library --max-drives 2"
```

The current [demo.sh](demo.sh) runs the local setup itself:

```sh
./ci/cta-dev.sh install
cta-dev up --scheduler-type pgsched --spawn-options "--one-logical-library --max-drives 2"
cta-dev deploy --scheduler-type pgsched --spawn-options "--one-logical-library --max-drives 2"
```

If the deployment is already running, copy/paste from the setup section of the
script instead of rerunning the deploy commands.

## Setup

Source the setup script so the wrapper directory is added to the current `PATH`:

```sh
source ci/mount_candidate_demo/setup.sh --namespace dev
```

The setup script runs the usual system-test setup suites, verifies that the
deployment is suitable for the demo, and exports:

```sh
PATH=<repo>/ci/mount_candidate_demo/wrappers:$PATH
CTA_DEMO_NAMESPACE=<namespace>
```

After setup, these commands can be run from outside the pods:

```sh
cta-admin --json mountcandidate ls
eos root://ctaeos ls /eos/ctaeos/cta
xrdcp /etc/group root://ctaeos//eos/ctaeos/cta/example
xrdfs ctaeos prepare -s /eos/ctaeos/cta/example
xrdfs-retrieve ctaeos prepare -s /eos/ctaeos/cta/example
```

Use `xrdfs-retrieve` for retrieve/stage requests. It runs as `poweruser1`
inside `cta-client-0` with `XrdSecPROTOCOL=krb5`.

The system-test cleanup restarts the maintd routine pods. That can leave a
mount-candidate refresh lease owned by the previous `cta-rt-mountdecisionloop`
pod until the 300 second lease expires. The demo clears only that lease:

```sh
kubectl -n dev exec cta-scheduler-postgres-db -- psql -U cta -d cta-scheduler-postgres -c "delete from scheduler_mount_work_lock where work_key = 'mount-candidate-refresh';"
```

## Demo

Run:

```sh
ci/mount_candidate_demo/demo.sh
```

The script is not a system test. It does not take arguments, does not restore the
final state, and is written so that each block can be copied into a terminal.

The example does the following:

1. Installs `cta-dev`, deploys with the postgres scheduler, and runs the normal
   system-test setup.
2. Clears the stale mount-candidate refresh lease left by the setup restart.
3. Archives four files to EOS. After each file reaches tape, the script disables
   the active written tape so the next file is pushed to another tape.
4. Prints the tape list, puts all drives down, and submits retrieve requests for
   the four archived files with `xrdfs-retrieve`.
5. Queues two extra archive requests while drives are down.
6. Shows `cta-admin mc ls`, reactivates disabled tapes, and prints EOS, tape,
   scheduler queue and mount-candidate state.
7. Changes retrieve candidate scores with `cta-admin mc ch`, including resetting
   one override back to zero.
8. Brings one drive up and repeatedly prints `cta-admin dr ls` and
   `cta-admin mc ls` until the mount-candidate list is empty.

The score-change commands in the example use fixed retrieve candidate keys such
as `RETRIEVE-ULT102`, `RETRIEVE-ULT103` and `RETRIEVE-ULT104`. If your tape names
are different, adjust those commands after looking at `cta-admin mc ls`.

The archive examples currently use `/etc/group` as the input file:

```sh
xrdcp /etc/group root://ctaeos//eos/ctaeos/cta/example0
```
