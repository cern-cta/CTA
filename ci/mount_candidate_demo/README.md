# Mount Candidate Demo

This directory contains a small operator demo for the mount-candidate scheduler
flow. It is meant to run against a local CTA development deployment.

## Deploy

Use the PostgreSQL scheduler and put all drives in the same logical library:

```sh
./ci/cta-dev.sh up --scheduler-type pgsched --spawn-options "--one-logical-library --max-drives 2"
```

The `mountcandidate` command requires the PostgreSQL scheduler backend.

## Setup

Source the setup script so the wrapper directory is added to your current
`PATH`:

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
```

## Demo

Run:

```sh
ci/mount_candidate_demo/demo.sh --namespace dev
```

The demo:

1. Finds a logical library with at least two drives and three tapes.
2. Archives one file to each of up to three tapes by keeping only one tape active
   at a time.
3. Blocks all tapes and drives.
4. Submits one archive request and retrieve requests for the archived files.
5. Shows `cta-admin mountcandidate ls`.
6. Raises one retrieve candidate's score with `mountcandidate ch`.
7. Reactivates tapes and a drive, then shows that the boosted retrieve mount is
   selected ahead of the archive candidate.

By default the script restores tapes/drives and removes the demo directory on
exit. Use `--keep-state` if you want to inspect the final state manually.

The wrappers assume the standard development pod names: `cta-cli-0` and
`cta-client-0`.
