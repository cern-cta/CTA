# CTA Tape Verification Framework

## Introduction
The CTA Tape Verify framework monitors tapes in the CTA system and alerts in case of potential data loss or corruption. Unlike standard user retrievals, verification requests read data from the tape drive but stream the output to `/dev/null` rather than writing it to disk. These requests are flagged internally with `isVerifyOnly` set to true.

## Architecture
The framework consists of three hierarchical components, ranging from high-level automation to low-level execution:

1.  **`cta-ops-verification-feeder`** (Python script): The high-level automation tool. It selects tapes to be verified based on user criteria (e.g., last read date, tape pool) and submits them to the verification queue.
2.  **`cta-ops-verify-tape`** (Python script): The intermediate tool. It selects specific files on a given tape to verify (e.g., first 10, last 10, random). It acts as a wrapper that calls the low-level binary.
3.  **`cta-verify-file`** (Binary): The low-level executable located at `/usr/bin/cta-verify-file`. It submits the actual verification retrieve request for a specific file ID to the CTA Frontend.

## Configuration
**Crucial Note:** Based on operational experience, the majority of deployment issues arise from authentication mismatches and missing mount policies. Follow these steps strictly.

### 1. Define the Verification Mount Policy
You must configure a specific mount policy for verification jobs to ensure they run with the correct priority (usually lower than user requests).

*   **In CTA Admin:** Create the policy.
    ```bash
    cta-admin mp add --name verification --minrequestage 600 --minarchiveage 14400 --priority 50 --comment "Tape Media Verification framework mount policy"
    ```
    *Note: Adjust priorities and ages as required.*

*   **In Frontend Configuration:**
    Edit `/etc/cta/cta-frontend.conf` to map the verification system to this policy. **If this is missing, verification requests will fail or be rejected**.
    ```conf
    cta.verification.mount_policy verification
    ```

### 2. Authentication and Security
Because `cta-verify-file` submits requests to the Frontend, it requires a valid SSS key and an identity recognized by CTA. Even though files are not written to disk, a valid "disk instance" logic is required for the protocol buffer authentication.

*   **Create a Keytab:** Create a keytab (e.g., `/etc/cta/tape-verification.keytab`) with a user/instance name (e.g., `tape-verification`).
*   **Update Frontend Keys:** Ensure the corresponding key is present in `/etc/cta/eos.sss.keytab` on the CTA Frontend to allow authentication.
*   **Register Admin User:** The user defined in the keytab must be registered as an admin in CTA.
    ```bash
    cta-admin admin add --username tape-verification --instance tape-verification --comment "Verification Framework"
    ```

### 3. Client Configuration
Configure the CLI configuration file (typically `/etc/cta/cta-cli.conf`) on the node running the verification scripts. The `eos.instance` must match the key identifier defined in step 2 to avoid "Instance name does not match key identifier" errors.

**Example `/etc/cta/cta-cli.conf`:**
```conf
eos.instance tape-verification
eos.requester.user verification
eos.requester.group it
```

### 4. Ops Tools Configuration
Configure the defaults for the automation tools in `/etc/cta-ops/cta-ops-config.yaml`.

```yaml
cta-ops-tape-verify:
  cta-ops-verify-tape:
    default_read_data_size: '0B'
    default_read_time: 0
    default_first: 10
    default_random: 10
    default_last: 10

  cta-ops-verification-feeder:
    verification_mount_policy: 'verification'
    default_max_verify: 10
    default_verify_options: '--first 10 --last 10 --read_time 30'
```

---

## Usage

### Level 1: Manual File Verification (Debugging)
Before running full automation, verify that a single file can be submitted. This confirms authentication and Mount Policy configuration.

```bash
# Point to your specific keytab if necessary
XrdSecPROTOCOL=sss XrdSecSSSKT=/etc/cta/tape-verification.keytab cta-verify-file --instance tape-verification --id <ArchiveFileID> --vid <TapeVID>
```
*Successful output:* Returns a Request ID (e.g., `RetrieveRequest-Frontend...`).

### Level 2: Tape Verification (`cta-ops-verify-tape`)
To verify a specific tape manually by sampling files:

```bash
cta-ops-verify-tape --vid <VID> --first 10 --last 10 --read_time 30
```
This script scans the tape's file list and submits multiple `cta-verify-file` commands.

### Level 3: Automated Feeder (`cta-ops-verification-feeder`)
To run the automated loop (usually via cron or Rundeck):

```bash
/opt/cta-ops/ops-venv/bin/cta-ops-verification-feeder --maxverify 20 --min_data_on_tape 1000000000000
```
This checks for eligible tapes (based on criteria like `last_read` or `last_verified`) and spawns `cta-ops-verify-tape` processes for them.

## Monitoring

### Queued Jobs
Check `cta-admin sq`. Verification jobs will appear in the queue with the configured mount policy (e.g., "verification").
```bash
cta-admin sq | grep verification
```

### Logs
*   **Feeder Logs:** `/var/log/cta/verification/cta-verification-feeder.log`.
*   **Tape Server Logs (`cta-taped`):** Look for messages with `isVerifyOnly="1"`.
    *   **Success:** `MSG="File successfully read from tape"`, `verifiedFilesCount="1"`, `verifiedBytesCount="..."`.
    *   **Failure:** `MSG="Error reading a file..."` or `MSG="Verification job failed"`.

### Note on "Dummy" Files
In the logs, you will observe the destination URL is `file://dummy` and the disk file path is `dummy`. This is expected behavior as the data is not written to disk.