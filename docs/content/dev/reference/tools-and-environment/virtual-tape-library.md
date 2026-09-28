# Virtual Tape Library with mhVTL

[mhVTL](https://github.com/markh794/mhvtl) presents disk-backed virtual tape drives and libraries as SCSI devices. CTA uses it for development and CI when physical tape hardware is unavailable. It consists of a kernel module and userspace daemons running on the development host.

!!! note "Kubernetes node requirements"
    mhVTL is why a CTA CI instance cannot simply be deployed on any Kubernetes cluster. The nodes running the tape services need the mhVTL kernel module and userspace daemons installed on the host, and the virtual tape and changer devices must be accessible to the pods. Deploying the Helm charts alone does not provide this hardware emulation.

## Purpose and setup

Follow [cta-ci-node-setup](https://gitlab.cern.ch/cta/ci/cta-ci-node-setup#bootstrapping-the-ci-machine) for installation instructions. Its [mhVTL directory](https://gitlab.cern.ch/cta/ci/cta-ci-node-setup/-/tree/master/mhvtl) contains the installer and supported configurations. For the full CTA development workflow, start with [Environment Setup](../../getting-started/environment-setup.md).

From the root of a `cta-ci-node-setup` checkout on the development host, run:

```bash
sudo ./mhvtl/install_mhvtl.sh
```

The installer copies the default `mhvtl/config/ULT` configuration into `/etc/mhvtl`, installs the tape utilities and development headers matching the running kernel, and installs `mhvtl-utils` from the supplied CTA dependencies repository. To select another configuration, pass `-f <configuration-directory>`.

The installer exits without changes if `mhvtl-utils` is already installed, so rerunning it does not update an existing configuration or rebuild the kernel module. For kernel updates, see [Troubleshooting](#troubleshooting).

Run the commands on this page on the development host, not inside a CTA pod. Service management requires root privileges; device access requires the appropriate permissions. Examples use `sudo` where needed.

## Configuration files

| Path | Purpose |
| --- | --- |
| `/etc/mhvtl/mhvtl.conf` | General settings, including default media capacity and logging. |
| `/etc/mhvtl/device.conf` | Library and drive identities, SCSI addresses, and drive-to-library assignments. Keep device identifiers unique. |
| `/etc/mhvtl/library_contents.<id>` | Drive mappings, storage slots, media barcodes, and import/export slots for the library ID. |
| `/opt/mhvtl/` | Default storage for virtual media data and metadata; check the configured library home directory. |

Keep the drive identities and library inventory consistent across the configuration files. Use the configuration supplied by the development-node setup as the starting point for changes.

## Verify the virtual hardware

Check the service target, individual units, and kernel module:

```bash
sudo systemctl status mhvtl.target
systemctl list-units --all 'vtl*'
lsmod | grep mhvtl
```

The `vtllibrary@<id>.service` units run the library daemons, and `vtltape@<id>.service` units run the tape daemons. An active `mhvtl.target` alone does not establish that these daemons are healthy; inspect failed units from the listing.

Discover the SCSI devices:

```bash
lsscsi -g
```

Find the `mediumx` entry for the virtual changer and the `tape` entries for its drives. Match them to the configured library and drive identities. For changer commands, use the generic device path (`/dev/sgN`) shown by `lsscsi`. For a tape listed as `/dev/stN`, use its non-rewinding device `/dev/nstN` for manual tape operations. Do not assume device numbers are fixed.

Replace `<changer-device>` with the discovered changer path:

```bash
sudo mtx -f <changer-device> inquiry
sudo mtx -f <changer-device> status
```

Check that the reported drives, slots, and barcodes match the configured inventory. The CTA development deployment discovers the host library and mounts its device as `/dev/smc` inside the media-changer container; that alias need not exist on the host.

## Manual tape operations

Use these commands only on an idle development drive, after ensuring CTA and tests are no longer using the selected drive and changer. Direct device commands bypass CTA's scheduling and can interfere with an archive or retrieve operation.

From the changer's `status` output, select an occupied storage slot and an empty data-transfer element. Replace the placeholders below with those numbers and the corresponding non-rewinding drive path:

```bash
sudo mtx -f <changer-device> load <slot> <drive-index>
sudo mt -f <non-rewinding-drive> status
sudo mt -f <non-rewinding-drive> rewind
sudo mt -f <non-rewinding-drive> eject
sudo mtx -f <changer-device> unload <slot> <drive-index>
sudo mtx -f <changer-device> status
```

The changer's drive index is not necessarily the number in `/dev/nstN`; verify the mapping before loading. Return the tape to its original slot when finished. For the records written by CTA, see [Tape Format](../../../concepts/tape/media/format.md).

## Troubleshooting

| Problem | What to check |
| --- | --- |
| No virtual devices appear | Check `lsmod` and the mhVTL units above. If the module is absent, run `sudo modprobe mhvtl` and inspect any error. |
| Devices disappear after a kernel update | Check the running kernel with `uname -r` and whether `modinfo mhvtl` finds a module for it. Follow the [node-setup kernel-update instructions](https://gitlab.cern.ch/cta/ci/cta-ci-node-setup#updates), which use `sudo /usr/bin/mhvtl_kernel_mod_build`, then repeat the device checks above. |
| A library or drive daemon fails | Use `sudo systemctl status <unit>` and `sudo journalctl -u <unit> -b` for the failing unit. Check the reported configuration, permissions, and media-storage errors. |
| Slots contain barcodes but loading fails | Check that the corresponding media files exist in the configured storage directory. Upstream provides `make_vtl_media` when installation has not created them; follow its installation instructions for your version. Starting the service alone does not guarantee that media exist. |
| The host sees devices but CTA cannot use them | Check the deployment's device mappings and permissions, then inspect the tape-daemon or media-changer logs using [Working with Development Pods](development-pods.md). |

For maintenance on a shared CI runner, first follow [CI Maintenance](../../contributing/maintainers/ci-maintenance.md) to pause the runner and wait for its jobs to finish.
