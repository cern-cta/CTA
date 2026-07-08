# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


import asyncio

from ...connections.remote_connection import ExecResult
from .disk_client_host import DiskClientHost


class EosClientHost(DiskClientHost):
    def __init__(self, conn):
        super().__init__(conn)

    def install_xrootd_python(self):
        """Ensure python3 and XRootD Python bindings are installed."""
        self.exec("rpm -q python3-xrootd || dnf install -y python3 xrootd-client python3-xrootd")

    def count_files_in_namespace(self, eos_host: str, dest_dir: str, num_dirs: int, count_procs: int) -> int:
        """Count files in namespace using parallel queries on the remote host.

        Requires count_files.py to be deployed to /root/count_files.py first.
        """
        cmd = (
            f"python3 /root/count_files.py "
            f"--eos-host {eos_host} "
            f"--dest-dir {dest_dir} "
            f"--num-dirs {num_dirs} "
            f"--num-procs {count_procs}"
        )
        output = self.exec_with_output(cmd)
        try:
            return int(output.strip())
        except ValueError:
            return 0

    def archive_async(
        self,
        eos_host: str,
        dest_dir: str,
        num_files: int,
        num_dirs: int,
        num_procs: int,
        *,
        file_size: int = 512,
        batch_size: int = 1000,
        sss_keytab: str = "/etc/eos.keytab",
        write_files_in_chunks: bool = False,
    ) -> asyncio.Future[ExecResult]:
        """Start archival asynchronously. Returns a future that can be awaited."""
        cmd = (
            f"python3 -u /root/xrootd_archive.py "
            f"--eos-host {eos_host} "
            f"--dest-dir {dest_dir} "
            f"--num-files {num_files} "
            f"--num-dirs {num_dirs} "
            f"--num-procs {num_procs} "
            f"--file-size {file_size} "
            f"--batch-size {batch_size} "
            f"--sss-keytab {sss_keytab}"
        )
        if write_files_in_chunks:
            cmd += " --write-files-in-chunks"
        return self.exec_async(cmd)

    def archive_file(
        self,
        disk_instance_name: str,
        destination_path: str,
        source_path: str,
        *,
        wait: bool = True,
        wait_for_evict: bool = True,
        wait_timeout_secs: int = 20,
    ) -> None:
        # TODO: specify protocol?
        print(f"Copying {source_path} to {destination_path} on disk instance {disk_instance_name}")
        self.exec(f"xrdcp {source_path} root://{disk_instance_name}/{destination_path}")
        if wait:
            self.wait_for_file_archival(disk_instance_name, destination_path, wait_timeout_secs=wait_timeout_secs)
            if wait_for_evict:
                self.wait_for_file_eviction(disk_instance_name, destination_path, wait_timeout_secs=wait_timeout_secs)

    def retrieve_file(
        self,
        disk_instance_name: str,
        path: str,
        *,
        user: str = "poweruser1",
        wait: bool = True,
        wait_timeout_secs: int = 20,
    ) -> None:
        print(f"Retrieving {path} on disk instance {disk_instance_name}")
        # We need the -s as we are staging the files from tape (see xrootd prepare definition)
        self.exec(f'KRB5CCNAME=/tmp/{user}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs {disk_instance_name} prepare -s "{path}"')
        if wait:
            self.wait_for_file_retrieval(disk_instance_name, path, wait_timeout_secs=wait_timeout_secs)

    def evict_file(
        self,
        disk_instance_name: str,
        path: str,
        *,
        user: str = "eosadmin1",
        wait: bool = True,
        wait_timeout_secs: int = 20,
    ) -> None:
        print(f"Evicting {path} on disk instance {disk_instance_name}")
        self.exec(
            f'KRB5CCNAME=/tmp/{user}/krb5cc_0 XrdSecPROTOCOL=krb5 eos -r 0 0 root://{disk_instance_name} file drop "{path}" 1'
        )
        if wait:
            self.wait_for_file_eviction(disk_instance_name, path, wait_timeout_secs=wait_timeout_secs)

    def delete_file(self, disk_instance_name: str, path: str) -> None:
        print(f"Deleting {path} on disk instance {disk_instance_name}")
        self.exec(f"eos root://{disk_instance_name} rm --no-confirmation {path}")

    def retrieve_async(
        self,
        eos_host: str,
        dest_dir: str,
        num_dirs: int,
        num_procs: int,
        *,
        krb5_cache: str = "/tmp/poweruser1/krb5cc_0",
        activity: str = "T0Reprocess",
    ) -> asyncio.Future[ExecResult]:
        """Start retrieve (prepare/stage-in) asynchronously. Returns a future that can be awaited."""
        cmd = (
            f"python3 -u /root/xrootd_retrieve.py "
            f"--eos-host {eos_host} "
            f"--dest-dir {dest_dir} "
            f"--num-dirs {num_dirs} "
            f"--num-procs {num_procs} "
            f"--krb5-cache {krb5_cache} "
            f"--activity {activity}"
        )
        return self.exec_async(cmd)

    def is_file_on_tape_only(self, disk_instance_name: str, path: str) -> bool:
        return int(self.exec_with_output(f'eos root://{disk_instance_name} ls {path} -y | grep "d0::t1" | wc -l')) == 1

    def is_file_on_tape(self, disk_instance_name: str, path: str) -> bool:
        return int(self.exec_with_output(f'eos root://{disk_instance_name} ls {path} -y | grep "::t1" | wc -l')) == 1

    def is_file_on_disk(self, disk_instance_name: str, path: str) -> bool:
        return int(self.exec_with_output(f'eos root://{disk_instance_name} ls {path} -y | grep "d1::" | wc -l')) == 1

    def is_file_on_disk_only(self, disk_instance_name: str, path: str) -> bool:
        return int(self.exec_with_output(f'eos root://{disk_instance_name} ls {path} -y | grep "d1::t0" | wc -l')) == 1

    def file_info(self, disk_instance_name: str, path: str) -> str:
        return self.exec_with_output(f"eos root://{disk_instance_name} file info {path}")
