# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from .disk_client_host import DiskClientHost


class EosClientHost(DiskClientHost):
    def __init__(self, conn):
        super().__init__(conn)

    def install_xrootd_python(self):
        """Ensure python3 and XRootD Python bindings are installed."""
        self.exec("rpm -q python3-xrootd || dnf install -y python3 xrootd-client python3-xrootd")

    def _archive_cmd(self, eos_host, dest_dir, num_files, num_dirs, num_procs, file_size, file_offset, sss_keytab, skip_mkdir, done_file):
        cmd = (
            f"python3 -u /root/xrootd_archive.py "
            f"--eos-host {eos_host} "
            f"--dest-dir {dest_dir} "
            f"--num-files {num_files} "
            f"--num-dirs {num_dirs} "
            f"--num-procs {num_procs} "
            f"--file-size {file_size} "
            f"--file-offset {file_offset} "
            f"--sss-keytab {sss_keytab} "
            f"--done-file {done_file}"
        )
        if skip_mkdir:
            cmd += " --skip-mkdir"
        return cmd

    def archive_files_xrootd_async(
        self,
        eos_host: str,
        dest_dir: str,
        num_files: int,
        num_dirs: int,
        num_procs: int,
        file_size: int = 512,
        log_file: str = "/root/archive.log",
        file_offset: int = 0,
        skip_mkdir: bool = False,
        sss_keytab: str = "/etc/eos.keytab"
    ) -> str:
        """Start archive in background. Returns path to done marker file."""
        import time
        done_file = f"/tmp/archive_done_{int(time.time() * 1000)}_{file_offset}"
        # Remove any stale marker file
        self.exec(f"rm -f {done_file}", throw_on_failure=False)
        cmd = self._archive_cmd(eos_host, dest_dir, num_files, num_dirs, num_procs, file_size, file_offset, sss_keytab, skip_mkdir, done_file)
        self.exec(f"nohup {cmd} > {log_file} 2>&1 &")
        return done_file

    def is_archive_running(self, done_file: str) -> bool:
        """Check if archive is still running by checking for completion marker."""
        result = self.exec(f"test -f {done_file}", throw_on_failure=False)
        # Archive is running if done file does NOT exist yet
        return result.returncode != 0

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
        output = self.execWithOutput(cmd)
        try:
            return int(output.strip())
        except ValueError:
            return 0
