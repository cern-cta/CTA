from .remote_host import RemoteHost


class ClientHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)

    # Get the eos instance name dynamically
    # Can we somehow make this somewhat agnostic to EOS? I guess not...
    def generate_and_copy_files_to_disk_instance(
        self,
        disk_instance_name: str,
        directory: str,
        num_files: str,
        file_size: int,
        file_prefix: str = "file",
        num_processes: int = 8,
    ):
        self.exec(f"eos root://{disk_instance_name} mkdir {directory}")
        # We don't really care about file contents, so just generate some arbitrary buffer first
        local_buffer_file = "/tmp/_buffer"
        self.exec(f"dd if=/dev/urandom of={local_buffer_file} bs={file_size} count=1 status=none")
        # now copy that buffer to a bunch of different files
        self.exec(
            f"seq 0 {num_files} | "
            f"xargs -P {num_processes} -I{{}} "
            f"xrdcp {local_buffer_file} root://{disk_instance_name}/{directory}/{file_prefix}_{{}} 2>/dev/null"
        )
        # And remove the buffer again
        self.exec(f"rm {local_buffer_file}")
        print(f"{num_files} files of {file_size} bytes generated in {directory}")
