import subprocess
import re

def list_all_tapes_in_libraries(library_devices: list[str]) -> list[str]:
    """Lists unique volume tags from multiple tape libraries."""
    volume_tags = set()
    for library_device in library_devices:
        # Ensure the library device path is correctly formatted
        library_device_full = library_device if library_device.startswith("/dev/") else f"/dev/{library_device}"
        cmd = ["mtx", "-f", library_device_full, "status"]
        process = subprocess.run(cmd, capture_output=True, text=True)
        if process.returncode != 0:
            raise RuntimeError(f"Command failed on {library_device_full} with exit code {process.returncode}: {process.stderr}")
        output = process.stdout.splitlines()
        # Extract tape VIDs
        for line in output:
            if "Storage Element" in line and "Full" in line:
                match = re.search(r'VolumeTag *= *(\S{6})', line)
                if match:
                    volume_tags.add(match.group(1))

    return sorted(volume_tags)

