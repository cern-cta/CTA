import re

def list_all_tapes_in_libraries(ctataped_hosts) -> list[str]:
    """Lists unique volume tags from multiple tape libraries."""
    volume_tags = set()
    for ctataped in ctataped_hosts:
        library_device = ctataped.library_device()
        output: list[str] = ctataped.execWithOutput(f"mtx -f {library_device} status").splitlines()
        # Extract tape VIDs
        for line in output:
            if "Storage Element" in line and "Full" in line:
                match = re.search(r'VolumeTag *= *(\S{6})', line)
                if match:
                    volume_tags.add(match.group(1))

    return sorted(volume_tags)


def get_loaded_drives(ctarmcd):
    """Retrieves a list of loaded drives and their corresponding slots for the library device associated with the provided ctarmcd host."""
    status_output = ctarmcd.execWithOutput(f"mtx -f {ctarmcd.library_device()} status").splitlines()
    loaded_drives = []

    for line in status_output:
        match = re.search(r'Data Transfer Element (\d+):Full \(Storage Element (\d+) Loaded\)', line)
        if match:
            drive = int(match.group(1))
            slot = int(match.group(2))
            loaded_drives.append((drive, slot))

    return loaded_drives

def unload_tapes(ctarmcd):
    """Unloads all loaded tapes from their drives for the library device associated with the provided rmcd host."""
    library_device = ctarmcd.library_device()
    loaded_drives = get_loaded_drives(ctarmcd)
    for drive, slot in loaded_drives:
        ctarmcd.exec(f"mtx -f {library_device} unload {slot} {drive}")
