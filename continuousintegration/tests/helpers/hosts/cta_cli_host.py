import json
import time

from .remote_host import RemoteHost

class CtaCliHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)


    def get_drive_status(self, drive_name: str) -> str:
        output = self.execWithOutput(f"cta-admin --json drive ls {drive_name}")
        drive_list = json.loads(output)

        if len(drive_list) == 0:
            raise RuntimeError(f"Failed to find drive: {drive_name}")

        drive = drive_list[0]
        if drive["driveName"] == drive_name:
            return drive["driveStatus"]

        raise RuntimeError(f"Failing to find drive status for drive: {drive_name}")

    def wait_for_all_drives_status(self, desired_status: str, timeout: int = 10):
        print(f"Waiting for drives to be {desired_status}")
        seconds_passed = 0

        while seconds_passed < timeout:
            drives_info = json.loads(self.execWithOutput("cta-admin --json drive ls"))
            all_in_desired_status: bool = True
            for drive in drives_info:
                if drive["driveStatus"] != desired_status:
                    all_in_desired_status = False
                    break

            if all_in_desired_status:
                print(f"Drives are set {desired_status}")
                return
            seconds_passed += 1
            time.sleep(1)

        raise RuntimeError(f"Timeout reached while trying to put drives to: {desired_status}")

    def wait_for_all_drives_down(self):
        self.wait_for_all_drives_status("DOWN")

    def wait_for_all_drives_up(self):
        self.wait_for_all_drives_status("UP")

    def set_all_drives_up(self, wait: bool = True):
        self.exec("cta-admin dr up '.*' --reason 'Setting all drives up'")
        if wait:
            self.wait_for_all_drives_up()

    def set_all_drives_down(self, wait: bool = True):
        self.exec("cta-admin dr down '.*' --reason 'Setting all drives down'")
        if wait:
            self.wait_for_all_drives_down()
