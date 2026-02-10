# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from ..remote_host import RemoteHost


class DiskClientHost(RemoteHost):
    # Eventually we specify the client specific code here
    ...

    def archive_random_file(destination: str, wait: bool = True) -> str: ...
