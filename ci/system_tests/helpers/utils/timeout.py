# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import time


class Timeout:
    def __init__(self, seconds):
        self.seconds = seconds

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, exc_type, exc, tb):
        pass

    @property
    def expired(self):
        return time.time() - self.start > self.seconds
