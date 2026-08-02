# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import time
from types import TracebackType
from typing import Optional


class Timeout:
    def __init__(self, seconds: float) -> None:
        self.seconds = seconds

    def __enter__(self) -> "Timeout":
        self.start = time.time()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        # Exceptions must propagate normally; this context manager only tracks elapsed time.
        pass

    @property
    def expired(self) -> bool:
        return time.time() - self.start > self.seconds
