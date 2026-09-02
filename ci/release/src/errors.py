# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Shared release workflow errors."""


class ReleaseWorkflowError(RuntimeError):
    """A failure while coordinating an otherwise valid release workflow."""
