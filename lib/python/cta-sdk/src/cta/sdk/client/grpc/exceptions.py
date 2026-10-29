# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Exceptions raised by the CTA workflow gRPC client."""


class WorkflowError(Exception):
    """Base class for workflow gRPC client errors."""


class WorkflowRpcError(WorkflowError):
    """Raised when a workflow gRPC call itself fails."""


class WorkflowConfigError(WorkflowError):
    """Raised when the workflow configuration cannot satisfy a request."""
