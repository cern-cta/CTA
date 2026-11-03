# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""gRPC client for the CTA workflow frontend service.

Contains the protobuf generation mechanism and the WorkflowRpcClient used to send workflow events.
Requires the ``grpc`` extra option of cta-sdk: ``pip install 'cta-sdk[grpc]'``.
"""
