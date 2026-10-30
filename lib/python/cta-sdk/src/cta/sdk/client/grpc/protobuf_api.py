# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Thin adapter around generated CTA protobuf and gRPC modules."""

from __future__ import annotations

import logging
import typing as t
from dataclasses import dataclass
from pathlib import Path

import grpc
from cta.sdk.client.grpc.exceptions import WorkflowRpcError
from cta.sdk.client.grpc.models import WorkflowConfig, WorkflowFileInfo
from cta.sdk.client.grpc.protobuf_generated import cta_common_pb2, cta_frontend_pb2

LOGGER = logging.getLogger("ctaops.cli.protobuf_api")


def build_channel(workflow_cfg: WorkflowConfig) -> grpc.Channel:
    """Build a gRPC channel to the workflow frontend service with optional TLS and metadata."""
    options = []
    if workflow_cfg.server_name_override:
        options.append(("grpc.ssl_target_name_override", workflow_cfg.server_name_override))

    if workflow_cfg.verify_cert:
        credentials = grpc.ssl_channel_credentials()
    else:
        if not workflow_cfg.ca_cert_file:
            raise RuntimeError(
                "workflow.verify_cert=false requires workflow.ca_cert_file for TLS trust. "
                "gRPC Python does not support encrypted channels with fully disabled certificate verification."
            )

        ca_cert_bytes = Path(workflow_cfg.ca_cert_file).read_bytes()
        credentials = grpc.ssl_channel_credentials(root_certificates=ca_cert_bytes)

    LOGGER.debug(
        "Creating gRPC channel to %s [TLS verification=%s, CA cert file=%s, server name override=%s]",
        workflow_cfg.frontend_endpoint,
        workflow_cfg.verify_cert,
        workflow_cfg.ca_cert_file,
        workflow_cfg.server_name_override,
    )

    assert workflow_cfg.frontend_endpoint

    return grpc.secure_channel(workflow_cfg.frontend_endpoint, credentials, options=options)


def build_metadata(workflow_cfg: WorkflowConfig) -> list[tuple[str, str]]:
    """Build gRPC metadata for authentication, including the JWT token."""
    token = workflow_cfg.jwt_token_file and Path(workflow_cfg.jwt_token_file).read_text().strip()
    metadata = [("authorization", f"Bearer {token}")] if token else []
    LOGGER.debug("Setting metadata for gRPC call: %s", metadata)
    return metadata


def call_rpc_with_error_handling(
    rpc_call: grpc.UnaryUnaryMultiCallable[cta_frontend_pb2.Request, cta_frontend_pb2.Response],
    request: cta_frontend_pb2.Request,
    metadata: list[tuple[str, str]],
    timeout: int,
) -> cta_frontend_pb2.Response:
    """Call a gRPC RPC method with error handling and logging."""
    LOGGER.debug("Sending gRPC request: %s", request)

    try:
        response, _ = rpc_call.with_call(request, metadata=tuple(metadata), timeout=timeout)
    except grpc.RpcError as err:
        LOGGER.exception("gRPC call failed: %s - %s", err.code(), err.details())
        raise WorkflowRpcError(f"gRPC call failed: {err.code()} - {err.details()}") from err

    LOGGER.debug(
        "Received gRPC response: type=%s, message=%s, content=%s",
        cta_frontend_pb2.Response.ResponseType.Name(response.type),
        response.message_txt,
        response,
    )

    return response


def file_info_to_pb_options(file_info: WorkflowFileInfo) -> dict[str, t.Any]:
    """Convert WorkflowFileInfo to a dict of protobuf options."""
    options = {}

    if file_info.archive_file_id:
        options["archive_file_id"] = int(file_info.archive_file_id)

    if file_info.size:
        options["size"] = int(file_info.size)

    if file_info.checksum:
        options["csb"] = cta_common_pb2.ChecksumBlob(
            cs=[
                cta_common_pb2.ChecksumBlob.Checksum(
                    type=cta_common_pb2.ChecksumBlob.Checksum.ADLER32,
                    value=bytes(reversed(bytes.fromhex(file_info.checksum))),
                )
            ]
        )

    if file_info.path:
        options["lpath"] = file_info.path

    if file_info.xattr:
        options["xattr"] = list(file_info.xattr.items())

    if file_info.id:
        options["disk_file_id"] = str(file_info.id)

    if file_info.uid is not None or file_info.gid is not None:
        params = {}
        if file_info.uid is not None:
            params["uid"] = int(file_info.uid)
        if file_info.gid is not None:
            params["gid"] = int(file_info.gid)

        options["owner"] = cta_common_pb2.OwnerId(**params)

    return options


@dataclass
class ProtobufRpcClient:
    """Client helper for CTA gRPC frontend services."""

    workflow_cfg: WorkflowConfig
    last_response: cta_frontend_pb2.Response | None = None
