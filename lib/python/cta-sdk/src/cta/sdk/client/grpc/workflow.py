# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import typing as t

from cta.sdk.client.grpc.disk_buffer_configs import _get_disk_buffer_mode_url
from cta.sdk.client.grpc.exceptions import WorkflowConfigError
from cta.sdk.client.grpc.models import WorkflowFileInfo
from cta.sdk.client.grpc.protobuf_api import (
    ProtobufRpcClient,
    _call_rpc_with_error_handling,
    build_channel,
    build_metadata,
    file_info_to_pb_options,
)
from cta.sdk.client.grpc.protobuf_generated import cta_common_pb2, cta_eos_pb2, cta_frontend_pb2, cta_frontend_pb2_grpc


class WorkflowRpcClient(ProtobufRpcClient):
    """Client for sending workflow events to the CTA gRPC frontend service."""

    def _send_wf_event(
        self,
        event_type: cta_eos_pb2.Workflow.EventType,
        user_group: t.Tuple[str, str],
        instance: cta_common_pb2.Service,
        transport: cta_eos_pb2.Transport,
        file_metadata: cta_eos_pb2.Metadata,
        *,
        workflow_options: t.Optional[dict] = None,
    ):
        """Helper method to send a workflow event to the CTA gRPC frontend service.
        Selects the appropriate RPC method based on the event type and handles the request/response."""

        if not workflow_options:
            workflow_options = {}

        request = cta_frontend_pb2.Request(
            notification=cta_eos_pb2.Notification(
                wf=cta_eos_pb2.Workflow(
                    instance=instance, requester_instance="cta-send-event", event=event_type, **workflow_options
                ),
                cli=cta_eos_pb2.Client(
                    user=cta_common_pb2.RequesterId(username=user_group[0], groupname=user_group[1]),
                ),
                transport=transport,
                file=file_metadata,
            )
        )

        # store the outgoing request for inspection by the caller
        self.last_request = request

        with build_channel(self.workflow_cfg) as channel:
            stub = cta_frontend_pb2_grpc.CtaRpcStub(channel)
            response = _call_rpc_with_error_handling(
                {
                    cta_eos_pb2.Workflow.CREATE: stub.Create,
                    cta_eos_pb2.Workflow.CLOSEW: stub.Archive,
                    cta_eos_pb2.Workflow.PREPARE: stub.Retrieve,
                    cta_eos_pb2.Workflow.ABORT_PREPARE: stub.CancelRetrieve,
                    cta_eos_pb2.Workflow.DELETE: stub.Delete,
                }[event_type],
                request=request,
                metadata=build_metadata(self.workflow_cfg),
                timeout=self.workflow_cfg.timeout,
            )

        self.last_response = response
        response_type = cta_frontend_pb2.Response.ResponseType.Name(response.type)
        return (response_type, response.message_txt)

    def send_create_event(
        self,
        file_info: "WorkflowFileInfo",
        storage_class: str,
        user_group: t.Tuple[str, str],
    ) -> tuple[str, str]:
        transport = cta_eos_pb2.Transport()
        instance = cta_common_pb2.Service(
            name=self.workflow_cfg.disk_instance,
        )

        file_metadata = cta_eos_pb2.Metadata(storage_class=storage_class, **file_info_to_pb_options(file_info))

        return self._send_wf_event(cta_eos_pb2.Workflow.CREATE, user_group, instance, transport, file_metadata)

    def send_archive_event(
        self,
        file_info: "WorkflowFileInfo",
        storage_class: str,
        user_group: t.Tuple[str, str],
        *,
        service_url: t.Optional[str] = None,
        report_url: t.Optional[str] = None,
        error_report_url: t.Optional[str] = None,
    ) -> tuple[str, str]:
        """Invoke the EOS event on the WFE frontend."""

        report_url = report_url or _get_disk_buffer_mode_url("archive_report_url", self.workflow_cfg, file_info)
        error_report_url = error_report_url or _get_disk_buffer_mode_url(
            "archive_error_report_url", self.workflow_cfg, file_info
        )

        transport = cta_eos_pb2.Transport(
            report_url=report_url,
            error_report_url=error_report_url,
        )

        instance = cta_common_pb2.Service(
            name=self.workflow_cfg.disk_instance,
            url=service_url or _get_disk_buffer_mode_url("service_url", self.workflow_cfg, file_info),
        )

        file_metadata = cta_eos_pb2.Metadata(
            storage_class=storage_class,
            **file_info_to_pb_options(file_info),
        )

        return self._send_wf_event(cta_eos_pb2.Workflow.CLOSEW, user_group, instance, transport, file_metadata)

    def send_prepare_event(
        self,
        file_info: "WorkflowFileInfo",
        vid: t.Optional[str],
        verify_only: bool,
        user_group: t.Tuple[str, str],
        *,
        service_url: t.Optional[str] = None,
        dst_url: t.Optional[str] = None,
        error_report_url: t.Optional[str] = None,
    ) -> tuple[str, str]:
        """Invoke the EOS event on the WFE frontend."""

        workflow_options = {}

        dst_url = dst_url or _get_disk_buffer_mode_url("prepare_dest_url", self.workflow_cfg, file_info)

        if not dst_url:
            raise WorkflowConfigError(
                "Destination URL for prepare event is not provided and cannot be determined from disk buffer mode."
            )

        error_report_url = error_report_url or _get_disk_buffer_mode_url(
            "prepare_error_report_url", self.workflow_cfg, file_info
        )
        transport = cta_eos_pb2.Transport(
            dst_url=dst_url,
            error_report_url=error_report_url,
        )

        instance = cta_common_pb2.Service(
            name=self.workflow_cfg.disk_instance,
            url=service_url or _get_disk_buffer_mode_url("service_url", self.workflow_cfg, file_info),
        )

        if verify_only:
            workflow_options["verify_only"] = True

        if vid:
            workflow_options["vid"] = vid

        file_metadata = cta_eos_pb2.Metadata(**file_info_to_pb_options(file_info))

        return self._send_wf_event(
            cta_eos_pb2.Workflow.PREPARE,
            user_group,
            instance,
            transport,
            file_metadata,
            workflow_options=workflow_options,
        )

    def send_abort_prepare_event(
        self,
        file_info: "WorkflowFileInfo",
        object_store_id: str,
        user_group: t.Tuple[str, str],
    ) -> tuple[str, str]:
        transport = cta_eos_pb2.Transport()
        instance = cta_common_pb2.Service(name=self.workflow_cfg.disk_instance)
        file_metadata = cta_eos_pb2.Metadata(
            request_objectstore_id=object_store_id, **file_info_to_pb_options(file_info)
        )
        return self._send_wf_event(cta_eos_pb2.Workflow.ABORT_PREPARE, user_group, instance, transport, file_metadata)

    def send_delete_event(
        self,
        file_info: "WorkflowFileInfo",
        user_group: t.Tuple[str, str],
    ) -> tuple[str, str]:
        transport = cta_eos_pb2.Transport()
        instance = cta_common_pb2.Service(name=self.workflow_cfg.disk_instance)
        file_metadata = cta_eos_pb2.Metadata(**file_info_to_pb_options(file_info))
        return self._send_wf_event(cta_eos_pb2.Workflow.DELETE, user_group, instance, transport, file_metadata)
