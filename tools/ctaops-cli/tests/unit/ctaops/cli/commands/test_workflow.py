# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
from click.testing import CliRunner
from cta.sdk.client.grpc import workflow as workflow_api
from cta.sdk.client.grpc.exceptions import WorkflowConfigError, WorkflowRpcError
from cta.sdk.client.grpc.models import WorkflowConfig
from cta.sdk.client.grpc.protobuf_api import file_info_to_pb_options
from cta.sdk.client.grpc.protobuf_generated import cta_eos_pb2, cta_frontend_pb2
from cta.sdk.client.grpc.workflow import WorkflowRpcClient

from ctaops.cli.commands.workflow import WorkflowFileInfo, WorkflowServerError
from ctaops.cli.config import AppConfig
from ctaops.cli.context import AppContext
from ctaops.cli.main import run as cli_run


class _FakeResponse:
    type = cta_frontend_pb2.Response.RSP_SUCCESS
    message_txt = "OK"


@pytest.fixture
def mock_send_wf_event(monkeypatch):
    """Fixture to mock WorkflowRpcClient._send_wf_event and capture call arguments."""
    captured = {}

    def fake_send_wf_event(self, event_type, user_group, instance, transport, file_metadata, workflow_options=None):
        captured["event_type"] = event_type
        captured["transport"] = transport
        captured["instance"] = instance
        captured["file_metadata"] = file_metadata
        captured["user_group"] = user_group
        captured["workflow_options"] = workflow_options
        captured["metadata"] = file_metadata
        return ("RSP_SUCCESS", "OK")

    monkeypatch.setattr(WorkflowRpcClient, "_send_wf_event", fake_send_wf_event)
    return captured


def test_info_pb_opts():
    """The metadata dictionary should expand to owner and checksum-aware protobuf fields."""
    file_info = WorkflowFileInfo(
        archive_file_id="123",
        id=42,
        path="/dummy/path",
        uid="1000",
        gid="1001",
        size=4096,
        checksum="00000000",
        xattr={"key": "value"},
    )

    options = file_info_to_pb_options(file_info)

    assert "archive_file_id" in options
    assert "size" in options
    assert "csb" in options
    assert "lpath" in options
    assert "xattr" in options
    assert "disk_file_id" in options
    assert "owner" in options

    metadata = cta_eos_pb2.Metadata(storage_class="SC", **options)
    assert metadata.storage_class == "SC"
    assert metadata.archive_file_id == 123
    assert metadata.size == 4096
    assert metadata.lpath == "/dummy/path"
    assert metadata.disk_file_id == "42"
    assert metadata.owner.uid == 1000
    assert metadata.owner.gid == 1001


def test_archive_explicit(mock_send_wf_event):
    """Archive RPC payloads should carry the explicit transport URLs through to the protobuf request."""
    cfg = WorkflowConfig(disk_buffer_mode="eos-test", disk_endpoint="eos.example", frontend_endpoint="frontend:10956")
    client = WorkflowRpcClient(workflow_cfg=cfg)
    captured = mock_send_wf_event

    file_info = WorkflowFileInfo(archive_file_id="777", id=99, path="/to/file", size=42, uid="1001", gid="1002")
    outcome = client.send_archive_event(
        file_info,
        "SC",
        ("someuser", "somegroup"),
        service_url="http://disk-service",
        report_url="root://archive-success",
        error_report_url="root://archive-error",
    )

    assert outcome == ("RSP_SUCCESS", "OK")
    assert captured["event_type"] == cta_eos_pb2.Workflow.CLOSEW
    assert captured["instance"].name == "ctaeos"
    assert captured["instance"].url == "http://disk-service"
    assert captured["transport"].report_url == "root://archive-success"
    assert captured["transport"].error_report_url == "root://archive-error"
    assert captured["file_metadata"].storage_class == "SC"
    assert captured["file_metadata"].archive_file_id == 777


def test_archive_lookup(mock_send_wf_event, monkeypatch):
    """Archive transport URLs should fallback to the default when explicit values are not supplied."""
    cfg = WorkflowConfig(disk_buffer_mode="eos-test", disk_endpoint="eos.example", frontend_endpoint="frontend:10956")
    client = WorkflowRpcClient(workflow_cfg=cfg)
    captured = mock_send_wf_event

    def fake_disk_buffer_config_lookup(key, workflow_cfg, file_info):
        return {
            "service_url": "http://disk-service",
            "archive_report_url": "root://archive-success",
            "archive_error_report_url": "root://archive-error",
        }[key]

    monkeypatch.setattr(workflow_api, "_get_disk_buffer_mode_url", fake_disk_buffer_config_lookup)

    file_info = WorkflowFileInfo(archive_file_id="777", id=99, path="/to/file", size=42, uid="1001", gid="1002")
    outcome = client.send_archive_event(
        file_info,
        "SC",
        ("someuser", "somegroup"),
    )

    assert outcome == ("RSP_SUCCESS", "OK")
    assert captured["instance"].url == "http://disk-service"
    assert captured["transport"].report_url == "root://archive-success"
    assert captured["transport"].error_report_url == "root://archive-error"


def test_prepare_missing_dst(monkeypatch):
    """Prepare should raise when it cannot derive a destination URL for the transport."""
    cfg = WorkflowConfig(disk_buffer_mode=None, disk_endpoint="eos.example", frontend_endpoint="frontend:10956")
    client = WorkflowRpcClient(workflow_cfg=cfg)

    def fake_disk_buffer_config_lookup(_key, _workflow_cfg, _file_info):
        return None

    monkeypatch.setattr(workflow_api, "_get_disk_buffer_mode_url", fake_disk_buffer_config_lookup)

    file_info = WorkflowFileInfo(archive_file_id="555", id=11, path="/tmp/file")

    with pytest.raises(WorkflowConfigError, match="Destination URL for prepare event"):
        client.send_prepare_event(
            file_info,
            None,
            False,
            ("someuser", "group"),
            service_url="http://svc",
        )


def test_prepare_flags(mock_send_wf_event, monkeypatch):
    """Prepare-only options should be forwarded into the workflow options dict."""
    cfg = WorkflowConfig(disk_buffer_mode=None, disk_endpoint="eos.example", frontend_endpoint="frontend:10956")
    client = WorkflowRpcClient(workflow_cfg=cfg)
    captured = mock_send_wf_event

    def fake_lookup(key, workflow_cfg, file_info):
        return {
            "service_url": "http://svc",
            "prepare_dest_url": "root://prepare-dest",
            "prepare_error_report_url": "root://prepare-error",
        }[key]

    monkeypatch.setattr(workflow_api, "_get_disk_buffer_mode_url", fake_lookup)

    file_info = WorkflowFileInfo(archive_file_id="555", id=11, path="/tmp/file")
    outcome = client.send_prepare_event(
        file_info,
        "volume-001",
        True,
        ("someuser", "somegroup"),
        service_url="http://svc",
    )

    assert outcome == ("RSP_SUCCESS", "OK")
    assert captured["event_type"] == cta_eos_pb2.Workflow.PREPARE
    assert captured["workflow_options"] == {"verify_only": True, "vid": "volume-001"}
    assert captured["transport"].dst_url == "root://prepare-dest"
    assert captured["transport"].error_report_url == "root://prepare-error"


def test_abort_contract(mock_send_wf_event):
    """Abort requests should keep the object-store id inside the metadata payload."""
    cfg = WorkflowConfig(disk_buffer_mode=None, disk_endpoint="eos.example", frontend_endpoint="frontend:10956")
    client = WorkflowRpcClient(workflow_cfg=cfg)
    captured = mock_send_wf_event

    file_info = WorkflowFileInfo(archive_file_id="901")
    client.send_abort_prepare_event(file_info, "store-abc", ("u", "g"))

    assert captured["event_type"] == cta_eos_pb2.Workflow.ABORT_PREPARE
    assert captured["metadata"].request_objectstore_id == "store-abc"
    assert captured["metadata"].archive_file_id == 901


def test_disk_buffer_mode_eos_test(mock_send_wf_event, monkeypatch):
    """Disk buffer mode 'eos-test' should resolve URLs correctly."""
    cfg = WorkflowConfig(
        disk_buffer_mode="eos-test",
        disk_endpoint="eos.example.com",
        frontend_endpoint="frontend:10956",
    )
    client = WorkflowRpcClient(workflow_cfg=cfg)
    captured = mock_send_wf_event

    file_info = WorkflowFileInfo(archive_file_id="555", id=42, path="/data/file.txt")
    outcome = client.send_archive_event(
        file_info,
        "SC",
        ("someuser", "group"),
    )

    assert outcome == ("RSP_SUCCESS", "OK")
    # Verify that the transport URLs were resolved from the eos-test mode
    assert captured["transport"].report_url.startswith("eosQuery://eos.example.com")
    assert "mgm.event=sync::archived" in captured["transport"].report_url
    assert "cta_archive_file_id=555" in captured["transport"].report_url
    assert captured["transport"].error_report_url.startswith("eosQuery://eos.example.com")
    assert "mgm.event=sync::archive_failed" in captured["transport"].error_report_url


def test_timeout_configuration(monkeypatch):
    """Timeout should be correctly passed to the gRPC with_call method."""
    custom_timeout = 42
    cfg = WorkflowConfig(
        disk_buffer_mode=None,
        disk_endpoint="eos.example",
        frontend_endpoint="frontend:10956",
        timeout=custom_timeout,
    )
    client = WorkflowRpcClient(workflow_cfg=cfg)

    captured_call_args = {}

    # Mock the gRPC stub with all RPC methods
    class FakeRpcMethod:
        def with_call(self, request, metadata=None, timeout=None):
            captured_call_args["timeout"] = timeout
            return (_FakeResponse(), None)

    class FakeStub:
        Create = FakeRpcMethod()
        Archive = FakeRpcMethod()
        Retrieve = FakeRpcMethod()
        CancelRetrieve = FakeRpcMethod()
        Delete = FakeRpcMethod()

    def fake_build_channel(workflow_cfg):
        class FakeChannel:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        return FakeChannel()

    def fake_grpc_stub(channel):
        return FakeStub()

    monkeypatch.setattr("cta.sdk.client.grpc.protobuf_api.build_channel", fake_build_channel)
    monkeypatch.setattr(
        "cta.sdk.client.grpc.workflow.cta_frontend_pb2_grpc.CtaRpcStub",
        fake_grpc_stub,
    )

    file_info = WorkflowFileInfo(archive_file_id="123", id=1, path="/test")
    client.send_delete_event(file_info, ("u", "g"))

    assert captured_call_args["timeout"] == custom_timeout


def test_create_suppresses_submission_message_raw(monkeypatch, capsys):
    """Verify create does not print submission messages when format is raw."""

    class FakeClient(WorkflowRpcClient):
        def __init__(self):
            self.last_response = type(
                "Response", (), {"type": cta_frontend_pb2.Response.RSP_SUCCESS, "message_txt": "OK"}
            )  # type: ignore

        def send_create_event(self, *args, **kwargs):
            self.last_response = type(
                "Response", (), {"type": cta_frontend_pb2.Response.RSP_SUCCESS, "message_txt": "OK"}
            )  # type: ignore
            return ("RSP_SUCCESS", "OK")

    obj = AppContext(
        config=AppConfig(),
        cta_client=object(),  # type: ignore
        workflow_client=FakeClient(),
        workflow_cmd_opts={"user": "user", "group": "group"},
    )
    obj.config.display.format = "raw"

    from ctaops.cli.commands.workflow import create

    runner = CliRunner()
    result = runner.invoke(
        create,
        [
            "--storage-class",
            "SC",
            "--archive-file-id",
            "123",
            "--id",
            "1",
            "--path",
            "/test",
            "--uid",
            "1000",
            "--gid",
            "1001",
        ],
        obj=obj,
    )

    assert result.exit_code == 0
    assert "WFE event CREATE submitted" not in result.output


def test_create_suppresses_submission_message_json(monkeypatch, capsys):
    """Verify create does not print submission messages when format is json."""

    class FakeClient(WorkflowRpcClient):
        def __init__(self):
            self.last_response = type(
                "Response", (), {"type": cta_frontend_pb2.Response.RSP_SUCCESS, "message_txt": "OK"}
            )  # type: ignore

        def send_create_event(self, *args, **kwargs):
            self.last_response = type(
                "Response", (), {"type": cta_frontend_pb2.Response.RSP_SUCCESS, "message_txt": "OK"}
            )  # type: ignore
            return ("RSP_SUCCESS", "OK")

    obj = AppContext(
        config=AppConfig(),
        cta_client=object(),  # type: ignore
        workflow_client=FakeClient(),
        workflow_cmd_opts={"user": "user", "group": "group"},
    )
    obj.config.display.format = "json"

    from ctaops.cli.commands.workflow import create

    runner = CliRunner()
    result = runner.invoke(
        create,
        [
            "--storage-class",
            "SC",
            "--archive-file-id",
            "123",
            "--id",
            "1",
            "--path",
            "/test",
            "--uid",
            "1000",
            "--gid",
            "1001",
        ],
        obj=obj,
    )

    assert result.exit_code == 0
    assert "WFE event CREATE submitted" not in result.output


def test_exit_code_server_error(monkeypatch):
    """Verify workflow server errors map to exit code 1."""
    import ctaops.cli.main as main_mod

    def fake_main(args=None, standalone_mode=False):
        raise WorkflowServerError("server rejected request")

    monkeypatch.setattr(main_mod.ctaops, "main", fake_main)

    with pytest.raises(SystemExit) as excinfo:
        cli_run(["--help"])

    assert excinfo.value.code == 1


def test_exit_code_rpc_failure(monkeypatch):
    """Verify workflow RPC failures map to exit code 2."""
    import ctaops.cli.main as main_mod

    def fake_main(args=None, standalone_mode=False):
        raise WorkflowRpcError("rpc failure")

    monkeypatch.setattr(main_mod.ctaops, "main", fake_main)

    with pytest.raises(SystemExit) as excinfo:
        cli_run(["--help"])

    assert excinfo.value.code == 2
