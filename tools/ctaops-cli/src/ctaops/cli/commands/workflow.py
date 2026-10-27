# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import functools
import json
import typing as t
from contextlib import contextmanager
from dataclasses import dataclass, fields
from pathlib import Path

import click
from cta.sdk.client.grpc.models import WorkflowFileInfo as WorkflowFileInfoBase
from cta.sdk.client.grpc.workflow import WorkflowRpcClient
from ctaops.utils.dataclasses import load_data
from ctaops.utils.typing import unwrap_optional_type
from rich import print as print_rich
from rich.panel import Panel
from rich.pretty import Pretty

from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext


def proto_to_dict(message: t.Any) -> dict:
    """Recursively convert a protobuf message to a dictionary."""
    if not hasattr(message, "DESCRIPTOR"):
        return message

    result = {}
    for field in message.DESCRIPTOR.fields:
        key = field.name
        value = getattr(message, key)

        if field.type == field.TYPE_MESSAGE:
            if field.label == field.LABEL_REPEATED:
                result[key] = [proto_to_dict(item) for item in value]
            else:
                result[key] = proto_to_dict(value)
        elif field.label == field.LABEL_REPEATED:
            result[key] = list(value)
        else:
            result[key] = str(value)
    return result


def print_response(
    format: t.Literal["json", "raw", "table", "long-table"],
    response: t.Any,
    is_error: bool = False,
):
    """Print the response in the specified format."""
    if format == "json":
        print(json.dumps(proto_to_dict(response), indent=2, default=str))
    elif format == "raw":
        print(response)
    else:
        pretty_response = Pretty(response, expand_all=True, indent_guides=False, max_string=128)
        color = "red" if is_error else "green"
        panel = Panel(pretty_response, title=f"[{color}]RSP_SUCCESS[/{color}]", title_align="left")
        print_rich(panel)


class WorkflowServerError(click.ClickException):
    """Raised when a workflow gRPC request returns an error response."""

    exit_code = 1


@dataclass
class WorkflowFileInfo(WorkflowFileInfoBase):
    """Subclass of the SDK WorkflowFileInfo dataclass to add CLI-specific functionality."""

    @property
    def color_repr(self) -> str:
        if self.path:
            return f"[cyan]{self.path}[/cyan] ([magenta]{self.archive_file_id}[/magenta])"
        elif self.id:
            return f"[cyan]0x{self.fxid}[/cyan] ([magenta]{self.archive_file_id}[/magenta])"
        elif self.archive_file_id:
            return f"file ([magenta]{self.archive_file_id}[/magenta])"
        else:
            return "file"

    @classmethod
    def from_file(cls, f: t.IO[str]) -> "WorkflowFileInfo":
        import json

        data = json.load(f)
        fi = cls(archive_file_id=data["xattr"].get("sys.archive.file_id"))
        fi.checksum = data.pop("checksumvalue", None) if data.pop("checksumtype", None) == "adler" else None

        return load_data(fi, data)


REQUIRED_FIELDS = {field.name for field in fields(WorkflowFileInfo) if unwrap_optional_type(field.type) is None}


def file_info_options(required: t.Union[set[str], frozenset[str]] = frozenset()):
    def _file_info_options(f):
        """Adds --file-info plus the individual options, and passes a single
        `file_info: WorkflowFileInfo` argument to the wrapped command instead."""

        options = [
            click.option(
                "--file-info", "file_info_path", type=click.File("r"), help="Path to a file describing the target."
            )
        ]

        # iterate over all members of WorkflowFileInfo and add the corresponding option
        type_hints = t.get_type_hints(WorkflowFileInfo)

        for field in fields(WorkflowFileInfo):
            annotation = type_hints[field.name]
            inner_t = unwrap_optional_type(annotation)
            field_t = inner_t if inner_t is not None else annotation

            options.append(click.option(f"--{field.name.replace('_', '-')}", f"opt_{field.name}", type=field_t))

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            file_info_path = kwargs.pop("file_info_path")

            individual = {field.name: kwargs.pop(f"opt_{field.name}") for field in fields(WorkflowFileInfo)}
            required_fields = REQUIRED_FIELDS | required

            has_some_individual = any(v is not None for v in individual.values())
            has_all_required = all(v is not None for k, v in individual.items() if k in required_fields)

            if file_info_path and has_some_individual:
                raise click.UsageError("--file-info cannot be combined with --id/--path/--uid/--gid/--archive-file-id")
            if file_info_path:
                file_info = WorkflowFileInfo.from_file(file_info_path)
            elif has_all_required:
                archive_file_id = individual.pop("archive_file_id")
                file_info = WorkflowFileInfo(archive_file_id, **individual)
            else:
                required_fields = ", ".join(f"--{f.replace('_', '-')}" for f in required_fields)
                raise click.UsageError(f"Provide either --file-info or all of {required_fields}")

            kwargs["file_info"] = file_info
            return f(*args, **kwargs)

        for opt in reversed(options):
            wrapper = opt(wrapper)
        return wrapper

    return _file_info_options


@click.group(cls=ClickAliasedGroup, aliases=["wf"])
@click.option("--user", "-u", required=True, help="User name to identify the request.")
@click.option("--group", "-g", required=True, help="Group name to identify the request.")
@click.option(
    "--disk-buffer-mode",
    type=click.Choice(["eos-test"]),
    help="Choose this to use a predefined disk buffer mode for workflow operations.",
)
@click.option(
    "--jwt-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="JWT token for authentication with the workflow service.",
)
@click.option("--frontend-endpoint", help="gRPC server endpoint for CTA WFE services.")
@click.option("--disk-endpoint", help="Disk buffer endpoint for workflow operations.")
@click.option("--disk-instance", help="Disk instance identifier.")
@click.option("--timeout", type=int, help="Timeout in seconds for workflow gRPC calls.")
@click.option("--verify-cert/--no-verify-cert", default=None, help="Whether to verify the server's TLS certificate.")
@click.option(
    "--ca-cert-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to a custom CA/server cert PEM file for TLS trust.",
)
@click.option("--server-name-override", help="Optional TLS server name override for hostname mismatch testing.")
@click.option("--show-request", is_flag=True, help="Show outgoing workflow request (for debugging).")
@click.pass_obj
def workflow(
    obj: AppContext,
    user: str,
    group: str,
    disk_buffer_mode: t.Optional[t.Literal["eos-test"]],
    jwt_file: t.Optional[Path],
    frontend_endpoint: t.Optional[str],
    disk_endpoint: t.Optional[str],
    disk_instance: t.Optional[str],
    timeout: t.Optional[int],
    verify_cert: t.Optional[bool],
    ca_cert_file: t.Optional[Path],
    server_name_override: t.Optional[str],
    show_request: bool,
):
    """Manage CTA workflow RPC operations."""

    if obj.workflow_client is None:
        raise click.ClickException("CTA Workflow integration is not configured. Check [workflow] in ctaops-cli.toml.")

    cfg = obj.workflow_client.workflow_cfg
    for attr, value in {
        "disk_buffer_mode": disk_buffer_mode,
        "jwt_token_file": jwt_file,
        "frontend_endpoint": frontend_endpoint,
        "disk_endpoint": disk_endpoint,
        "disk_instance": disk_instance,
        "timeout": timeout,
        "verify_cert": verify_cert,
        "ca_cert_file": ca_cert_file,
        "server_name_override": server_name_override,
    }.items():
        if value is not None:
            setattr(cfg, attr, value)

    obj.workflow_cmd_opts = {"user": user, "group": group}
    if show_request:
        obj.workflow_cmd_opts["show_request"] = True


@contextmanager
def _workflow_client(obj: AppContext) -> t.Generator[t.Tuple[WorkflowRpcClient, dict], None, None]:
    """
    Context manager to handle the workflow client and its options. Does error handling.
    """
    from cta.sdk.client.grpc.protobuf_generated import cta_frontend_pb2

    if obj.workflow_client is None:
        raise click.ClickException("CTA Workflow integration is not configured. Check [workflow] in ctaops-cli.toml.")

    assert obj.workflow_cmd_opts

    try:
        yield obj.workflow_client, obj.workflow_cmd_opts

    finally:
        # Optionally display the outgoing request before printing the response
        last_request = getattr(obj.workflow_client, "last_request", None)
        if obj.workflow_cmd_opts.get("show_request") and last_request:
            if obj.config.display.format == "json":
                print(json.dumps(proto_to_dict(last_request), indent=2, default=str))
            elif obj.config.display.format == "raw":
                print(last_request)
            else:
                pretty_request = Pretty(last_request, expand_all=True, indent_guides=False, max_string=128)
                panel_req = Panel(pretty_request, title="Request", title_align="left")
                print_rich(panel_req)

        last_response = obj.workflow_client.last_response
        if last_response:
            if last_response.type == cta_frontend_pb2.Response.RSP_SUCCESS:
                print_response(obj.config.display.format, last_response)
            else:
                print_response(obj.config.display.format, last_response, is_error=True)
                raise WorkflowServerError(f"Event submission failed: {last_response}")


@workflow.command(aliases=["crt"])
@click.option("--storage-class", required=True, help="Storage class identifier.")
@file_info_options(required={"uid"})
@click.pass_obj
def create(
    obj: AppContext,
    file_info: WorkflowFileInfo,
    storage_class: str,
):
    if not storage_class:
        raise click.UsageError("Storage class must be provided with --storage-class")

    with _workflow_client(obj) as (client, options):
        client.send_create_event(
            file_info,
            storage_class,
            (options["user"], options["group"]),
        )

    if obj.config.display.format not in ("raw", "json"):
        print_rich(f"WFE event CREATE submitted for {file_info.color_repr}")


@workflow.command(aliases=["arch"])
@click.option("--storage-class", required=True, help="Storage class identifier.")
@click.option("--service-url", help="Optional service URL for the archive operation.")
@file_info_options(required={"archive_file_id", "id", "path", "size"})
@click.pass_obj
def archive(
    obj: AppContext,
    file_info: WorkflowFileInfo,
    storage_class: str,
    service_url: t.Optional[str],
):
    if not storage_class:
        raise click.UsageError("Storage class must be provided with --storage-class")

    with _workflow_client(obj) as (client, options):
        client.send_archive_event(
            file_info,
            storage_class,
            (options["user"], options["group"]),
            service_url=service_url,
        )

    if obj.config.display.format not in ("raw", "json"):
        print_rich(f"WFE event [blue]ARCHIVE[/blue] submitted for file {file_info.color_repr}")


@workflow.command(aliases=["prep"])
@click.option("--verify-only", is_flag=True, help="Prepare only a verify (do not actually stage)")
@click.option("--vid", help="Stage from a particular volume (provide ID)")
@click.option("--service-url", help="Optional service URL for the prepare operation.")
@file_info_options(required={"archive_file_id", "id", "path"})
@click.pass_obj
def prepare(
    obj: AppContext, file_info: WorkflowFileInfo, service_url: t.Optional[str], verify_only: bool, vid: t.Optional[str]
):
    with _workflow_client(obj) as (client, options):
        client.send_prepare_event(
            file_info,
            vid,
            verify_only,
            (options["user"], options["group"]),
            service_url=service_url,
        )

    if obj.config.display.format not in ("raw", "json"):
        print_rich(f"WFE event [green]PREPARE[/green] submitted for file {file_info.color_repr}")


@workflow.command(aliases=["abort"])
@click.option("--object-store-id", required=True, help="Object store ID to abort.")
@file_info_options(required={"archive_file_id"})
@click.pass_obj
def abort_prepare(obj: AppContext, file_info: WorkflowFileInfo, object_store_id: str):
    with _workflow_client(obj) as (client, options):
        client.send_abort_prepare_event(
            file_info,
            object_store_id,
            (options["user"], options["group"]),
        )

    if obj.config.display.format not in ("raw", "json"):
        print_rich(f"WFE event [yellow]ABORT_PREPARE[/yellow] submitted for file {file_info.color_repr}")


@workflow.command(aliases=["del"])
@file_info_options(required={"archive_file_id", "id", "path"})
@click.pass_obj
def delete(
    obj: AppContext,
    file_info: WorkflowFileInfo,
):
    with _workflow_client(obj) as (client, options):
        client.send_delete_event(
            file_info,
            (options["user"], options["group"]),
        )

    if obj.config.display.format not in ("raw", "json"):
        print_rich(f"WFE event [red]DELETE[/red] submitted for file {file_info.color_repr}")
