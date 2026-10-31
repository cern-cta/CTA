# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Main Cli entry point for CTA Operations.

This module defines the root Click command group and handles the initialization
of the Cli environment, including configuration loading, logging setup, and
CTA client initialization. It also manages dynamic loading of command modules.
"""

import importlib
import pkgutil
import sys
import typing as t
from dataclasses import asdict
from pathlib import Path
from typing import Optional

import click
from cta.sdk import get_cta_client
from cta.sdk.client.grpc.exceptions import WorkflowConfigError, WorkflowRpcError
from cta.sdk.exceptions import CtaSdkError
from ctaops.cli.display.printers import print_dataclasses
from ctaops.utils.config import CtaOpsConfigLoader
from ctaops.utils.logging import configure_logging
from rich.console import Console
from rich.panel import Panel

from .click_extension.aliases import ClickAliasedGroup
from .config import AppConfig
from .context import AppContext, __version__


@click.group("ctaops", context_settings={"help_option_names": ["-h", "--help"]}, cls=ClickAliasedGroup)
@click.version_option(__version__, prog_name="ctaops")
@click.option(
    "--format",
    "-f",
    type=click.Choice(["json", "raw", "table"]),
    help="Override output format from the config file.",
)
@click.option(
    "--log-level",
    type=click.Choice(["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]),
)
@click.option(
    "--config-file",
    "-C",
    type=click.Path(path_type=Path),
    default=CtaOpsConfigLoader.find_default_config_file(project_name="ctaops", file_name="ctaops-cli.toml"),
    help="Path to valid yaml config file",
    show_default=True,
)
@click.pass_context
def ctaops(ctx: click.Context, format: str, log_level: str, config_file: Optional[Path]):
    """Manage CERN Tape Archive (CTA) operations from the command line.

    This is the main entry point for the CTA operations Cli. Use subcommands to
    manage tapes, drives, storage classes, and other CTA resources.
    """
    config = (
        CtaOpsConfigLoader(AppConfig)
        .load_from_toml(config_file, file_exist_error="ignore")
        .set("display", "format", value=format)
        .set("logging", "level", value=log_level)
        .get_safe_config()
    )

    configure_logging(config.logging)

    cta_client = get_cta_client(config.backend, **asdict(getattr(config.backends, config.backend)))
    workflow_client = None

    # if a workflow endpoint has been configured, attempt to initialize the ProtobufRpcClient
    if config.workflow.frontend_endpoint:
        try:
            from cta.sdk.client.grpc.workflow import WorkflowRpcClient
        except ModuleNotFoundError as exc:
            raise click.ClickException(
                "Workflow support requires optional dependencies. Install with: pip install 'ctaops-cli[workflow]'"
            ) from exc
        workflow_client = WorkflowRpcClient(config.workflow)

    ctx.obj = AppContext(config, cta_client, workflow_client=workflow_client)


@ctaops.command(aliases=["v"])
@click.pass_obj
def cta_version(obj: AppContext):
    """Show CTA backend version information."""
    print_dataclasses(obj.cta_client.cta_version, display_config=obj.config.display)


def load_commands(package_name: str):
    """Dynamically load all command modules from a package.

    Imports all modules from the specified package and registers any
    Click Group objects found as subcommands of the main ctaops group.

    Args:
        package_name: Fully qualified name of the package to load commands from.
    """
    package = importlib.import_module(package_name)

    for _, module_name, _ in pkgutil.iter_modules(package.__path__):
        module = importlib.import_module(f"{package_name}.{module_name}")
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, click.Group):
                # Preserve aliases when adding command
                ctaops.add_command(attr)


def _infer_output_format(argv: t.Optional[list[str]] = None) -> t.Optional[str]:
    """Infer the output format from command-line arguments or config file."""
    # Unfortunately, the only way to do this seems to be creating a temporary Click
    # context and parsing the arguments.
    args = argv if argv is not None else sys.argv[1:]
    try:
        ctx = ctaops.make_context("ctaops", args, allow_extra_args=True)
    except Exception:
        return None

    explicit_format = ctx.params.get("format")
    config_file = ctx.params.get("config_file") or Path("/etc/ctaops/ctaops-cli.toml")

    # there is a format set via the CLI
    if explicit_format:
        return explicit_format

    try:
        config = CtaOpsConfigLoader(AppConfig).load_from_toml(config_file, "ignore").get_safe_config()
        # there is a format set via the config file
        return getattr(getattr(config, "display", None), "format", None)
    except Exception:
        return None


class WorkflowRpcCliError(click.ClickException):
    """Workflow gRPC call failed at the transport level (exit code 2)."""

    # we set a custom exit code to highlight that this is a server error
    exit_code = 2


class WorkflowConfigCliError(click.ClickException):
    """Workflow gRPC call failed due to misconfiguration (exit code 3)."""

    # we set a custom exit code to highlight that this is a configuration error
    exit_code = 3


def run(argv: t.Optional[list[str]] = None) -> None:
    """Run the Cli application and map exceptions to standard exit codes."""
    load_commands("ctaops.cli.commands")
    try:
        try:
            ctaops.main(args=argv, standalone_mode=False)
        except WorkflowRpcError as exc:
            raise WorkflowRpcCliError(str(exc)) from exc
        except WorkflowConfigError as exc:
            raise WorkflowConfigCliError(str(exc)) from exc
        except CtaSdkError as exc:
            raise click.ClickException(str(exc)) from exc
    except click.ClickException as exc:
        output_format = _infer_output_format(argv)
        if output_format in {"raw", "json"}:
            exc.show(file=sys.stderr)
        else:
            console = Console(file=sys.stderr)
            console.print(Panel(str(exc), title="[red]Error[/red]", title_align="left"))
        raise SystemExit(exc.exit_code) from exc


def main():
    """Entry point for the Cli application.

    Loads command modules and starts the Cli event loop.
    """
    run()


if __name__ == "__main__":
    main()
