# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Application context and version management for CTA Operations Cli.

This module defines the application context dataclass that holds shared state
between Cli commands, including configuration and CTA client instances.
It also manages version detection for the Cli package.
"""

import importlib.util
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar

import click
from cta.sdk.client.protocol import CtaClient
from ctaops.cli.config import AppConfig

if TYPE_CHECKING:
    from cta.sdk.client.grpc.workflow import WorkflowRpcClient

try:
    __version__ = version("ctaops-cli")
except PackageNotFoundError:
    __version__ = "UNKNOWN"

EXTRA_TPSRV = importlib.util.find_spec("ctaops.sdk") is not None


@dataclass
class AppContext:
    """Application context containing shared Cli state.

    This dataclass is passed through Click's context object to all
    subcommands, making the configuration and CTA client available
    throughout the command execution.

    Attributes:
        config: Application configuration containing display, logging,
            and backend settings.
        cta_client: Initialized CTA backend client for executing operations.
    """

    config: AppConfig
    cta_client: CtaClient
    workflow_client: Optional["WorkflowRpcClient"] = None
    workflow_cmd_opts: Optional[dict[str, Any]] = None


F = TypeVar("F", bound=Callable[..., Any])


def tapeserver_option(*option_args: str, **option_kwargs: Any) -> Callable[[F], F]:
    """Conditionally add Click options based on tapeserver availability.

    This decorator allows Cli commands to optionally support tapeserver-specific options
    only when the ctaops.sdk package is installed. If the package is not available,
    the decorator returns the function unchanged.

    Args:
        *option_args: Positional arguments to pass to click.option().
        **option_kwargs: Keyword arguments to pass to click.option().

    Returns:
        A decorator function that conditionally applies click.option decorator.
    """

    def decorator(func: F) -> F:
        # Detect project name automatically from package metadata of func module
        if EXTRA_TPSRV:
            return click.option(*option_args, **option_kwargs)(func)
        else:
            return func

    return decorator
