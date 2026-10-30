# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Module for obtaining concrete CtaClient instances.

This module provides a factory function for instantiating the appropriate
CtaClient implementation based on the requested backend.
"""

from importlib import import_module
from typing import Any, Literal

from cta.sdk.client.cli import CtaClientCli
from cta.sdk.client.protocol import CtaClient


def get_cta_client(backend: Literal["CLI", "FILE"], *args: Any, **kwargs: Any) -> CtaClient:
    """Return a concrete CtaClient instance for the given backend.

    Factory function that selects and instantiates the appropriate
    CtaClient subclass based on the `backend` identifier.

    Args:
        backend: Name of the backend to use. `"CLI"` and `"FILE"` are supported.
        *args: Positional arguments forwarded to the client constructor.
        **kwargs: Keyword arguments forwarded to the client constructor.
            For `"CLI"` backend, see
            [CtaClientCli][cta.sdk.client.cli.CtaClientCli] for accepted
            parameters.
            For `"File"` backend, see
            [CtaClientFile][cta.sdk.client.file.CtaClientFile] for accepted
            parameters.

    Warning:
        Use `"File"` backend only for unit tests, there is nothing behind just raw local files,
        no scheduler, no catalogue DB, no real frontend.

    Returns:
        A concrete CtaClient instance for the requested backend.

    Raises:
        NotImplementedError: If `backend` is not a recognized value.

    Example:
        ```python
        from cta.sdk import get_cta_client

        client = get_cta_client("CLI", timeout=120)
        ```
    """
    if backend == "CLI":
        return CtaClientCli(*args, **kwargs)
    elif backend == "FILE":
        try:
            import_module("cattrs")
        except ImportError as e:
            raise ImportError("Cannot instantiate CtaClient with FILE backend: [dev] extra is not installed") from e
        from cta.sdk.client.file import CtaClientFile

        return CtaClientFile(*args, **kwargs)
    else:
        raise NotImplementedError(f"Backend {backend!r} is not available")
