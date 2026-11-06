# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Module for obtaining concrete CtaClient instances.

This module provides a factory function for instantiating the appropriate
CtaClient implementation based on the requested backend.
"""

from importlib import import_module
from typing import Any, Literal

from cta.sdk.client.cta_admin import CtaClientCtaAdmin
from cta.sdk.client.protocol import CtaClient


def get_cta_client(backend: Literal["CTA_ADMIN", "MOCK"], *args: Any, **kwargs: Any) -> CtaClient:
    """Return a concrete CtaClient instance for the given backend.

    Factory function that selects and instantiates the appropriate
    CtaClient subclass based on the `backend` identifier.

    Args:
        backend: Name of the backend to use. `"CTA_ADMIN"` and `"MOCK"` are supported.
        *args: Positional arguments forwarded to the client constructor.
        **kwargs: Keyword arguments forwarded to the client constructor.
            For `"CTA_ADMIN"` backend, see
            [CtaClientCtaAdmin][cta.sdk.client.cta_admin.CtaClientCtaAdmin] for accepted
            parameters.
            For `"MOCK"` backend, see
            [CtaClientMock][cta.sdk.client.mock.CtaClientMock] for accepted
            parameters.

    Warning:
        Use `"MOCK"` backend only for unit tests, there is nothing behind just raw local files,
        no scheduler, no catalogue DB, no real frontend.

    Returns:
        A concrete CtaClient instance for the requested backend.

    Raises:
        NotImplementedError: If `backend` is not a recognized value.

    Example:
        ```python
        from cta.sdk import get_cta_client

        client = get_cta_client("CTA_ADMIN", timeout=120)
        ```
    """
    if backend == "CTA_ADMIN":
        return CtaClientCtaAdmin(*args, **kwargs)
    elif backend == "MOCK":
        try:
            import_module("cattrs")
        except ImportError as e:
            raise ImportError("Cannot instantiate CtaClient with MOCK backend: [dev] extra is not installed") from e
        from cta.sdk.client.mock import CtaClientMock

        return CtaClientMock(*args, **kwargs)
    else:
        raise NotImplementedError(f"Backend {backend!r} is not available")
