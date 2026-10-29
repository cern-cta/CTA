# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""The top level package to interact with CTA.

You can directly begin by getting a CtaClient object and explore the different Dao
available to interact with CTA resource.

Example:
    ```python
    from cta.sdk import CtaClient, get_cta_client
    cta_client = get_cta_client("CLI")
    ```
"""

from cta.sdk.client.protocol import CtaClient
from cta.sdk.factory import get_cta_client

__all__ = ["CtaClient", "get_cta_client"]
