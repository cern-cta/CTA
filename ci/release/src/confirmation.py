# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Consistent interactive confirmation for release workflows."""

from __future__ import annotations

import sys
from collections.abc import Sequence

from errors import ReleaseWorkflowError


class ConfirmationError(ReleaseWorkflowError):
    """A required release confirmation was unavailable or declined."""


def ask_yes_no(
    prompt: str,
    *,
    warnings: Sequence[str] = (),
    assume_yes: bool = False,
    dry_run: bool = False,
    default_yes: bool = False,
) -> bool:
    """Display warnings and return an explicit yes/no decision."""
    for warning in warnings:
        print(f"WARNING: {warning}", file=sys.stderr)

    if assume_yes:
        return True
    if dry_run:
        print(f"DRY-RUN: would ask: {prompt}")
        return False
    if not sys.stdin.isatty():
        raise ConfirmationError("Confirmation requires an interactive terminal; rerun with --yes where supported")

    try:
        choice_hint = "[Y/n]" if default_yes else "[y/N]"
        answer = input(f"{prompt} {choice_hint} ").strip().lower()
    except EOFError as error:
        raise ConfirmationError("Confirmation input ended unexpectedly") from error
    return answer in ("y", "yes") or (default_yes and not answer)


def confirm(
    prompt: str,
    decline_message: str,
    *,
    warnings: Sequence[str] = (),
    assume_yes: bool = False,
    dry_run: bool = False,
) -> None:
    """Require an affirmative decision or raise a workflow error."""
    if not ask_yes_no(
        prompt,
        warnings=warnings,
        assume_yes=assume_yes,
        dry_run=dry_run,
    ):
        if dry_run:
            return
        raise ConfirmationError(decline_message)
