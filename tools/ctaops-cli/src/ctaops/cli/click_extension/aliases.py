# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Click extension for command and group aliasing support.

This module provides a custom Click Group class that extends Click's standard
functionality with support for command and group aliases, allowing users to
invoke commands using shorter alternative names.
"""

from typing import Any, Callable, Optional, Protocol, Union, cast

import click
from typing_extensions import override


class _HasAliases(Protocol):
    aliases: list[str]


class ClickAliasedGroup(click.Group):
    """Click Group with support for command and group aliases.

    This custom Click Group class manages both commands/groups and their
    associated aliases, allowing users to invoke commands using shortened
    or alternative names. Aliases are displayed in the help output.

    Attributes:
        _commands: Dictionary mapping command names to their aliases.
        _aliases: Dictionary mapping aliases to their canonical command names.
        aliases: List of aliases for the group itself.
    """

    def __init__(self, *args: Any, aliases: Optional[list[str]] = None, **kwargs: Any) -> None:
        """Initialize the ClickAliasedGroup.

        Args:
            *args: Positional arguments passed to parent Click Group.
            aliases: List of aliases for the group itself.
            **kwargs: Keyword arguments passed to parent Click Group.
        """
        super().__init__(*args, **kwargs)
        self._commands: dict[str, list[str]] = {}
        self._aliases: dict[str, str] = {}
        self.aliases = aliases or []

    @override
    def add_command(self, cmd: click.Command, name: Optional[str] = None, aliases: Optional[list[str]] = None) -> None:
        """Add a command to the group with optional aliases.

        Args:
            cmd: The Click command to add.
            name: Optional override name for the command.
            aliases: Optional list of aliases for the command.
        """
        # Extract aliases if they were stored on the command object
        if aliases is None and hasattr(cmd, "aliases"):
            aliases = cast(_HasAliases, cmd).aliases

        # Call parent's add_command with proper arguments
        super().add_command(cmd, name=name)

        # Get the actual command name that was registered
        cmd_name = name or cmd.name
        if cmd_name is None:
            raise TypeError("Command has no name.")

        # Register aliases
        if aliases:
            self._commands[cmd_name] = aliases
            for alias in aliases:
                self._aliases[alias] = cmd_name

    def command(  # type: ignore[override]
        self, *args: Any, **kwargs: Any
    ) -> Union[Callable[[Callable[..., Any]], click.Command], click.Command]:
        """Register a command with optional aliases.

        Args:
            *args: Positional arguments passed to parent command decorator.
            **kwargs: Keyword arguments passed to parent command decorator.
                Can include 'aliases' key for command aliases.

        Returns:
            Decorator function for registering the command.
        """
        aliases = kwargs.pop("aliases", [])
        decorator = super().command(*args, **kwargs)
        if not aliases:
            return decorator

        def _decorator(f: Callable[..., Any]):
            cmd = decorator(f)
            if cmd.name is None:
                raise TypeError("Command has no name.")
            if aliases:
                self._commands[cmd.name] = aliases
                for alias in aliases:
                    self._aliases[alias] = cmd.name
            return cmd

        return _decorator

    def group(  # type: ignore[override]
        self, *args: Any, **kwargs: Any
    ) -> Union[Callable[[Callable[..., Any]], click.Group], click.Group]:
        """Register a group with optional aliases.

        Args:
            *args: Positional arguments passed to parent group decorator.
            **kwargs: Keyword arguments passed to parent group decorator.
                Can include 'aliases' key for group aliases.

        Returns:
            Decorator function for registering the group.
        """
        aliases = kwargs.pop("aliases", [])
        kwargs.setdefault("cls", ClickAliasedGroup)
        decorator = super().group(*args, **kwargs)
        if not aliases:
            return decorator

        def _decorator(f: Callable[..., Any]):
            cmd = decorator(f)
            if aliases:
                # Store aliases on the command object itself
                cast(_HasAliases, cmd).aliases = aliases
                if cmd.name is None:
                    raise TypeError("Group has no name.")
                self._commands[cmd.name] = aliases
                for alias in aliases:
                    self._aliases[alias] = cmd.name
            return cmd

        return _decorator

    def resolve_alias(self, cmd_name: str) -> str:
        """Resolve a command alias to its canonical command name.

        Args:
            cmd_name: The command name or alias to resolve.

        Returns:
            The canonical command name, or the original name if not an alias.
        """
        if cmd_name in self._aliases:
            return self._aliases[cmd_name]
        return cmd_name

    @override
    def get_command(self, ctx: click.Context, cmd_name: str) -> Optional[click.Command]:
        """Get a command by name, resolving aliases.

        Args:
            ctx: Click context object.
            cmd_name: The command name or alias to retrieve.

        Returns:
            The Click command object, or None if not found.
        """
        cmd_name = self.resolve_alias(cmd_name)
        command = super().get_command(ctx, cmd_name)
        if command:
            return command
        return None

    @override
    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        """Format the help text for available commands, showing aliases.

        Args:
            ctx: Click context object.
            formatter: Click help formatter.
        """
        rows = []

        sub_commands = self.list_commands(ctx)

        max_len = 0
        if len(sub_commands) > 0:
            max_len = max(len(cmd) for cmd in sub_commands)

        limit = formatter.width - 6 - max_len

        for sub_command in sub_commands:
            cmd = self.get_command(ctx, sub_command)
            if cmd is None:
                continue
            if hasattr(cmd, "hidden") and cmd.hidden:
                continue
            display_name = sub_command
            if sub_command in self._commands:
                aliases = ",".join(sorted(self._commands[sub_command]))
                display_name = f"{sub_command} ({aliases})"
            cmd_help = cmd.get_short_help_str(limit)
            rows.append((display_name, cmd_help))

        if rows:
            with formatter.section("Commands"):
                formatter.write_dl(rows)
