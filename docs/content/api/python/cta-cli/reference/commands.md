# Commands

Every command module lives under `cta.cli.commands`. Each module exports one `click.Group` (typically named after the resource it manages) and is auto-loaded at start-up by [`load_commands`][cta.cli.main.load_commands].

For a behaviour-oriented overview see the [command examples](../../../../ops/tools/cta-usage.md). The reference below is the source-level documentation for every command module.

::: cta.cli.commands
