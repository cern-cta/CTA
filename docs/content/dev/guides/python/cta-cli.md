# Extending cta

See the [operator guide](../../../ops/tools/cta.md) for installation and configuration.

## Architecture

```mermaid
flowchart
    operator((Operator))

    subgraph cta_cli[cta-cli]
        click[Click root group]
        commands[Auto-loaded command modules]
        ctx(AppContext)
        display[Display layer<br/>encoders, formatters, printers]
    end

    subgraph cta_sdk[cta-sdk]
        sdk_iface(CtaClient interface)
    end

    subgraph cta_common[cta-common]
        config[Config loader]
        logging[Logging]
    end

    subgraph optional[cta-tapeserver-sdk - optional, on tape servers]
        tpsrv_ctrl[Library &amp; Drive controllers]
    end

    cta_admin[cta-admin]
    frontend[\CTA Frontend/]

    operator -- click --> commands
    commands --> ctx --> sdk_iface
    sdk_iface -- through --> cta_admin --> frontend
    commands --> display
    ctx --> config
    ctx --> logging
    commands -. tapeserver extra .-> tpsrv_ctrl
```

### Command resolution

1. Operator types e.g. `cta tape list --all`.
2. Click's root group (`cta`) parses the global options and builds the [AppContext](../../../api/python/cta-cli/reference/entrypoint.md#cta.cli.context.AppContext).
3. The root group resolves `tape` (or its alias `ta`) using the custom [ClickAliasedGroup](../../../api/python/cta-cli/reference/entrypoint.md#cta.cli.click_extension.aliases.ClickAliasedGroup).
4. The `list` subcommand executes against the [CtaClient](../../../api/python/cta-sdk/reference/client.md#cta.sdk.client.protocol.CtaClient) held in the context.
5. The result is formatted by the [display layer](../../../api/python/cta-cli/reference/display.md#cta.cli.display.printers) according to the selected format (`table` / `json` / `raw`).

### Extensible by convention

New command groups are picked up automatically: drop a new module in [cta.cli.commands](../../../api/python/cta-cli/reference/commands.md#cta.cli.commands) that exports a `click.Group`, and the next CLI invocation will load it via [load_commands](../../../api/python/cta-cli/reference/entrypoint.md#cta.cli.main.load_commands).
