# Configuration

Dataclasses describing the on-disk TOML configuration of the CLI. These are the structures that [`AppConfig`][ctaops.cli.config.AppConfig] is loaded into by [`CtaOpsConfigLoader`][ctaops.utils.config.CtaOpsConfigLoader] before being made available on the [`AppContext`][ctaops.cli.context.AppContext].

::: ctaops.cli.config
    options:
        merge_init_into_class: true
