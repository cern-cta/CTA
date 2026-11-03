# Configuration

Dataclasses describing the on-disk TOML configuration of the CLI. These are the structures that [`AppConfig`][cta.cli.config.AppConfig] is loaded into by [`CtaOpsConfigLoader`][ctaops.utils.config.CtaOpsConfigLoader] before being made available on the [`AppContext`][cta.cli.context.AppContext].

::: cta.cli.config
    options:
        merge_init_into_class: true
