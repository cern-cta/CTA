# Developing with dCache

!!! note "Contributions welcome"

    Contributions from external developers actively working with dCache and CTA are welcome, particularly a validated development setup, an explanation of the integration components, and guidance for running integration tests.

The development tooling supports selecting a dCache-backed test deployment; see [cta-dev Reference](../tools-and-environment/cta-dev.md#dcache). The missing guidance here concerns developing and validating that integration, not its availability.

## Error report URLs

The dCache CTA plugin gives CTA report URLs of the form `eosQuery://<host>:<port>/success/<pnfsid>?archiveid=<id>` and `eosQuery://<host>:<port>/error/<pnfsid>?error=` for both archive and retrieve requests.
They point to an XRootD endpoint run by the plugin, which CTA contacts with an opaque query, as it does for EOS.
When a request fails, CTA appends the base64-encoded failure reason to the empty `error=` parameter, and the plugin fails the dCache request with the decoded reason.
See [Error report URLs](eos/archival.md#error-report-urls) for how CTA prepares the failure reason.

## Upstream references

- [dCache CTA plugin](https://github.com/dCache/dcache-cta#readme): integration source, build instructions, and configuration.
- [dCache Developer’s Corner](https://www.dcache.org/development/): development resources and contribution guidance for dCache itself.
- [dCache documentation](https://www.dcache.org/documentation/): administrator and user guides by release.

For operational references, see [dCache Integration](../../../ops/deploy-and-configure/integrations/dcache.md).
