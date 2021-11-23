# dCache CTA integration

gRPC based frontend for integration with dCache 7.2.2 and later. This is thin layer
that only populates CTA catalog and scheduler queue. THe connection between a CTA and
a dCache instance maintained by hsm URI, for example:

```
osm://cta/00001D43C0C086CA459298C634D67F68AB6B?archiveid=8402
```

where _00001D43C0C086CA459298C634D67F68AB6B_ corresponds to the dCache unique file id
(so called pnfs id) and _8402_ corresponds to the archive id within CTA catalog.

> NOTE: the original file name, owner and owner group are not propagated by dCache to CTA.

## Configuration

As mentioned above, the _cta-dcache_ interface requires only connection to the CTA catalog
and the scheduler. Those configurations are taken from `/etc/cta/cta-catalogue.conf` and
`/etc/cta/cta.conf`. The TPC port number used to accept dCache requests is specified in
`/etc/sysconfig/cta-dcache` and defaults to `17017`.