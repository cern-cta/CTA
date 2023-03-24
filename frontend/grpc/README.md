# gRPC frontend for  CTA

gRPC based frontend for disk storage systems. This is thin layer that only populates

## Configuration

The  _cta-frontend-grpc_ interface requires only connection to the CTA catalog
and the scheduler. Those configurations are taken from `/etc/cta/cta-catalogue.conf` and
`/etc/cta/cta.conf`. The TPC port number used to accept storage system requests is specified
in `/etc/sysconfig/cta-frontend-grpc` and defaults to `17017`.
