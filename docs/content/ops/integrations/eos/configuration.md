# EOS Configuration

This page is specific to the EOS integration. Configure the [Workflow API](../../configuration/workflow-api.md) and [Admin API](../../configuration/admin-api.md) and [authentication](../../configuration/authentication.md) separately.

## Workflow connection

!!! info "Documentation outline"
    Document the EOS workflow endpoint, credentials, directory policies, and a verification procedure for the integration here.

## Data-transfer authentication

Document SSS credentials shared by EOS and the tape daemons, permissions, and key rotation here.

## Historical configuration notes

!!! warning "Needs content review"
    The following material was moved from the deprecated archival workflow page. It describes the SSI integration and must be checked against the selected EOS/CTA deployment before use.

### 1. Configure EOS for tape-backed operation

#### A. Enable tape features

Tape-related features including the "proto" workflow event handlers are disabled by default. To enable these features, set `tapeenabled` to true.

`protowfendpoint` is the hostname and port of the CTA Frontend.

`protowfresource` should always be set to the literal `/ctafrontend`. The XRootD protocol allows for different resources
on the server (see [XRootD Scalable Service Interface](https://xrootd.slac.stanford.edu/doc/dev50/ssi_reference-V3.htm#_Toc4172913)
documentation). In practice only `/ctafrontend` is defined.

In `/etc/xrd.cf.mgm`:

```
mgmofs.tapeenabled true
mgmofs.protowfendpoint ctafrontend:10955
mgmofs.protowfresource /ctafrontend
```

Also ensure that v2 of the file system object instantiation API is enabled:

```
xrootd.fslib -2 libXrdEosMgm.so
```

The `-2` tells XRootD that the MGM will handle `query prepare` requests. (In v1 this was specified with `ofs.preplib` which is no longer required in v2).

In `/etc/xrd.cf.fst`:

```
fstofs.protowfendpoint ctafrontend:10955
fstofs.protowfresource /ctafrontend
```

#### B. Create extended attributes on destination directory

```
# eos attr ls /eos/ctaatlas/archive
sys.acl="u:10763:rwx+dp,u:98119:rwx+dp,z:!u,u:0:+u"
sys.cta.storage_class="migration"
sys.eos.btime="1592827411.338239153"
sys.forced.checksum="adler"
sys.link.workflow.sync::abort_prepare.default="proto"
sys.link.workflow.sync::archive_failed.default="proto"
sys.link.workflow.sync::archived.default="proto"
sys.link.workflow.sync::closew.default="proto"
sys.link.workflow.sync::closew.retrieve_written="proto"
sys.link.workflow.sync::create.default="proto"
sys.link.workflow.sync::delete.default="proto"
sys.link.workflow.sync::evict_prepare.default="proto"
sys.link.workflow.sync::prepare.default="proto"
sys.link.workflow.sync::retrieve_failed.default="proto"
```

The value `"proto"` for the workflow event handlers is a literal which is used by the MGM and FST to send event handling
requests via the [XRootD SSI Protocol Buffer interface](https://gitlab.cern.ch/eos/xrootd-ssi-protobuf-interface) used
by the CTA Frontend.

`sys.acl` user flags are `rwx+dp`. `+d` means the user is allowed to delete the file. `p` means the user has PREPARE permission,
*i.e.* bring a file online from tape to disk. `z:` is a rule for all non-root users; `z:!u` means that files are not updatable:
they are immutable and may not be appended to or modified.

`sys.cta.storage_class` must be set to a valid CTA storage class with a defined archive route. This is inherited by
newly-created files and validated during the **CREATE** workflow event.

## Related configuration and procedures

- [Tape REST API](tape-rest-api.md)
- [Performance and disk layout](performance.md)
- [Metadata consistency and recovery](metadata-recovery.md)
- [Troubleshooting and repair](troubleshooting.md)
