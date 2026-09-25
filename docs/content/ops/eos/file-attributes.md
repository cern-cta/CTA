# EOS File Attributes

A file on EOS that is archived on CTA may have the following extended attributes (`xattr`):

- `sys.retrieve.req_id`: List of Retrieve request IDs for this file
- `sys.retrieve.req_time`: Last time the Retrieve request was actioned
- `sys.retrieve.evict_counter`: Counter for multiple staging requests on same file
- `sys.retrieve.error`: Retrieve request failure reason
- `sys.archive.error`: Archive request failure reason
- `sys.archive.file_id`: Archive file ID
- `sys.archive.storage_class`: Archive storage class

The following attributes are workflow related:

- `sync::retrieve_failed`
- `sync::archive_failed`
- `sync::create`
- `sync::closew`
- `sync::archived`
- `sync::archive_failed`
- `sync::prepare`
- `sync::abort_prepare`
- `sync::evict_prepare`
- `sync::closew.retrieve_written`
- `sync::retrieve_failed`
- `sync::delete`

There may be two additional attributes present, which are specific to the objectstore in CTA:

- `sys.cta.archive.objectstore.id`: CTA internal objectsore id for archive requests
- `sys.cta.objectstore.id`: CTA internal objectsore id for retrieve requests

These attributes exist because the objectstore cannot index files efficiently by anything other than their id. With the new Postgres scheduler, these will be removed.

To check any of the above attributes for a give file id `<fid>`, run:

```sh
eos -j file info fid:<id> | jq .
```

## Timestamps

EOS also associates a number of timestamps with a file. Specifically:

- `btime`: the birth/creation timestamp
- `mtime`: the modification timestamp
- `ctime`: the change timestamp

When archiving a file on CTA (or doing any workflow on CTA for that matter), only the `ctime` will change. Files on EOS will not have their `btime` and `mtime` affected by the CTA archival process.
