# Catalogue

The CTA Catalogue schema is used to store all persistent metadata about tape pools, tapes and tape files. It is also used to
store some transient data including drive states, drive configuration and disk instance configuration.

The following section describes the process of making changes to the CTA catalogue schema.

**Developers** who need to make changes to the catalogue schema should refer to [Modifying the CTA Catalogue Schema](schema-development/index.md).

**Operators** who need to upgrade the schema on a CTA production or pre-production instance should refer to [Upgrading the schema](../../../ops/upgrades/catalogue-schema/index.md).

## Catalogue Description

The diagram shows the catalogue tables and their relationships. The sections below describe selected tables.

![CTA Catalogue schema](db-schema.svg)

### Mount Rule Tables

There are 3 mount rule tables defined:

`requester_activity_mount_rule`
`requester_group_mount_rule`
`requester_mount_rule`

The Scheduler code checks these 3 tables for each archive/retrieve request at queueing time by the CTA-Frontend. The purpose is to find a matching mount rule row(s) and resolve the appropriate mount policy. `mount_policy_name` is a column referencing the Mount Policy table.

### Mount Policy Table

This table stores named mount policies which are matched with each archive/retrieve request at queueing time via the Mount Rule tables. They are one of the key parameters determining if the Scheduler shall schedule a mount.

Each row corresponds to a mount policy defined by the following 5 values which are being assigned to each queued Archive and Retrieve Requests:

| Value | Type | Description |
|---|---|---|
| mount_policy_name | String | The name of the mount policy |
| archive_priority | Unsigned int | The priority of Archive Requests. If this number is high, the Archival priority will be high |
| archive_min_request_age (in seconds) | Unsigned int | The minimum age of the queued Archive Request to trigger a mount |
| retrieve_priority | Unsigned int | The priority of RetrieveRequests. If this number is high, the Retrieval priority will be high |
| retrieve_min_request_age (in seconds) | Unsigned int | The minimum age of the queued Retrieve Request to trigger a mount |. Example : if this value is set to 1, the user will have 1 drive for retrieval and one drive for archival within the same tapepool. |
