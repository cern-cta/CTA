!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Scheduler Configuration

Configure the scheduler backend used by the CTA services. See [Scheduler DB](../../../concepts/components/scheduler-db.md).

## Objectstore backend

!!! note "TODO"
    TODO: Document provisioning, connection configuration, access permissions, and verification.

## PostgreSQL backend

The schema is created and dropped with `cta-scheduler-schema-create` and `cta-scheduler-schema-drop`, which connect with the username from the connection string in the configuration file.

!!! tip

    The schema is named `scheduler` by default. To use a different name, pass it as the `search_path` option in the connection string, either plain (`postgresql://user:pw@dbhost/mydb?options=-c search_path=myschemaname`) or URL-encoded (`postgresql://user:pw@dbhost/mydb?options=-c%20search_path%3Dmyschemaname`). The search path is then set on the account, so later sessions use that schema automatically.

!!! note "TODO"
    Document provisioning, connection configuration, and verification.

### Opportunistic batching

With the PostgreSQL backend, the workflow frontend can merge concurrent archive and retrieve queueing requests into a single bulk insert. The feature is experimental and disabled by default. It is configured in `cta-workflow-api.toml` (see `app/cta-workflow-api/cta-workflow-api.pg.example.toml`):

| Key | Default | Meaning |
| --- | --- | --- |
| `experimental.opportunistic_batching_enabled` | `false` | Enables batching for archive and retrieve queueing. |
| `opportunistic_batching.window_ms` | `10` | Maximum time a batch waits for concurrent requests to join before inserting. |
| `opportunistic_batching.max_batch_size` | `1000` | Maximum number of requests per batch. Must be greater than zero. |

A longer window adds latency to each request in exchange for bigger batches. See [PostgreSQL Opportunistic Batching](../../../dev/internals/shared-state/scheduler-db/postgresql/opportunistic-batching.md) for how it works.

## Maintenance and reporting

!!! note "TODO"
    Document the routines required by each backend and how to check that they are running. See [Maintenance Daemon Configuration](maintenance-daemon.md).

## Isolate repack with separate scheduler backends

Repack objectstore scheduler can put a lot of pressure on the objectstore scheduler. This can lead to a significant performance degradation on user requests if repack and user share the same objectstore backend.
Separating User and Repack sheduler backends allows to completely isolate these disjoint use cases and preserve end user service performance during intense tape repack campaigns.

Separating User and Repack requires:

- separate resources for another scheduler backend for repack
- 1 dedicated Admin Frontend (`cta-frontend-admin`) endpoint to allow operators to submit repack requests
- moving some tape drives from user scheduler to repack scheduler when repack is needed

!!! tip

    Use the `REPACK` archive route to create tape pools for the REPACK VO. This feature prevents consuming all experiment writeable tapes when repack to avoid *user write starvation*. It also avoid mixing old repacked files with newly archived user files.

## Objectstore connection example

???+ example "cta-scheduler.example.conf"

    ```text
    --8<--
    scheduler/cta-scheduler.example.conf
    --8<--
    ```

## PostgreSQL connection example

???+ example "cta-scheduler.pg.example.conf"

    ```text
    --8<--
    scheduler/cta-scheduler.pg.example.conf
    --8<--
    ```
