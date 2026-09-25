# Scheduler Configuration

Configure the scheduler backend used by the CTA services. See [Scheduler Concepts](../../concepts/components/scheduler.md).

!!! info "Documentation outline"
    The sections below reserve space for the detailed documentation to be added.

## Objectstore backend

Document provisioning, connection configuration, access permissions, and verification.

## PostgreSQL backend

Document provisioning, schema initialisation, connection configuration, and verification.

## Maintenance and reporting

Document the routines required by each backend and how to check that they are running. See [Maintenance Daemon Configuration](maintenance-daemon.md).

## Isolate repack with separate scheduler backends

Repack objectstore scheduler can put a lot of pressure on the objectstore scheduler. This can lead to a significant performance degradation on user requests if repack and user share the same objectstore backend.
Separating User and Repack sheduler backends allows to completely isolate these disjoint use cases and preserve end user service performance during intense tape repack campaigns.

Separating User and Repack requires:

- separate resources for another scheduler backend for repack
- 1 dedicated Admin API (`cta-frontend-admin`) endpoint to allow operators to submit repack requests
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
