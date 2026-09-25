# Scheduler

The scheduler coordinates archive, retrieve, and repack work across CTA services. It provides the queues and scheduling decisions used by tape daemons to select and execute tape work.

## Shared backend

The scheduler relies on a data store for requests, queues, and coordination state. CTA has objectstore and PostgreSQL scheduler backends. This work state is separate from the catalogue's persistent record of tape copies and resources.

## Relationships with services

The Workflow API queues requests, tape daemons select and process work, and the Maintenance Daemon handles reporting and backend maintenance. Administrative tools inspect or manage scheduler state through the Admin API.

See [Scheduling](../data-management/scheduling.md) for batching, mount selection, and policies. [Scheduler Configuration](../../ops/configuration/scheduler.md) covers deployment; [Scheduling and Queues](../../ops/administration/requests.md) covers operator procedures.
