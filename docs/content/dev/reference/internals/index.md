# Implementation Internals

This section is for developers working on CTA components: understanding their design, changing their behaviour, and validating those changes. Maintainers use the same technical guidance when reviewing changes; release and project-administration procedures live under [For Maintainers](../../contributing/maintainers/index.md).

Choose the guide by the task:

| Task | Where to go |
| --- | --- |
| Understand component responsibilities and system behaviour | [Concepts](../../../concepts/index.md) |
| Develop a component or test a schema migration | The relevant component below; for catalogue changes, start with [Schema Development](catalogue/schema-development/index.md). |
| Coordinate, tag, and publish a release | [For Maintainers](../../contributing/maintainers/index.md), including [Catalogue Schema Releases](../../contributing/maintainers/catalogue-schema-releases.md). |
| Apply an upgrade to a deployed instance | [Operations](../../../ops/index.md) |

For schema changes, writing and testing the migration is part of development. Maintainers then review that evidence and publish the schema and CTA releases; operators carry out the deployment. An upgrade test in a development environment or on an isolated database clone belongs to the first task, even when performed by a maintainer.
