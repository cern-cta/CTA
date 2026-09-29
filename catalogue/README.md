# Catalogue

The catalogue stores tape and file metadata, archive routes, mount policies, and deployment configuration.

- `Catalogue.hpp`: catalogue interface.
- `interfaces/`: interfaces for individual catalogue entities.
- `rdbms/`: shared relational implementation and database backends.
- `cta-catalogue-schema/`: schema and migration submodule.
- `tests/`: catalogue tests.

See [Catalogue internals](../docs/content/dev/internals/components/catalogue/index.md) for the architecture, [Schema Development](../docs/content/dev/internals/components/catalogue/schema-development.md) for schema changes, and [Catalogue Schema Releases](../docs/content/dev/contributing/maintainers/catalogue-schema-releases.md) for publication.
