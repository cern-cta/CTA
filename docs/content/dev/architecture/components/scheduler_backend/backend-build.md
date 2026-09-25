# CTA Build with PostgreSQL scheduler backend


## Build Configuration

The PostgreSQL scheduler is enabled at compile time using the CTA_USE_PGSCHED CMake flag. By default, CTA builds with the object store scheduler; the PostgreSQL scheduler must be explicitly requested.

To enable the PostgreSQL scheduler, pass `-DCTA_USE_PGSCHED:Bool=TRUE` to CMake.

## Build Dependencies

When building with PostgreSQL scheduler support:

* **Build-time**: Requires `postgresql-server` for unit tests
* **Runtime**: Requires `postgresql-libs` for database connectivity
* **Excluded**: The `ctaobjectstore` library is not built or linked





