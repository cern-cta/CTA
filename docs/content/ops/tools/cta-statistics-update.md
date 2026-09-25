# `cta-statistics-update`

Recompute cached statistics for tapes marked dirty in the catalogue. This command updates catalogue records; it does not scan tape media.

```console
cta-statistics-update /path/to/catalogue-connection.conf
```

The positional argument supplies the catalogue database connection file. `--help` (`-h`) displays usage. The command reports the number of tapes updated and elapsed time.

See [Catalogue Statistics](../monitoring/catalogue-statistics.md) for scheduling and exporting the results.
