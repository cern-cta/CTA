# Catalogue Statistics

CTA provides commands to refresh cached tape statistics and export catalogue statistics for monitoring.

## Refresh tape statistics

[`cta-statistics-update`](../tools/cta-statistics-update.md) updates statistics for tapes marked dirty in the catalogue. It recomputes file counts and byte totals from catalogue records and clears the dirty flag for updated tapes. This changes catalogue data.

## Export statistics

[`cta-statistics-save`](../tools/cta-statistics-save.md) reads catalogue statistics and writes JSON to standard output. Capture that output in the monitoring pipeline. It does not take the historical separate statistics-database configuration argument.

## Scheduling and interpretation

!!! info "Documentation outline"
    Detailed procedures and examples will be completed during the content review.

Document refresh/export ordering, credentials, cadence, failure alerts, and the JSON fields. Explain data freshness and distinguish catalogue accounting from physical tape usage and live scheduler metrics.
