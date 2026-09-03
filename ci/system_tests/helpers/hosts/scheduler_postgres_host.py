# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from system_tests.helpers.connections.remote_connection import RemoteConnection
from .remote_host import RemoteHost


class SchedulerPostgresHost(RemoteHost):
    """The postgres scheduler DB pod.

    Only present when the postgres scheduler is deployed: see the `scheduler_postgres` fixture,
    which skips a test rather than handing back a host that does not exist.
    """

    def __init__(self, conn: RemoteConnection) -> None:
        super().__init__(conn)

    def run_sql(self, sql: str) -> str:
        """Run one statement against the scheduler DB and return its unaligned, header-less output."""
        # POSTGRES_USER and POSTGRES_DB are set as environment variables on the scheduler postgres pod
        return self.exec_with_output(
            f'psql -q -t -A -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "{sql}"'
        )

    def insert_row(self, table: str, columns: str, values: str, *, returning: str = "JOB_ID") -> str:
        """Insert one row and return the value of the given `returning` column.

        Useful for seeding e.g. the failed job queues directly, to exercise failure scenarios which
        cannot be triggered reliably by driving an actual archive/retrieve through the system.
        """
        # The statement is built from the literals the callers define; a table/column name cannot be
        # bound as a query parameter anyway
        sql = f"INSERT INTO {table} ({columns}) VALUES ({values}) RETURNING {returning}"  # noqa: S608
        result = self.run_sql(sql)
        assert result.isdigit(), f"unexpected {returning} returned by the insert into {table}: '{result}'"
        return result
