/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

-- The pgsched scheduler schema has no automated migration tooling today (unlike the catalogue
-- schema, which uses Liquibase under catalogue/cta-catalogue-schema/migrations/) - schema creation
-- only ever produces a fresh schema from postgres_scheduler_schema.sql. This file exists because
-- that gap matters here: AJS_WaitReplicasBeforeReportingFailureToDisk was added to
-- ARCHIVE_JOB_STATUS in postgres_scheduler_schema.sql, which only takes effect for a schema
-- created from scratch. An EXISTING scheduler schema's ARCHIVE_JOB_STATUS type still lacks this
-- value, and the new settlement SQL that writes it (failTransferBatchExceedRetries(), see
-- failure-handling.md) will fail against that schema with "invalid input value for enum
-- ARCHIVE_JOB_STATUS" the first time a user multi-copy archive request needs to park a copy.
--
-- Run this once against every existing scheduler schema, before deploying binaries built from
-- this change. IF NOT EXISTS makes it safe to run more than once. ADD VALUE takes an
-- ACCESS EXCLUSIVE lock on the type for the duration of the statement but does not block readers
-- of rows already using the type - only concurrent DDL on the same type.
ALTER TYPE ARCHIVE_JOB_STATUS ADD VALUE IF NOT EXISTS 'AJS_WaitReplicasBeforeReportingFailureToDisk';
