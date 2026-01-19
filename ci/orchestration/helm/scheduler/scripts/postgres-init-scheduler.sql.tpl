{{- /*
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{- $schedulerConfig := include "scheduler.config" . | fromYaml }}
{{- $username := $schedulerConfig.postgresConfig.username }}
{{- $schema := $schedulerConfig.postgresConfig.schema | default "scheduler" }}

-- Create the scheduler schema owned by the application user
CREATE SCHEMA IF NOT EXISTS {{ $schema }} AUTHORIZATION {{ $username }};

-- Ensure all future connections use this schema by default
ALTER ROLE {{ $username }} SET search_path TO {{ $schema }}, public, pg_catalog;
