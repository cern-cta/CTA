{{- /*
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{- $schedulerConfig := include "scheduler.config" . | fromYaml }}
{{- $username := $schedulerConfig.postgresConfig.username }}
{{- $schema := $schedulerConfig.postgresConfig.schema | default "scheduler" }}

-- Create the scheduler schema owned by the application user
CREATE SCHEMA IF NOT EXISTS {{ $schema }} AUTHORIZATION {{ $username }};

-- Grant full control on the schema itself
GRANT ALL ON SCHEMA {{ $schema }} TO {{ $username }};

-- Grant full privileges on existing objects (idempotent, safe on first run)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {{ $schema }} TO {{ $username }};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {{ $schema }} TO {{ $username }};
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {{ $schema }} TO {{ $username }};

-- Ensure all future objects also grant privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA {{ $schema }} GRANT ALL PRIVILEGES ON TABLES TO {{ $username }};
ALTER DEFAULT PRIVILEGES IN SCHEMA {{ $schema }} GRANT ALL PRIVILEGES ON SEQUENCES TO {{ $username }};
ALTER DEFAULT PRIVILEGES IN SCHEMA {{ $schema }} GRANT ALL PRIVILEGES ON FUNCTIONS TO {{ $username }};

-- Ensure all future connections use this schema by default
ALTER ROLE {{ $username }} SET search_path TO {{ $schema }}, public, pg_catalog;
