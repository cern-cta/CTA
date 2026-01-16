{{- /*
SPDX-FileCopyrightText: 2025 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{/* Generate the catalogue URL based on backend type */}}
{{- define "catalogue.url" -}}
{{- $catalogueConfig := .Values.configuration }}
{{- if eq (typeOf $catalogueConfig) "string" -}}
  {{- $catalogueConfig = $catalogueConfig | fromYaml }}
{{- end }}
{{- $backend := $catalogueConfig.backend }}
{{- if eq $backend "oracle" -}}
oracle:{{ $catalogueConfig.oracleConfig.username }}/{{ $catalogueConfig.oracleConfig.password }}@{{ $catalogueConfig.oracleConfig.database }}
{{- else if eq $backend "postgres" -}}
postgresql:postgresql://{{ $catalogueConfig.postgresConfig.username }}:{{ $catalogueConfig.postgresConfig.password }}@{{ $catalogueConfig.postgresConfig.server }}/{{ $catalogueConfig.postgresConfig.database }}
{{- else }}
{{- fail (printf "Unsupported catalogue backend type: %s. Please use 'oracle' or 'postgres'." $backend) -}}
{{- end }}
{{- end }}

{{- define "cataloguePostgres.fullname" -}}
  {{- printf "%s-%s" "cta-catalogue" "postgres-db" | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "catalogueReset.fullname" -}}
  {{- printf "%s-%s" "cta-catalogue" "reset" | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "catalogueReset.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.resetImage ) }}
{{- end }}

{{- define "catalogueReset.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.resetImage ) }}
{{- end }}

{{- define "catalogue.config" -}}
  {{- $catalogueConfig := .Values.configuration }}
  {{- if ne (typeOf $catalogueConfig) "string" -}}
    {{- $catalogueConfig = $catalogueConfig | toYaml }}
  {{- end }}
  {{- $catalogueConfig -}}
{{- end }}
