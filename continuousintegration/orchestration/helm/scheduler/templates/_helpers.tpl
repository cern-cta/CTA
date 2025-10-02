// SPDX-FileCopyrightText: 2025 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

{{/* Define a helper function to generate the scheduler URL based on backend type */}}
{{- define "scheduler.url" -}}
{{- $schedulerConfig := .Values.configuration }}
{{- if eq (typeOf $schedulerConfig) "string" -}}
  {{- $schedulerConfig = $schedulerConfig | fromYaml }}
{{- end }}
{{- $backend := $schedulerConfig.backend -}}
{{- if eq $backend "ceph" -}}
rados://{{ $schedulerConfig.cephConfig.id }}@{{ $schedulerConfig.cephConfig.pool }}:{{ $schedulerConfig.cephConfig.namespace }}
{{- else if eq $backend "postgres" -}}
postgresql:postgresql://{{ $schedulerConfig.postgresConfig.username }}:{{ $schedulerConfig.postgresConfig.password }}@{{ $schedulerConfig.postgresConfig.server }}/{{ $schedulerConfig.postgresConfig.database }}
{{- else if eq $backend "vfs" -}}
/objectstore
{{- else if eq $backend "vfsDeprecated" -}}
{{ $schedulerConfig.vfsDeprecatedConfig.path | replace "%NAMESPACE" .Release.Namespace }}
{{- else }}
{{- fail (printf "Unsupported scheduler backend type: %s. Please use 'ceph', 'postgres', or 'file'." $schedulerConfig.backend) -}}
{{- end }}
{{- end }}

{{- define "schedulerPostgres.fullname" -}}
  {{- printf "%s-%s" "cta-scheduler" "postgres-db" | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "schedulerReset.fullname" -}}
  {{- printf "%s-%s" "cta-scheduler" "reset" | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "schedulerReset.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.resetImage ) }}
{{- end }}

{{- define "schedulerReset.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.resetImage ) }}
{{- end }}

{{- define "schedulerReset.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.resetImage ) }}
{{- end }}

{{- define "scheduler.config" -}}
  {{- $schedulerConfig := .Values.configuration }}
  {{- if ne (typeOf $schedulerConfig) "string" -}}
    {{- $schedulerConfig = $schedulerConfig | toYaml }}
  {{- end }}
  {{- $schedulerConfig -}}
{{- end }}
