{{- /*
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{/* Define a helper function to generate the Mount Decision DB URL */}}
{{- define "mountDecision.url" -}}
{{- $mountDecisionConfig := .Values.configuration }}
{{- if eq (typeOf $mountDecisionConfig) "string" -}}
  {{- $mountDecisionConfig = $mountDecisionConfig | fromYaml }}
{{- end }}
{{- $backend := $mountDecisionConfig.backend -}}
{{- if eq $backend "postgres" -}}
postgresql:postgresql://{{ $mountDecisionConfig.postgresConfig.username }}:{{ $mountDecisionConfig.postgresConfig.password }}@{{ $mountDecisionConfig.postgresConfig.server }}/{{ $mountDecisionConfig.postgresConfig.database }}
{{- else }}
{{- fail (printf "Unsupported Mount Decision DB backend type: %s. Please use 'postgres'." $backend) -}}
{{- end }}
{{- end }}

{{- define "mountDecisionPostgres.fullname" -}}
  {{- printf "%s-%s" "cta-mount-decision" "postgres-db" | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "mountDecisionReset.fullname" -}}
  {{- printf "%s-%s" "cta-mount-decision" "reset" | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "mountDecisionReset.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.resetImage ) }}
{{- end }}

{{- define "mountDecisionReset.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.resetImage ) }}
{{- end }}

{{- define "mountDecisionReset.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.resetImage ) }}
{{- end }}

{{- define "mountDecision.config" -}}
  {{- $mountDecisionConfig := .Values.configuration }}
  {{- if ne (typeOf $mountDecisionConfig) "string" -}}
    {{- $mountDecisionConfig = $mountDecisionConfig | toYaml }}
  {{- end }}
  {{- $mountDecisionConfig -}}
{{- end }}
