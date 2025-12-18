{{- /*
SPDX-FileCopyrightText: 2025 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{- define "ctamaintd.fullname" -}}
  {{ include "common.names.fullname" . }}
{{- end }}

{{- define "maintd.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "maintd.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "maintd.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "scheduler.config" -}}
  {{- $schedulerConfig := .Values.global.configuration.scheduler }}
  {{- if ne (typeOf $schedulerConfig) "string" -}}
    {{- $schedulerConfig = $schedulerConfig | toYaml }}
  {{- end }}
  {{- $schedulerConfig -}}
{{- end }}
