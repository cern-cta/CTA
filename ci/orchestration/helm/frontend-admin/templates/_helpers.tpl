{{- /*
SPDX-FileCopyrightText: 2025 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{- define "ctafrontend.fullname" -}}
  {{ include "common.names.fullname" . }}
{{- end }}

{{- define "ctafrontendgrpc.fullname" -}}
  {{ printf "%s-grpc" (include "common.names.fullname" .) }}
{{- end }}

{{- define "ctafrontend.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "ctafrontend.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "ctafrontend.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "scheduler.config" -}}
  {{- $schedulerConfig := .Values.global.configuration.scheduler }}
  {{- if ne (typeOf $schedulerConfig) "string" -}}
    {{- $schedulerConfig = $schedulerConfig | toYaml }}
  {{- end }}
  {{- $schedulerConfig -}}
{{- end }}
