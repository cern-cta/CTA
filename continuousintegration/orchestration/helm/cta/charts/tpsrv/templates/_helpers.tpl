{{- define "tpsrv.name" -}}
  {{ include "common.names.name" . }}
{{- end }}

{{- define "tpsrv.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "tpsrv.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "tpsrv.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "tpsrv.tapeConfig" -}}
  {{- $tapeConfig := .Values.tapeConfig }}
  {{- if ne (typeOf $tapeConfig) "string" -}}
    {{- $tapeConfig = $tapeConfig | toYaml }}
  {{- end }}
  {{- $tapeConfig -}}
{{- end }}

{{- define "scheduler.config" -}}
  {{- $schedulerConfig := .Values.global.configuration.scheduler }}
  {{- if ne (typeOf $schedulerConfig) "string" -}}
    {{- $schedulerConfig = $schedulerConfig | toYaml }}
  {{- end }}
  {{- $schedulerConfig -}}
{{- end }}

{{/* Check if systemd is enabled. Use global if available, otherwise fallback to local value. */}}
{{- define "tpsrv.useSystemd" -}}
{{- if .Values.global.useSystemd }}
  {{- .Values.global.useSystemd }}
{{- else }}
  {{- .Values.useSystemd }}
{{- end }}
{{- end }}
