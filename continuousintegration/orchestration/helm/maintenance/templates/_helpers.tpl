{{- define "ctamaintenance.fullname" -}}
  {{ include "common.names.fullname" . }}
{{- end }}

{{- define "maintenance.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "maintenance.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "maintenance.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "scheduler.config" -}}
  {{- $schedulerConfig := .Values.global.configuration.scheduler }}
  {{- if ne (typeOf $schedulerConfig) "string" -}}
    {{- $schedulerConfig = $schedulerConfig | toYaml }}
  {{- end }}
  {{- $schedulerConfig -}}
{{- end }}
