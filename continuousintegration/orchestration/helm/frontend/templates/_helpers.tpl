{{- define "ctafrontend.name" -}}
  {{ include "common.names.name" . }}
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

{{- define "ctafrontend.volumes" -}}
  {{ include "common.volumes" . }}
{{- end -}}

{{- define "ctafrontend.volumeMounts" -}}
  {{ include "common.volumeMounts" . }}
{{- end -}}

{{- define "scheduler.config" -}}
  {{- $schedulerConfig := .Values.global.configuration.scheduler }}
  {{- if ne (typeOf $schedulerConfig) "string" -}}
    {{- $schedulerConfig = $schedulerConfig | toYaml }}
  {{- end }}
  {{- $schedulerConfig -}}
{{- end }}
