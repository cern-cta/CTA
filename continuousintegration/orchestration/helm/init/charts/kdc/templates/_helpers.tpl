{{/*
Get the namespace
*/}}
{{- define "kdc.namespace" -}}
{{ $namespace := .Release.Namespace | quote}}
{{- end -}}


{{- define "kdc.name" -}}
  {{ include "common.names.name" . }}
{{- end -}}

{{- define "kdc.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end -}}

{{- define "kdc.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end -}}

{{- define "kdc.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end -}}

{{/*
Sets pesistent volumes for the chart.
*/}}
{{- define "kdc.volumes" -}}
{{- if or (.Values.global.volumes) (.Values.volumes) -}}
    {{- if (.Values.global.volumes) -}}
    {{- .Values.global.volumes | toYaml -}}
    {{- end -}}
    {{- if (.Values.volumes) -}}
    {{- .Values.volumes | toYaml -}}
    {{- end -}}
{{- end -}}
{{- end -}}

{{- define "kdc.volumeMounts" -}}
{{- if or (.Values.global.volumeMounts) (.Values.volumeMounts) -}}
  {{- if (.Values.global.volumeMounts) -}}
    {{- .Values.global.volumeMounts | toYaml -}}
  {{- end -}}
  {{- if (.Values.volumeMounts) -}}
    {{- .Values.volumeMounts | toYaml -}}
  {{- end -}}
{{- end -}}
{{- end -}}
