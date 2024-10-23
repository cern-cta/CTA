{{- define "client.name" -}}
  {{ include "common.names.name" . }}
{{- end }}

{{- define "client.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "client.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "client.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}


{{/*
Sets pesistent volumes for the chart. When loaded as as subchart
it will load additionally all of the global configs as well
(or if .Values.global is defined in subchart as well)
*/}}
{{- define "client.volumes" -}}
{{ if or (.Values.global) (.Values.volumes) }}
    {{- if (.Values.global.volumes)}}
      {{- .Values.global.volumes | toYaml }}
    {{- end }}
    {{- if (.Values.volumes)}}
      {{- .Values.volumes | toYaml }}
    {{- end }}
{{- end}}
{{- end -}}

{{- define "client.volumeMounts" -}}
{{ if or (.Values.global) (.Values.volumeMounts) }}
  {{- if .Values.global.volumeMounts}}
    {{- .Values.global.volumeMounts | toYaml }}
  {{- end }}
  {{- if (.Values.volumeMounts)}}
    {{- .Values.volumeMounts | toYaml -}}
  {{- end }}
{{- end}}
{{- end -}}
