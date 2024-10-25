{{/*
   _volumes.tpl
   This file contains helper functions to handle `volumes` and `volumeMounts`.
   It consolidates volume definitions from both global (.Values.global) and chart-specific (.Values) settings.
   - `common.volumes`: Generates YAML for volume definitions.
   - `common.volumeMounts`: Generates YAML for volume mount definitions.
*/}}

{{/*
   common.volumes:
   Outputs the volume definitions from `.Values.global.volumes` and `.Values.volumes`.
*/}}
{{- define "common.volumes" -}}
  {{- if .Values.global.volumes -}}
{{ .Values.global.volumes | toYaml }}
  {{- end }}
  {{- if .Values.volumes }}
{{ .Values.volumes | toYaml }}
  {{- end }}
{{- end }}

{{/*
   common.volumeMounts:
   Outputs the volume mount definitions from `.Values.global.volumeMounts` and `.Values.volumeMounts`.
*/}}
{{- define "common.volumeMounts" -}}
  {{- if .Values.global.volumeMounts -}}
{{ .Values.global.volumeMounts | toYaml }}
  {{- end }}
  {{- if .Values.volumeMounts }}
{{ .Values.volumeMounts | toYaml }}
  {{- end }}
{{- end }}
