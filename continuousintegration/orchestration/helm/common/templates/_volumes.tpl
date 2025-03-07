{{/*
   _volumes.tpl
   This file contains helper functions to handle `volumes` and `volumeMounts`.
   It consolidates volume definitions from both global (.Values.global) and chart-specific (.Values) settings.
   - `common.extraVolumes`: Generates YAML for volume definitions.
   - `common.extraVolumeMounts`: Generates YAML for volume mount definitions.
*/}}

{{/*
   common.extraVolumes:
   Outputs the volume definitions from `.Values.global.extraVolumes` and `.Values.extraVolumes`.
*/}}
{{- define "common.extraVolumes" -}}
  {{- if .Values.global.extraVolumes -}}
{{ .Values.global.extraVolumes | toYaml }}
  {{- end }}
  {{- if .Values.extraVolumes }}
{{ .Values.extraVolumes | toYaml }}
  {{- end }}
{{- end }}

{{/*
   common.extraVolumeMounts:
   Outputs the volume mount definitions from `.Values.global.extraVolumeMounts` and `.Values.extraVolumeMounts`.
*/}}
{{- define "common.extraVolumeMounts" -}}
  {{- if .Values.global.extraVolumeMounts -}}
{{ .Values.global.extraVolumeMounts | toYaml }}
  {{- end }}
  {{- if .Values.extraVolumeMounts }}
{{ .Values.extraVolumeMounts | toYaml }}
  {{- end }}
{{- end }}
