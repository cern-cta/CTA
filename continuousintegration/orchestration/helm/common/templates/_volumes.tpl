// SPDX-FileCopyrightText: 2025 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

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
  {{- if .Values.global.extraVolumes.volumes -}}
{{ .Values.global.extraVolumes.volumes | toYaml }}
  {{- end }}
  {{- if .Values.extraVolumes.volumes }}
{{ .Values.extraVolumes.volumes | toYaml }}
  {{- end }}
{{- end }}

{{/*
   common.extraVolumeMounts:
   Outputs the volume mount definitions from `.Values.global.extraVolumeMounts` and `.Values.extraVolumeMounts`.
*/}}
{{- define "common.extraVolumeMounts" -}}
  {{- if .Values.global.extraVolumes.volumeMounts -}}
{{ .Values.global.extraVolumes.volumeMounts | toYaml }}
  {{- end }}
  {{- if .Values.extraVolumes.volumeMounts }}
{{ .Values.extraVolumes.volumeMounts | toYaml }}
  {{- end }}
{{- end }}
