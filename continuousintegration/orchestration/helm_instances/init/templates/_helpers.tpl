{{/*
Expand the name of the chart.
*/}}
{{- define "init.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}



{{/*
Sets pesistent volumes for the chart.
*/}}
{{- define "init.volumes" -}}
{{- if (.Values.volumes) }}
volumes:
    {{- .Values.volumes | toYaml | nindent 2}}
{{- else}}
{{- fail "You must provide volumes via .Values.volumes"}}
{{- end}}
{{- end -}}

{{- define "init.volumeMounts" -}}
{{ if (.Values.volumeMounts) }}
volumeMounts:
  {{- .Values.volumeMounts | toYaml | nindent 1 -}}
{{- else}}
{{- fail "You must provide .Values.volumeMounts value so the init pod have something to work on."}}
{{- end}}
{{- end -}}



{{/*
Pick container image. It may be from:
    - `.Values.image` (Has the highest priority)
    - `.Values.global.image` (Has lower priority)
*/}}
{{- define "init.image" -}}
{{- if .Values.global }}
{{- .Values.global.image | quote -}}
{{- else if .Values.image  }}
{{- .Values.image | quote -}}
{{- else }}
{{- fail "You must either provide .Values.image or .Values.global.image value."}}
{{- end }}
{{- end -}}


{{/* Pick image pull secret. It might be from:
    - .Values.global.imagePullSecret (Takes priority)
    - .Values.imagePullSecret
*/}}
{{- define "init.imageSecret" -}}
{{- if (.Values.global ) }}
{{- .Values.global.imagePullSecret | quote -}}
{{- else if (.Values.imagePullSecret) }}
{{- .Values.imagePullSecret | quote -}}
{{- else}}
{{ fail "You must provide imagePullSecret value either in .Values.global.imagePullSecret or in .Values.imagePullSecret"}}
{{- end }}
{{- end -}}