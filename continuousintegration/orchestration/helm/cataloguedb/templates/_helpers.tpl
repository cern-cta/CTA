{{/* Expand the name of the chart. */}}
{{- define "cataloguedb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}



{{/* Sets pesistent volumes for the chart. */}}
{{- define "cataloguedb.volumes" -}}
{{- if (.Values.volumes) }}
volumes:
    {{- .Values.volumes | toYaml | nindent 2}}
{{- else}}
{{- fail "You must provide volumes via .Values.volumes"}}
{{- end}}
{{- end -}}

{{- define "cataloguedb.volumeMounts" -}}
{{ if (.Values.volumeMounts) }}
volumeMounts:
  {{- .Values.volumeMounts | toYaml | nindent 1 -}}
{{- else}}
{{- fail "You must provide .Values.volumeMounts value so the cataloguedb pod have something to work on."}}
{{- end}}
{{- end -}}
