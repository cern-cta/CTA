{{/*
Get the namespace
*/}}
{{- define "ctafrontend.namespace" -}}
{{ $namespace := .Release.Namespace | quote}}
{{- end -}}


{{/*
Expand the name of the chart.
*/}}
{{- define "ctafrontend.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{/*
Pick docker image. It may be from:
    - `.Values.image` (Has the higher priority)
    - `.Values.global.image` (Has lower priority)
*/}}
{{- define "ctafrontend.image" -}}
{{- if .Values.global }}
{{- .Values.global.image | quote -}}
{{- else if .Values.image  }}
{{- .Values.image | quote -}}
{{- else }}
{{- fail "You must provide docker image, either by .Values.image or .Values.global.image value."}}
{{- end }}
{{- end -}}

{{/*
Defines ReadinessProbe. Its use case might vary whether
you want to just check if pod is ready to use or to try
to invoke commands at the runtime of the pod.
*/}}
{{- define "ctafrontend.readinessProbe" -}}
readinessProbe:
  exec:
    command: {{.commandsAtRuntime | toJson}}
  initialDelaySeconds: {{.delay}}
  periodSeconds: 10
  failureThreshold: {{.failureTolerance}}
{{- end -}}


{{/*
Sets pesistent volumes for the chart.
*/}}
{{- define "ctafrontend.volumes" -}}
{{ if or (.Values.global.volumes) (.Values.volumes) }}
volumes:
    {{- if (.Values.global.volumes)}}
    {{- .Values.global.volumes | toYaml | nindent 2}}
    {{- end }}
    {{- if (.Values.volumes)}}
    {{- .Values.volumes | toYaml | nindent 2}}
    {{- end }}
{{- end}}
{{- end -}}

{{- define "ctafrontend.volumeMounts" -}}
{{ if or (.Values.global.volumeMounts) (.Values.volumeMounts) }}
volumeMounts:
  {{- if (.Values.global.volumeMounts)}}
    {{- .Values.global.volumeMounts | toYaml | nindent 1 }}
  {{- end }}
  {{- if (.Values.volumeMounts)}}
    {{- .Values.volumeMounts | toYaml | nindent 1 -}}
  {{- end }}
{{- end}}
{{- end -}}