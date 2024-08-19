{{/*
Get the namespace
*/}}
{{- define "client.namespace" -}}
{{ $namespace := .Release.Namespace | quote}}
{{- end -}}


{{/*
Expand the name of the chart.
*/}}
{{- define "client.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{/*
Pick docker image. It may be from:
    - `.Values.image` (Has the higher priority)
    - `.Values.global.image` (Has lower priority)
*/}}
{{- define "client.image" -}}
{{- if .Values.image }}
{{- .Values.image | quote -}}
{{- else if .Values.global.image  }}
{{- .Values.global.image | quote -}}
{{- else }}
{{- fail "You must provide docker image, either by .Values.image or .Values.global.image value."}}
{{- end }}
{{- end -}}

{{/*
Defines ReadinessProbe. Its use case might vary whether
you want to just check if pod is ready to use or to try
to invoke commands at the runtime of the pod.
*/}}
{{- define "client.readinessProbe" -}}
readinessProbe:
  exec:
    command: {{.commandsAtRuntime | toJson}}
  initialDelaySeconds: {{.delay}}
  periodSeconds: 10
  failureThreshold: {{.failureTolerance}}
{{- end -}}


{{/*
Sets pesistent volumes for the chart. When loaded as as subchart
it will load additionally all of the global configs as well
(or if .Values.global is defined in subchart as well)
*/}}
{{- define "client.volumes" -}}
{{ if or (.Values.global) (.Values.volumes) }}
volumes:
    {{- if (.Values.global)}}
    {{- .Values.global.volumes | toYaml | nindent 2}}
    {{- end }}
    {{- if (.Values.volumes)}}
    {{- .Values.volumes | toYaml | nindent 2}}
    {{- end }}
{{- end}}
{{- end -}}

{{- define "client.volumeMounts" -}}
{{ if or (.Values.global) (.Values.volumeMounts) }}
volumeMounts:
  {{- if .Values.global}}
    {{- .Values.global.volumeMounts | toYaml | nindent 1 }}
  {{- end }}
  {{- if (.Values.volumeMounts)}}
    {{- .Values.volumeMounts | toYaml | nindent 1 -}}
  {{- end }}
{{- end}}
{{- end -}}

{{/* Pick image registry. It might be from:
    - .Values.global.imagePullSecret (Takes priority)
    - .Values.imagePullSecret
*/}}
{{- define "client.imageSecret" -}}
{{- if (.Values.global ) }}
{{- .Values.global.imagePullSecret | quote -}}
{{- else if (.Values.imagePullSecret) }}
{{- .Values.imagePullSecret | quote -}}
{{- else}}
{{ fail "You must provide imagePullSecret value either in .Values.global.imagePullSecret or in .Values.imagePullSecret"}}
{{- end }}
{{- end -}}