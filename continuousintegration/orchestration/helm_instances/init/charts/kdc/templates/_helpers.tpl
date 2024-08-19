{{/*
Get the namespace
*/}}
{{- define "kdc.namespace" -}}
{{ $namespace := .Release.Namespace | quote}}
{{- end -}}


{{/*
Expand the name of the chart.
*/}}
{{- define "kdc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{/*
Pick container image. It may be from:
    - `.Values.image` (Has the highest priority)
    - `.Values.global.image` (Has lower priority)
*/}}
{{- define "kdc.image" -}}
{{- if or .Values.image .Values.global.image }}
{{- default .Values.global.image .Values.image | quote -}}
{{- else}}
{{- fail "You must either provide .Values.image or .Values.global.image value."}}
{{- end }}
{{- end -}}

{{/*
Defines ReadinessProbe. Its use case might vary whether
you want to just check if pod is ready to use or to try
to invoke commands at the runtime of the pod.
*/}}
{{- define "kdc.readinessProbe" -}}
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
{{- define "kdc.volumes" -}}
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

{{- define "kdc.volumeMounts" -}}
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

{{/* Pick image pull secret. It might be from:
    - .Values.global.imagePullSecret (Takes priority)
    - .Values.imagePullSecret
*/}}
{{- define "kdc.imageSecret" -}}
{{- if (.Values.global ) }}
{{- .Values.global.imagePullSecret | quote -}}
{{- else if (.Values.imagePullSecret) }}
{{- .Values.imagePullSecret | quote -}}
{{- else}}
{{ fail "You must provide imagePullSecret value either in .Values.global.imagePullSecret or in .Values.imagePullSecret"}}
{{- end }}
{{- end -}}