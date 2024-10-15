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


{{/* Pick container image. It may be from:
    - `.Values.image` (Has the highest priority)
    - `.Values.global.image` (Has lower priority)
*/}}
{{- define "kdc.image" -}}
{{- if and .Values.global.image.registry .Values.global.image.repository .Values.global.image.tag -}}
  {{- $registry := .Values.global.image.registry -}}
  {{- $repository := .Values.global.image.repository -}}
  {{- $tag := .Values.global.image.tag -}}
  {{- printf "%s/%s:%s" $registry $repository $tag | quote -}}
{{- else if and .Values.image.registry .Values.image.repository .Values.image.tag -}}
  {{- $registry := .Values.image.registry -}}
  {{- $repository := .Values.image.repository -}}
  {{- $tag := .Values.image.tag -}}
  {{- printf "%s/%s:%s" $registry $repository $tag | quote -}}
{{- else }}
{{- fail "You must either provide .Values.global.image or .Values.image with registry, repository, and tag values." -}}
{{- end }}
{{- end }}

{{/* Pick image pull policy. It might be from:
    - .Values.global.image.pullPolicy (Takes priority)
    - .Values.image.pullPolicy
*/}}
{{- define "kdc.imagePullPolicy" -}}
{{- if and .Values.global.image.pullPolicy (not (empty .Values.global.image.pullPolicy)) -}}
{{- .Values.global.image.pullPolicy | quote -}}
{{- else if and .Values.image.pullPolicy (not (empty .Values.image.pullPolicy)) -}}
{{- .Values.image.pullPolicy | quote -}}
{{- else }}
"Always"
{{- end }}
{{- end }}

{{/* Pick image pull secrets. It might be from:
    - .Values.global.image.pullSecrets (Takes priority)
    - .Values.image.pullSecrets
*/}}
{{- define "kdc.imagePullSecrets" -}}
{{- if and .Values.global.image.pullSecrets -}}
  {{- range .Values.global.image.pullSecrets }}
    - name: {{ . | quote }}
  {{- end }}
{{- else if and .Values.image.pullSecrets -}}
  {{- range .Values.image.pullSecrets }}
    - name: {{ . | quote }}
  {{- end }}
{{- else }}
{{- fail "You must provide imagePullSecrets value either in .Values.global.image.pullSecrets or .Values.image.pullSecrets" -}}
{{- end }}
{{- end }}

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
