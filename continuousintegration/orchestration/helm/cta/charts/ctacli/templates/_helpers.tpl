{{- define "ctacli.name" -}}
  {{ include "common.names.name" . }}
{{- end }}

{{- define "ctacli.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "ctacli.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "ctacli.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{/*
Defines ReadinessProbe. Its use case might vary whether
you want to just check if pod is ready to use or to try
to invoke commands at the runtime of the pod.
*/}}
{{- define "ctacli.readinessProbe" -}}
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
{{- define "ctacli.volumes" -}}
{{ if or (.Values.global) (.Values.volumes) }}
    {{- if (.Values.global.volumes)}}
    {{- .Values.global.volumes | toYaml}}
    {{- end }}
    {{- if (.Values.volumes)}}
    {{- .Values.volumes | toYaml}}
    {{- end }}
{{- end}}
{{- end -}}

{{- define "ctacli.volumeMounts" -}}
{{ if or (.Values.global) (.Values.volumeMounts) }}
  {{- if .Values.global.volumeMounts}}
    {{- .Values.global.volumeMounts | toYaml }}
  {{- end }}
  {{- if (.Values.volumeMounts)}}
    {{- .Values.volumeMounts | toYaml -}}
  {{- end }}
{{- end}}
{{- end -}}
