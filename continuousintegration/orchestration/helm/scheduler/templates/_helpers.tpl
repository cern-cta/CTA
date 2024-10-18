{{/* Define a helper function to generate the scheduler URL based on backend type */}}
{{- define "scheduler.url" -}}
{{- $config := .Values.configuration }}
{{- if eq (typeOf $config) "string" -}}
  {{- $config = $config | fromYaml }}
{{- end }}
{{- $backend := $config.backend -}}
{{- if eq $backend "ceph" -}}
rados://{{ $config.cephConfig.id }}@{{ $config.cephConfig.pool }}:{{ $config.cephConfig.namespace }}
{{- else if eq $backend "postgres" -}}
postgresql:postgresql://{{ $config.postgresConfig.username }}:{{ $config.postgresConfig.password }}@{{ $config.postgresConfig.server }}/{{ $config.postgresConfig.database }}
{{- else if eq $backend "file" -}}
{{ $config.fileConfig.path | replace "%NAMESPACE" .Release.Namespace }}
{{- else }}
{{- fail (printf "Unsupported scheduler backend type: %s. Please use 'ceph', 'postgres', or 'file'." $config.backend) -}}
{{- end }}
{{- end }}

{{- define "schedulerWipe.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.wipeImage ) }}
{{- end }}

{{- define "schedulerWipe.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.wipeImage ) }}
{{- end }}

{{- define "schedulerWipe.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.wipeImage ) }}
{{- end }}

{{- define "scheduler.config" -}}
  {{- $schedulerConfig := .Values.configuration }}
  {{- if ne (typeOf $schedulerConfig) "string" -}}
    {{- $schedulerConfig = $schedulerConfig | toYaml }}
  {{- end }}
  {{- $schedulerConfig -}}
{{- end }}