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

{{/* Set container image for the init job */}}
{{- define "wipeImage.url" -}}
{{- if and .Values.wipeImage.registry .Values.wipeImage.repository .Values.wipeImage.tag -}}
  {{- $registry := .Values.wipeImage.registry -}}
  {{- $repository := .Values.wipeImage.repository -}}
  {{- $tag := .Values.wipeImage.tag -}}
  {{- printf "%s/%s:%s" $registry $repository $tag | quote -}}
{{- else }}
{{- fail "You must provide .Values.wipeImage with registry, repository, and tag values." -}}
{{- end }}
{{- end }}
