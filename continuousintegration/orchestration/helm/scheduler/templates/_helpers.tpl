{{/* Define a helper function to generate the scheduler URL based on backend type */}}
{{- define "scheduler.url" -}}
{{- $backend := .Values.scheduler.backend -}}
{{- if eq $backend "ceph" -}}
rados://{{ .Values.scheduler.config.ceph.id }}@{{ .Values.scheduler.config.ceph.pool }}:{{ .Values.scheduler.config.ceph.namespace }}
{{- else if eq $backend "postgres" -}}
postgresql:postgresql://{{ .Values.scheduler.config.postgres.username }}:{{ .Values.scheduler.config.postgres.password }}@{{ .Values.scheduler.config.postgres.server }}/{{ .Values.scheduler.config.postgres.database }}
{{- else if eq $backend "file" -}}
{{ .Values.scheduler.config.file.path | replace "%NAMESPACE" .Release.Namespace }}
{{- else }}
{{- fail "Unsupported scheduler backend type. Please use 'ceph', 'postgres', or 'file'." -}}
{{- end }}
{{- end }}

{{/* Set container image for the init job */}}
{{- define "initImage.url" -}}
{{- if and .Values.initImage.registry .Values.initImage.repository .Values.initImage.tag -}}
  {{- $registry := .Values.initImage.registry -}}
  {{- $repository := .Values.initImage.repository -}}
  {{- $tag := .Values.initImage.tag -}}
  {{- printf "%s/%s:%s" $registry $repository $tag | quote -}}
{{- else }}
{{- fail "You must provide .Values.initImage with registry, repository, and tag values." -}}
{{- end }}
{{- end }}
