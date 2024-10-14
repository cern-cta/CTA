{{/* Define a helper function to generate the catalogue URL based on backend type */}}
{{- define "catalogue.url" -}}
{{- $backend := .Values.catalogue.backend -}}
{{- if eq $backend "oracle" -}}
oracle:{{ .Values.catalogue.config.oracle.username }}/{{ .Values.catalogue.config.oracle.password }}@{{ .Values.catalogue.config.oracle.database }}
{{- else if eq $backend "postgres" -}}
postgresql:postgresql://{{ .Values.catalogue.config.postgres.username }}:{{ .Values.catalogue.config.postgres.password }}@{{ .Values.catalogue.config.postgres.server }}/{{ .Values.catalogue.config.postgres.database }}
{{- else if eq $backend "sqlite" -}}
sqlite:{{ .Values.catalogue.config.sqlite.filepath | replace "%NAMESPACE" .Release.Namespace }}
{{- else }}
{{- fail "Unsupported catalogue backend type. Please use 'oracle', 'postgres', or 'sqlite'." -}}
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
