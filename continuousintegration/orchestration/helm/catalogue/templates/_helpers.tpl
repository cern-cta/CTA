{{/* Generate the catalogue URL based on backend type */}}
{{- define "catalogue.url" -}}
{{- $config := .Values.configuration }}
{{- if eq (typeOf $config) "string" -}}
  {{- $config = $config | fromYaml }}
{{- end }}
{{- $backend := $config.backend }}
{{- if eq $backend "oracle" -}}
oracle:{{ $config.oracleConfig.username }}/{{ $config.oracleConfig.password }}@{{ $config.oracleConfig.database }}
{{- else if eq $backend "postgres" -}}
postgresql:postgresql://{{ $config.postgresConfig.username }}:{{ $config.postgresConfig.password }}@{{ $config.postgresConfig.server }}/{{ $config.postgresConfig.database }}
{{- else if eq $backend "sqlite" -}}
sqlite:{{ $config.sqliteConfig.filepath | replace "%NAMESPACE" .Release.Namespace }}
{{- else }}
{{- fail (printf "Unsupported catalogue backend type: %s. Please use 'oracle', 'postgres', or 'sqlite'." $backend) -}}
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
