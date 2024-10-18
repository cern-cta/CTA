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

{{- define "catalogueWipe.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.wipeImage ) }}
{{- end }}

{{- define "catalogueWipe.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.wipeImage ) }}
{{- end }}

{{- define "catalogueWipe.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.wipeImage ) }}
{{- end }}

{{- define "catalogue.config" -}}
  {{- $catalogueConfig := .Values.configuration }}
  {{- if ne (typeOf $catalogueConfig) "string" -}}
    {{- $catalogueConfig = $catalogueConfig | toYaml }}
  {{- end }}
  {{- $catalogueConfig -}}
{{- end }}