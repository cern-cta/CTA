{{/* Generate the catalogue URL based on backend type */}}
{{- define "catalogue.url" -}}
{{- $catalogueConfig := .Values.configuration }}
{{- if eq (typeOf $catalogueConfig) "string" -}}
  {{- $catalogueConfig = $catalogueConfig | fromYaml }}
{{- end }}
{{- $backend := $catalogueConfig.backend }}
{{- if eq $backend "oracle" -}}
oracle:{{ $catalogueConfig.oracleConfig.username }}/{{ $catalogueConfig.oracleConfig.password }}@{{ $catalogueConfig.oracleConfig.database }}
{{- else if eq $backend "postgres" -}}
postgresql:postgresql://{{ $catalogueConfig.postgresConfig.username }}:{{ $catalogueConfig.postgresConfig.password }}@{{ $catalogueConfig.postgresConfig.server }}/{{ $catalogueConfig.postgresConfig.database }}
{{- else if eq $backend "sqlite" -}}
sqlite:{{ $catalogueConfig.sqliteConfig.filepath | replace "%NAMESPACE" .Release.Namespace }}
{{- else }}
{{- fail (printf "Unsupported catalogue backend type: %s. Please use 'oracle', 'postgres', or 'sqlite'." $backend) -}}
{{- end }}
{{- end }}

{{- define "catalogueReset.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.resetImage ) }}
{{- end }}

{{- define "catalogueReset.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.resetImage ) }}
{{- end }}

{{- define "catalogueReset.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.resetImage ) }}
{{- end }}

{{- define "catalogue.config" -}}
  {{- $catalogueConfig := .Values.configuration }}
  {{- if ne (typeOf $catalogueConfig) "string" -}}
    {{- $catalogueConfig = $catalogueConfig | toYaml }}
  {{- end }}
  {{- $catalogueConfig -}}
{{- end }}
