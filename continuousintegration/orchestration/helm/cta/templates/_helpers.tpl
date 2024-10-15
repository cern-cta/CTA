{{/* Define a helper function to generate the scheduler URL based on backend type */}}
{{- define "global.configuration.scheduler.url" -}}
{{- $config := .Values.global.configuration.scheduler }}
{{- if eq (typeOf $config) "string" -}}
  {{- $config = $config | fromYaml }}
{{- end }}
{{- if eq $config.backend "ceph" }}
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
{{- end }}
