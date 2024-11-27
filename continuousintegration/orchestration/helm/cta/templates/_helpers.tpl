{{/* Define a helper function to generate the scheduler URL based on backend type */}}
{{- define "global.schedulerUrl" -}}
{{- $schedulerConfig := .Values.global.configuration.scheduler }}
{{- if eq (typeOf $schedulerConfig) "string" }}
  {{- $schedulerConfig = fromYaml $schedulerConfig }}
{{- end }}
{{- if $schedulerConfig.backend }}
  {{- if eq $schedulerConfig.backend "ceph" -}}
    rados://{{ $schedulerConfig.cephConfig.id }}@{{ $schedulerConfig.cephConfig.pool }}:{{ $schedulerConfig.cephConfig.namespace }}
  {{- else if eq $schedulerConfig.backend "postgres" -}}
    postgresql:postgresql://{{ $schedulerConfig.postgresConfig.username }}:{{ $schedulerConfig.postgresConfig.password }}@{{ $schedulerConfig.postgresConfig.server }}/{{ $schedulerConfig.postgresConfig.database }}
  {{- else if eq $schedulerConfig.backend "VFS" -}}
    {{ $schedulerConfig.vfsConfig.path | replace "%NAMESPACE" .Release.Namespace }}
  {{- else -}}
    {{- fail (printf "Unsupported scheduler backend type: %s. Please use 'ceph', 'postgres', or 'file'." $schedulerConfig.backend) -}}
  {{- end }}
{{- else -}}
  {{- fail "No 'backend' key found in global.configuration.scheduler. Please specify a valid backend." -}}
{{- end }}
{{- end }}
