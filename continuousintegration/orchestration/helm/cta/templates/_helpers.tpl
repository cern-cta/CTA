{{/* Define a helper function to generate the catalogue URL based on backend type */}}
{{- define "global.catalogue.url" -}}
{{- $backend := .Values.global.catalogue.backend -}}
{{- if eq $backend "oracle" -}}
oracle://{{ .Values.global.catalogue.config.oracle.username }}:{{ .Values.global.catalogue.config.oracle.password }}@{{ .Values.global.catalogue.config.oracle.database }}
{{- else if eq $backend "postgres" -}}
postgresql:postgresql://{{ .Values.global.catalogue.config.postgres.username }}:{{ .Values.global.catalogue.config.postgres.password }}@{{ .Values.global.catalogue.config.postgres.server }}/{{ .Values.global.catalogue.config.postgres.database }}
{{- else if eq $backend "sqlite" -}}
sqlite://{{ .Values.global.catalogue.config.sqlite.filepath | replace "%NAMESPACE" .Release.Namespace }}
{{- else }}
{{- fail (printf "Unsupported catalogue backend type: %s. Please use 'oracle', 'postgres', or 'sqlite'." .Values.global.catalogue.backend) -}}
{{- end }}
{{- end }}

{{/* Define a helper function to generate the scheduler URL based on backend type */}}
{{- define "global.scheduler.url" -}}
{{- $backend := .Values.global.scheduler.backend -}}
{{- if eq $backend "ceph" -}}
rados://{{ .Values.global.scheduler.config.ceph.id }}@{{ .Values.global.scheduler.config.ceph.pool }}:{{ .Values.global.scheduler.config.ceph.namespace }}
{{- else if eq $backend "postgres" -}}
postgresql:postgresql://{{ .Values.global.scheduler.config.postgres.username }}:{{ .Values.global.scheduler.config.postgres.password }}@{{ .Values.global.scheduler.config.postgres.server }}/{{ .Values.global.scheduler.config.postgres.database }}
{{- else if eq $backend "file" -}}
{{ .Values.global.scheduler.config.file.path | replace "%NAMESPACE" .Release.Namespace }}
{{- else }}
{{- fail (printf "Unsupported scheduler backend type: %s. Please use 'ceph', 'postgres', or 'file'." .Values.global.scheduler.backend) -}}
{{- end }}
{{- end }}
