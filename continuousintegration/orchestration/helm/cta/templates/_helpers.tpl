{{/* Define a helper function to generate the catalogue URL based on backend type */}}
{{- define "global.catalogue.url" -}}
{{- $backend := .Values.global.catalogue.backend -}}
{{- if eq $backend "oracle" }}
  oracle:{{ .Values.global.catalogue.config.oracle.username }}/{{ .Values.global.catalogue.config.oracle.password }}@{{ .Values.global.catalogue.config.oracle.database }}
{{- else if eq $backend "postgres" }}
  postgresql:postgresql://{{ .Values.global.catalogue.config.postgres.username }}:{{ .Values.global.catalogue.config.postgres.password }}@{{ .Values.global.catalogue.config.postgres.server }}/{{ .Values.global.catalogue.config.postgres.database }}
{{- else if eq $backend "sqlite" }}
  sqlite:{{ .Values.global.catalogue.config.sqlite.file-path | replace "%NAMESPACE" .Release.Namespace }}
{{- else }}
  {{- fail "Unsupported catalogue backend type. Please use 'oracle', 'postgres', or 'sqlite'." -}}
{{- end }}
{{- end }}

{{/* Define a helper function to generate the scheduler URL based on backend type */}}
{{- define "global.scheduler.url" -}}
{{- $backend := .Values.scheduler.backend -}}
{{- if eq $backend "ceph" }}
  rados://{{ .Values.scheduler.config.ceph.id }}@{{ .Values.scheduler.config.ceph.pool }}:{{ .Values.scheduler.config.ceph.namespace }}
{{- else if eq $backend "postgres" }}
  postgresql:postgresql://{{ .Values.scheduler.config.postgres.username }}:{{ .Values.scheduler.config.postgres.password }}@{{ .Values.scheduler.config.postgres.server }}/{{ .Values.objectstore.config.postgres.database }}
{{- else if eq $backend "file" }}
  {{ .Values.scheduler.config.file.path | replace "%NAMESPACE" .Release.Namespace }}
{{- else }}
  {{- fail "Unsupported scheduler backend type. Please use 'ceph', 'postgres', or 'file'." -}}
{{- end }}
{{- end }}
