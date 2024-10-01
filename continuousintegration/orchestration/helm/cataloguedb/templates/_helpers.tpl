{{/* Expand the name of the chart. */}}
{{- define "init.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}



{{/* Sets pesistent volumes for the chart. */}}
{{- define "init.volumes" -}}
{{- if (.Values.volumes) }}
volumes:
    {{- .Values.volumes | toYaml | nindent 2}}
{{- else}}
{{- fail "You must provide volumes via .Values.volumes"}}
{{- end}}
{{- end -}}

{{- define "init.volumeMounts" -}}
{{ if (.Values.volumeMounts) }}
volumeMounts:
  {{- .Values.volumeMounts | toYaml | nindent 1 -}}
{{- else}}
{{- fail "You must provide .Values.volumeMounts value so the init pod have something to work on."}}
{{- end}}
{{- end -}}


{{/* Oracle based Catalogue. Using .Values.catalogue.config scope. */}}
{{- define "cataloguedb.oracle" -}}
database.type: oracle
database.oracle.username: {{ .oracle.username | quote }}
database.oracle.password: {{ .oracle.password | quote }}
database.oracle.database: {{ .oracle.database | quote }}
{{- end -}}



{{/* PostgreSQL based Catalogue. Using .Values.catalogue.config scope. */}}
{{- define "cataloguedb.postgres" -}}
database.type: postgres
database.postgres.username: {{ .postgres.username | quote }}
database.postgres.password: {{ .postgres.password | quote }}
database.postgres.server: {{ .postgres.server | quote }}
database.postgres.database: {{ .postgres.database | quote }}
{{- end -}}



{{/* Sqlite based Catalogue. Using .Values.catalogue.config scope. */}}
{{- define "cataloguedb.sqlite" -}}
database.type: sqlite
database.file.path: {{ .sqlite.filepath | quote }}
{{- end -}}
