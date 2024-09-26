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



{{/* Pick container image. It may be from:
    - `.Values.image` (Has the highest priority)
    - `.Values.global.image` (Has lower priority)
*/}}
{{- define "init.image" -}}
{{- if .Values.global }}
{{- .Values.global.image | quote -}}
{{- else if .Values.image  }}
{{- .Values.image | quote -}}
{{- else }}
{{- fail "You must either provide .Values.image or .Values.global.image value."}}
{{- end }}
{{- end -}}


{{/* Pick image pull secret. It might be from:
    - .Values.global.imagePullSecret (Takes priority)
    - .Values.imagePullSecret
*/}}
{{- define "init.imageSecret" -}}
{{- if (.Values.global ) }}
{{- .Values.global.imagePullSecret | quote -}}
{{- else if (.Values.imagePullSecret) }}
{{- .Values.imagePullSecret | quote -}}
{{- else}}
{{ fail "You must provide imagePullSecret value either in .Values.global.imagePullSecret or in .Values.imagePullSecret"}}
{{- end }}
{{- end -}}



{{/* PostgreSQL Scheduler Template. Using .Values.schedulerconfig scope. */}}
{{- define "init.scheduler_postgres" -}}
database.type: postgres
database.postgres.username: {{ .postgres.username | quote }}
database.postgres.password: {{ .postgres.password | quote }}
database.postgres.database: {{ .postgres.database | quote }}
database.postgres.server: {{ .postgres.server | quote }}
{{- end -}}



{{/* File based ObjectStore Scheduler Template. Using .Values.schedulerconfig scope. */}}
{{- define "init.scheduler_file" -}}
objectstore.type: file
objectstore.file.path: {{ .file.path | quote }}
{{- end -}}



{{/* CEPH based ObjectStore Scheduler Template Using .Values.schedulerconfig scope. */}}
{{- define "init.scheduler_ceph" -}}
objectstore.type: ceph
objectstore.ceph.mon: {{ .ceph.mon | quote }}
objectstore.ceph.monport: {{ .ceph.monport | quote }}
objectstore.ceph.pool: {{ .ceph.pool | quote }}
objectstore.ceph.namespace: {{ .ceph.namespace | quote }}
objectstore.ceph.id: {{ .ceph.id | quote }}
objectstore.ceph.key: {{ .ceph.key | quote }}
{{- end -}}



{{/* Oracle based Catalogue. Using .Values.catalogueconfig scope. */}}
{{- define "init.catalogue_oracle" -}}
database.type: oracle
database.oracle.username: {{ .oracle.username | quote }}
database.oracle.password: {{ .oracle.password | quote }}
database.oracle.database: {{ .oracle.database | quote }}
{{- end -}}



{{/* PostgreSQL based Catalogue. Using .Values.catalogueconfig scope. */}}
{{- define "init.catalogue_postgres" -}}
database.type: postgres
database.postgres.username: {{ .postgres.username | quote }}
database.postgres.password: {{ .postgres.password | quote }}
database.postgres.server: {{ .postgres.server | quote }}
database.postgres.database: {{ .postgres.database | quote }}
{{- end -}}



{{/* Sqlite based Catalogue. Using .Values.catalogueconfig scope. */}}
{{- define "init.catalogue_sqlite" -}}
database.type: sqlite
database.file.path: {{ .sqlite.filepath | quote }}
{{- end -}}
