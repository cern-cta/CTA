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
{{- if and .Values.global.image.registry .Values.global.image.repository .Values.global.image.tag -}}
  {{- $registry := .Values.global.image.registry -}}
  {{- $repository := .Values.global.image.repository -}}
  {{- $tag := .Values.global.image.tag -}}
  {{- printf "%s/%s:%s" $registry $repository $tag | quote -}}
{{- else if and .Values.image.registry .Values.image.repository .Values.image.tag -}}
  {{- $registry := .Values.image.registry -}}
  {{- $repository := .Values.image.repository -}}
  {{- $tag := .Values.image.tag -}}
  {{- printf "%s/%s:%s" $registry $repository $tag | quote -}}
{{- else }}
{{- fail "You must either provide .Values.global.image or .Values.image with registry, repository, and tag values." -}}
{{- end }}
{{- end }}

{{/* Pick image pull policy. It might be from:
    - .Values.global.image.pullPolicy (Takes priority)
    - .Values.image.pullPolicy
*/}}
{{- define "init.imagePullPolicy" -}}
{{- if and .Values.global.image.pullPolicy (not (empty .Values.global.image.pullPolicy)) -}}
{{- .Values.global.image.pullPolicy | quote -}}
{{- else if and .Values.image.pullPolicy (not (empty .Values.image.pullPolicy)) -}}
{{- .Values.image.pullPolicy | quote -}}
{{- else }}
"Always"
{{- end }}
{{- end }}

{{/* Pick image pull secrets. It might be from:
    - .Values.global.image.pullSecrets (Takes priority)
    - .Values.image.pullSecrets
*/}}
{{- define "init.imagePullSecrets" -}}
{{- if and .Values.global.image.pullSecrets -}}
  {{- range .Values.global.image.pullSecrets }}
    - name: {{ . | quote }}
  {{- end }}
{{- else if and .Values.image.pullSecrets -}}
  {{- range .Values.image.pullSecrets }}
    - name: {{ . | quote }}
  {{- end }}
{{- else }}
{{- fail "You must provide imagePullSecrets value either in .Values.global.image.pullSecrets or .Values.image.pullSecrets" -}}
{{- end }}
{{- end }}



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
