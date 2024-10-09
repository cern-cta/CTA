{{/* Expand the name of the chart. */}}
{{- define "init.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
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

{{/* Define a helper function to generate the catalogue URL based on backend type */}}
{{- define "init.catalogue.url" -}}
{{- $backend := .Values.global.catalogue.backend -}}
{{- if eq $backend "oracle" -}}
oracle://{{ .Values.global.catalogue.config.oracle.username }}:{{ .Values.global.catalogue.config.oracle.password }}@{{ .Values.global.catalogue.config.oracle.database }}
{{- else if eq $backend "postgres" -}}
postgresql:postgresql://{{ .Values.global.catalogue.config.postgres.username }}:{{ .Values.global.catalogue.config.postgres.password }}@{{ .Values.global.catalogue.config.postgres.server }}/{{ .Values.global.catalogue.config.postgres.database }}
{{- else if eq $backend "sqlite" -}}
sqlite://{{ .Values.global.catalogue.config.sqlite.filepath | replace "%NAMESPACE" .Release.Namespace }}
{{- else }}
{{- fail "Unsupported catalogue backend type. Please use 'oracle', 'postgres', or 'sqlite'." -}}
{{- end }}
{{- end }}

{{/* Define a helper function to generate the scheduler URL based on backend type */}}
{{- define "init.scheduler.url" -}}
{{- $backend := .Values.global.scheduler.backend -}}
{{- if eq $backend "ceph" -}}
rados://{{ .Values.global.scheduler.config.ceph.id }}@{{ .Values.global.scheduler.config.ceph.pool }}:{{ .Values.global.scheduler.config.ceph.namespace }}
{{- else if eq $backend "postgres" -}}
postgresql:postgresql://{{ .Values.global.scheduler.config.postgres.username }}:{{ .Values.global.scheduler.config.postgres.password }}@{{ .Values.global.scheduler.config.postgres.server }}/{{ .Values.global.scheduler.config.postgres.database }}
{{- else if eq $backend "file" -}}
{{ .Values.global.scheduler.config.file.path | replace "%NAMESPACE" .Release.Namespace }}
{{- else }}
{{- fail "Unsupported scheduler backend type. Please use 'ceph', 'postgres', or 'file'." -}}
{{- end }}
{{- end }}