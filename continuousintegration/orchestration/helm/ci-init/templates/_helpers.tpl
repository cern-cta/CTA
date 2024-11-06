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
