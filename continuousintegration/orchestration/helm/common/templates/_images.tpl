{{/*
   _images.tpl
   This file contains helper functions to work with Docker images, including image name construction,
   pull policy selection, and pull secrets.

   The `common.images.image` function generates the full image name,
   `common.images.pullPolicy` selects the appropriate image pull policy, and
   `common.images.pullSecrets` handles image pull secrets.
*/}}

{{/*
   common.images.image:
   This function constructs the full image name by using values from either the global image settings
   (.Values.global.image) or the chart-specific image settings (.Values.image).
   It expects a dictionary containing "imageRoot" (for chart-specific values) and "global" (for global values).
*/}}
{{- define "common.images.image" -}}
  {{- $imageRoot := .imageRoot -}}
  {{- $global := .global -}}
  {{- if and $global.registry $global.repository $global.tag -}}
    {{- printf "%s/%s:%s" $global.registry $global.repository ( $global.tag | toString ) | quote -}}
  {{- else if and $imageRoot.registry $imageRoot.repository $imageRoot.tag -}}
    {{- printf "%s/%s:%s" $imageRoot.registry $imageRoot.repository ( $imageRoot.tag | toString ) | quote -}}
  {{- else }}
    {{- fail "You must provide registry, repository, and tag for the image in either .Values.global.image or .Values.image." -}}
  {{- end }}
{{- end }}

{{/*
   common.images.pullPolicy:
   This function picks the image pull policy, prioritizing the global setting (.Values.global.image.pullPolicy).
   If no global setting is defined, it falls back to the chart-specific pull policy (.Values.image.pullPolicy),
   and if none are provided, it defaults to "Always".
   It expects a dictionary containing "imageRoot" and "global" values.
*/}}
{{- define "common.images.pullPolicy" -}}
  {{- $imageRoot := .imageRoot -}}
  {{- $global := .global -}}
  {{- if $global.pullPolicy -}}
    {{ $global.pullPolicy | quote }}
  {{- else if $imageRoot.pullPolicy -}}
    {{ $imageRoot.pullPolicy | quote }}
  {{- else }}
    "Always"
  {{- end }}
{{- end }}

{{/*
   common.images.pullSecrets:
   This function generates the image pull secrets in YAML format.
   It prioritizes global pull secrets (.Values.global.image.pullSecrets) and falls back to chart-specific
   pull secrets (.Values.image.pullSecrets). If no secrets are provided, it skips the secret generation.
   It expects a dictionary containing "imageRoot" and "global" values.
*/}}
{{- define "common.images.pullSecrets" -}}
  {{- $imageRoot := .imageRoot -}}
  {{- $global := .global -}}
  {{- if $global.pullSecrets -}}
    {{- range $global.pullSecrets }}
- name: {{ . | quote }}
    {{- end -}}
  {{- else if $imageRoot.pullSecrets -}}
    {{- range $imageRoot.pullSecrets }}
- name: {{ . | quote }}
    {{- end -}}
  {{- end -}}
{{- end -}}