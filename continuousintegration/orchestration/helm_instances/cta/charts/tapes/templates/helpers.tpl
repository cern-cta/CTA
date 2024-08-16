{{/* Pick image registry. It might be from:
    - .Values.global.imagePullSecret (Takes priority)
    - .Values.imagePullSecret
*/}}
{{- define "tapes.imageSecret" -}}
{{- if (.Values.global ) }}
{{- .Values.global.imagePullSecret | quote -}}
{{- else if (.Values.imagePullSecret) }}
{{- .Values.imagePullSecret | quote -}}
{{- else}}
{{ fail "You must provide imagePullSecret value either in .Values.global.imagePullSecret or in .Values.imagePullSecret"}}
{{- end }}
{{- end -}}


{{/*
Pick docker image. It may be from:
    - `.Values.image` (Has the higher priority)
    - `.Values.global.image` (Has lower priority)
*/}}
{{- define "tapes.image" -}}
{{- if .Values.image }}
{{- .Values.image | quote -}}
{{- else if .Values.global.image  }}
{{- .Values.image | quote -}}
{{- else }}
{{- fail "You must provide docker image, either by .Values.image or .Values.global.image value."}}
{{- end }}
{{- end -}}