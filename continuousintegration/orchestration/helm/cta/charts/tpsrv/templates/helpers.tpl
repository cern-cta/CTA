{{/* Pick image pull secret. It might be from:
    - .Values.global.imagePullSecret (Takes priority)
    - .Values.imagePullSecret
*/}}
{{- define "tpsrv.imageSecret" -}}
{{- if (.Values.global ) }}
{{- .Values.global.imagePullSecret | quote -}}
{{- else if (.Values.imagePullSecret) }}
{{- .Values.imagePullSecret | quote -}}
{{- else}}
{{ fail "You must provide imagePullSecret value either in .Values.global.imagePullSecret or in .Values.imagePullSecret"}}
{{- end }}
{{- end -}}


{{/*
Pick container image. It may be from:
    - `.Values.image` (Has the higher priority)
    - `.Values.global.image` (Has lower priority)
*/}}
{{- define "tpsrv.image" -}}
{{- if .Values.image }}
{{- .Values.image | quote -}}
{{- else if .Values.global.image  }}
{{- .Values.global.image | quote -}}
{{- else }}
{{- fail "You must provide container image, either by .Values.image or .Values.global.image value."}}
{{- end }}
{{- end -}}