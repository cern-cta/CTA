{{/*
Get the namespace
*/}}
{{- define "kdc.namespace" -}}
{{ $namespace := .Release.Namespace | quote }}
{{- end -}}


{{- define "kdc.name" -}}
  {{ include "common.names.name" . }}
{{- end -}}

{{- define "kdc.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image) }}
{{- end -}}

{{- define "kdc.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image) }}
{{- end -}}

{{- define "kdc.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image) }}
{{- end -}}
