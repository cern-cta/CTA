
{{- define "kdc.fullname" -}}
  {{ include "common.names.fullname" . }}
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
