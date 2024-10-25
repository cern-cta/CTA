{{- define "ctaeos.name" -}}
  {{ include "common.names.name" . }}
{{- end }}

{{- define "ctaeos.image" -}}
  {{ include "common.images.image" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "ctaeos.imagePullPolicy" -}}
  {{ include "common.images.pullPolicy" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "ctaeos.imagePullSecrets" -}}
  {{ include "common.images.pullSecrets" (dict "imageRoot" .Values.image "global" .Values.global.image) }}
{{- end }}

{{- define "ctaeos.volumes" -}}
  {{ include "common.volumes" . }}
{{- end -}}

{{- define "ctaeos.volumeMounts" -}}
  {{ include "common.volumeMounts" . }}
{{- end -}}
